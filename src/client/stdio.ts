import { ChildProcess, IOType } from 'node:child_process';
import spawn from 'cross-spawn';
import process from 'node:process';
import { Stream, PassThrough } from 'node:stream';
import { ReadBuffer, serializeMessage } from '../shared/stdio.js';
import { Transport } from '../shared/transport.js';
import { JSONRPCMessage } from '../types.js';

export type StdioServerParameters = {
    /**
     * The executable to run to start the server.
     */
    command: string;

    /**
     * Command line arguments to pass to the executable.
     */
    args?: string[];

    /**
     * The environment to use when spawning the process.
     *
     * If not specified, the result of getDefaultEnvironment() will be used.
     */
    env?: Record<string, string>;

    /**
     * How to handle stderr of the child process. This matches the semantics of Node's `child_process.spawn`.
     *
     * The default is "inherit", meaning messages to stderr will be printed to the parent process's stderr.
     */
    stderr?: IOType | Stream | number;

    /**
     * The working directory to use when spawning the process.
     *
     * If not specified, the current working directory will be inherited.
     */
    cwd?: string;

    /**
     * Maximum size of the read buffer in bytes. If a single message exceeds
     * this size the transport will emit an error and close.
     *
     * Defaults to 10 MB.
     */
    maxBufferSize?: number;
};

/**
 * Environment variables to inherit by default, if an environment is not explicitly given.
 */
export const DEFAULT_INHERITED_ENV_VARS =
    process.platform === 'win32'
        ? [
              'APPDATA',
              'HOMEDRIVE',
              'HOMEPATH',
              'LOCALAPPDATA',
              'PATH',
              'PROCESSOR_ARCHITECTURE',
              'SYSTEMDRIVE',
              'SYSTEMROOT',
              'TEMP',
              'USERNAME',
              'USERPROFILE',
              'PROGRAMFILES'
          ]
        : /* list inspired by the default env inheritance of sudo */
          ['HOME', 'LOGNAME', 'PATH', 'SHELL', 'TERM', 'USER'];

/**
 * Returns a default environment object including only environment variables deemed safe to inherit.
 */
export function getDefaultEnvironment(): Record<string, string> {
    const env: Record<string, string> = {};

    for (const key of DEFAULT_INHERITED_ENV_VARS) {
        const value = process.env[key];
        if (value === undefined) {
            continue;
        }

        if (value.startsWith('()')) {
            // Skip functions, which are a security risk.
            continue;
        }

        env[key] = value;
    }

    return env;
}

/**
 * Client transport for stdio: this will connect to a server by spawning a process and communicating with it over stdin/stdout.
 *
 * This transport is only available in Node.js environments.
 */
export class StdioClientTransport implements Transport {
    private _process?: ChildProcess;
    private _readBuffer: ReadBuffer;
    private _serverParams: StdioServerParameters;
    private _stderrStream: PassThrough | null = null;
    private _stdoutDataHandler?: (chunk: Buffer) => void;
    private _stdoutErrorHandler?: (error: Error) => void;
    private _stdinErrorHandler?: (error: Error) => void;
    private _pendingDrainWaiters: Array<() => void> = [];
    private _drainHandler?: () => void;
    private _drainStream?: Stream;

    onclose?: () => void;
    onerror?: (error: Error) => void;
    onmessage?: (message: JSONRPCMessage) => void;

    constructor(server: StdioServerParameters) {
        this._serverParams = server;
        this._readBuffer = new ReadBuffer({ maxBufferSize: server.maxBufferSize });
        if (server.stderr === 'pipe' || server.stderr === 'overlapped') {
            this._stderrStream = new PassThrough();
        }
    }

    /**
     * Starts the server process and prepares to communicate with it.
     */
    async start(): Promise<void> {
        if (this._process) {
            throw new Error(
                'StdioClientTransport already started! If using Client class, note that connect() calls start() automatically.'
            );
        }

        return new Promise((resolve, reject) => {
            this._process = spawn(this._serverParams.command, this._serverParams.args ?? [], {
                // merge default env with server env because mcp server needs some env vars
                env: {
                    ...getDefaultEnvironment(),
                    ...this._serverParams.env
                },
                stdio: ['pipe', 'pipe', this._serverParams.stderr ?? 'inherit'],
                shell: false,
                windowsHide: process.platform === 'win32',
                cwd: this._serverParams.cwd
            });

            this._process.on('error', error => {
                reject(error);
                this.onerror?.(error);
            });

            this._process.on('spawn', () => {
                resolve();
            });

            this._process.on('exit', () => {
                // Settle at exit, not just close: 'close' can lag behind if a grandchild inherits the child's stdio.
                this.settlePendingSends();
            });

            this._process.on('close', _code => {
                this._process = undefined;
                this.onclose?.();
                this.settlePendingSends();
            });

            this._stdinErrorHandler = error => {
                this.onerror?.(error);
            };
            this._process.stdin?.on('error', this._stdinErrorHandler);

            this._stdoutDataHandler = chunk => {
                try {
                    this._readBuffer.append(chunk);
                    this.processReadBuffer();
                } catch (error) {
                    this.onerror?.(error as Error);
                    this.close().catch(() => {});
                }
            };
            this._process.stdout?.on('data', this._stdoutDataHandler);

            this._stdoutErrorHandler = error => {
                this.onerror?.(error);
            };
            this._process.stdout?.on('error', this._stdoutErrorHandler);

            if (this._stderrStream && this._process.stderr) {
                this._process.stderr.pipe(this._stderrStream);
            }
        });
    }

    /**
     * The stderr stream of the child process, if `StdioServerParameters.stderr` was set to "pipe" or "overlapped".
     *
     * If stderr piping was requested, a PassThrough stream is returned _immediately_, allowing callers to
     * attach listeners before the start method is invoked. This prevents loss of any early
     * error output emitted by the child process.
     */
    get stderr(): Stream | null {
        if (this._stderrStream) {
            return this._stderrStream;
        }

        return this._process?.stderr ?? null;
    }

    /**
     * The child process pid spawned by this transport.
     *
     * This is only available after the transport has been started.
     */
    get pid(): number | null {
        return this._process?.pid ?? null;
    }

    private processReadBuffer() {
        while (true) {
            try {
                const message = this._readBuffer.readMessage();
                if (message === null) {
                    break;
                }

                this.onmessage?.(message);
            } catch (error) {
                this.onerror?.(error as Error);
            }
        }
    }

    async close(): Promise<void> {
        if (this._process) {
            const processToClose = this._process;
            this._process = undefined;

            // Detach the listeners installed in start() so late stdout cannot reach the JSON-RPC
            // parser. The stderr pass-through is deliberately left attached until the child has
            // fully shut down below, so stderr the child emits while dying still reaches consumers.
            if (this._stdoutDataHandler) {
                processToClose.stdout?.removeListener('data', this._stdoutDataHandler);
                this._stdoutDataHandler = undefined;
            }
            if (this._stdoutErrorHandler) {
                processToClose.stdout?.removeListener('error', this._stdoutErrorHandler);
                this._stdoutErrorHandler = undefined;
            }
            if (this._stdinErrorHandler) {
                processToClose.stdin?.removeListener('error', this._stdinErrorHandler);
                this._stdinErrorHandler = undefined;
            }
            // Keep no-op error handlers so late errors cannot throw uncaught.
            processToClose.stdin?.on('error', () => {});
            processToClose.stdout?.on('error', () => {});
            processToClose.stderr?.on('error', () => {});

            const closePromise = new Promise<void>(resolve => {
                processToClose.once('close', () => {
                    resolve();
                });
            });

            try {
                processToClose.stdin?.end();
            } catch {
                // ignore
            }

            await Promise.race([closePromise, new Promise(resolve => setTimeout(resolve, 2_000).unref())]);

            if (processToClose.exitCode === null) {
                try {
                    processToClose.kill('SIGTERM');
                } catch {
                    // ignore
                }

                await Promise.race([closePromise, new Promise(resolve => setTimeout(resolve, 2_000).unref())]);
            }

            if (processToClose.exitCode === null) {
                try {
                    processToClose.kill('SIGKILL');
                } catch {
                    // ignore
                }
            }

            // The child is gone: tear down the stderr pass-through so a shared stream the caller piped
            // it into deterministically drops its pipe/drain listeners, even when the child never
            // exited cleanly (e.g. a grandchild kept the child's stderr write-end open).
            if (this._stderrStream) {
                try {
                    processToClose.stderr?.unpipe(this._stderrStream);
                } catch {
                    // ignore
                }
                this._stderrStream.end();
            }

            // A pending send may still wait on a 'drain' the dead process will never emit.
            this.settlePendingSends();
        }

        this._readBuffer.clear();
    }

    send(message: JSONRPCMessage): Promise<void> {
        return new Promise(resolve => {
            if (!this._process?.stdin) {
                throw new Error('Not connected');
            }

            const json = serializeMessage(message);
            if (this._process.stdin.write(json)) {
                resolve();
                return;
            } else if (this._pendingDrainWaiters.length === 0) {
                // Share one 'drain' wait so back-pressure cannot accumulate listeners on stdin.
                this._drainStream = this._process.stdin;
                this._drainHandler = () => {
                    this._drainHandler = undefined;
                    this._drainStream = undefined;
                    const waiters = this._pendingDrainWaiters;
                    this._pendingDrainWaiters = [];
                    for (const wait of waiters) {
                        wait();
                    }
                };
                this._drainStream.once('drain', this._drainHandler);
            }
            this._pendingDrainWaiters.push(resolve);
        });
    }

    /**
     * Resolve the send() promises still waiting on a stdin 'drain' that the process
     * will no longer emit once it is gone. They resolve rather than reject: the bytes
     * were already handed to stdin.write(), so delivery past process exit is out of scope
     * - use onclose to learn the connection is gone.
     *
     * @internal
     */
    private settlePendingSends() {
        const waiters = this._pendingDrainWaiters;
        this._pendingDrainWaiters = [];
        for (const wait of waiters) {
            wait();
        }
        // The shared 'drain' wait may still be attached to a stdin the process will never drain;
        // drop it so it does not linger on a dead stream.
        if (this._drainHandler && this._drainStream) {
            this._drainStream.removeListener('drain', this._drainHandler);
            this._drainHandler = undefined;
            this._drainStream = undefined;
        }
    }
}
