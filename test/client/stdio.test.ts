import { JSONRPCMessage } from '../../src/types.js';
import { StdioClientTransport, StdioServerParameters } from '../../src/client/stdio.js';

// Configure default server parameters based on OS
// Uses 'more' command for Windows and 'tee' command for Unix/Linux
const getDefaultServerParameters = (): StdioServerParameters => {
    if (process.platform === 'win32') {
        return { command: 'more' };
    }
    return { command: '/usr/bin/tee' };
};

const serverParameters = getDefaultServerParameters();

test('should start then close cleanly', async () => {
    const client = new StdioClientTransport(serverParameters);
    client.onerror = error => {
        throw error;
    };

    let didClose = false;
    client.onclose = () => {
        didClose = true;
    };

    await client.start();
    expect(didClose).toBeFalsy();
    await client.close();
    expect(didClose).toBeTruthy();
});

test('should read messages', async () => {
    const client = new StdioClientTransport(serverParameters);
    client.onerror = error => {
        throw error;
    };

    const messages: JSONRPCMessage[] = [
        {
            jsonrpc: '2.0',
            id: 1,
            method: 'ping'
        },
        {
            jsonrpc: '2.0',
            method: 'notifications/initialized'
        }
    ];

    const readMessages: JSONRPCMessage[] = [];
    const finished = new Promise<void>(resolve => {
        client.onmessage = message => {
            readMessages.push(message);

            if (JSON.stringify(message) === JSON.stringify(messages[1])) {
                resolve();
            }
        };
    });

    await client.start();
    await client.send(messages[0]);
    await client.send(messages[1]);
    await finished;
    expect(readMessages).toEqual(messages);

    await client.close();
});

test('should return child process pid', async () => {
    const client = new StdioClientTransport(serverParameters);

    await client.start();
    expect(client.pid).not.toBeNull();
    await client.close();
    expect(client.pid).toBeNull();
});

test('should respect custom maxBufferSize option', async () => {
    const client = new StdioClientTransport({
        command: 'node',
        args: ['-e', 'process.stdout.write(Buffer.alloc(200, 0x41))'],
        maxBufferSize: 100
    });

    const errorReceived = new Promise<Error>(resolve => {
        client.onerror = resolve;
    });
    const closed = new Promise<void>(resolve => {
        client.onclose = () => resolve();
    });

    await client.start();

    const error = await errorReceived;
    expect(error.message).toMatch(/ReadBuffer exceeded maximum size/);
    await closed;
});

test('should fire onerror and close when ReadBuffer overflows', async () => {
    const client = new StdioClientTransport({
        command: 'node',
        args: ['-e', 'process.stdout.write(Buffer.alloc(11 * 1024 * 1024, 0x41))']
    });

    const errorReceived = new Promise<Error>(resolve => {
        client.onerror = resolve;
    });
    const closed = new Promise<void>(resolve => {
        client.onclose = () => resolve();
    });

    await client.start();

    const error = await errorReceived;
    expect(error.message).toMatch(/ReadBuffer exceeded maximum size/);
    await closed;
});

test('close() removes the data listener from stdout to prevent stale reads after close', async () => {
    // The 'data' listener on stdout must be removed in close(), otherwise late
    // non-JSON output from the child triggers processReadBuffer() -> JSON.parse()
    // -> SyntaxError after close.
    const client = new StdioClientTransport({
        command: process.execPath,
        args: ['-e', 'process.stdin.resume(); process.stdin.on("end", () => process.exit(0));']
    });

    await client.start();

    const proc = (client as unknown as { _process: import('node:child_process').ChildProcess })._process;
    expect(proc).toBeDefined();
    expect(proc!.stdout).toBeDefined();

    // Before close, the data listener should be present
    expect(proc!.stdout!.listenerCount('data')).toBeGreaterThan(0);

    await client.close();

    // After close, the data listener should be removed
    expect(proc!.stdout!.listenerCount('data')).toBe(0);
});

test('send() under back-pressure does not accumulate drain listeners on stdin', async () => {
    // Regression test for #3070: each send() that hits a full stdin buffer used
    // to add its own 'drain' listener, so sustained back-pressure piled them up
    // (MaxListenersExceededWarning). Sends now share a single drain wait.
    const warnings: string[] = [];
    const onWarning = (warning: Error) => {
        if (warning.name === 'MaxListenersExceededWarning') warnings.push(warning.message);
    };
    process.on('warning', onWarning);

    try {
        // A child that never reads stdin keeps the pipe permanently full.
        const client = new StdioClientTransport({
            command: process.execPath,
            args: ['-e', 'setTimeout(() => {}, 10_000)']
        });
        await client.start();

        const pending: Promise<void>[] = [];
        for (let i = 0; i < 30; i++) {
            pending.push(client.send({ jsonrpc: '2.0', id: i, method: 'x', params: { blob: 'a'.repeat(4096) } }));
        }
        await new Promise(resolve => setTimeout(resolve, 100));

        const proc = (client as unknown as { _process: import('node:child_process').ChildProcess })._process;
        expect(proc!.stdin!.listenerCount('drain')).toBeLessThanOrEqual(1);
        expect(warnings).toHaveLength(0);

        await client.close();
        // The back-pressured sends settle once the process is gone (no dangling promises).
        await Promise.all(pending);
    } finally {
        process.off('warning', onWarning);
    }
}, 15_000);

test('close() removes the shared drain listener from stdin when the child never drains', async () => {
    // A back-pressured send() registers the shared 'drain' wait on the child's stdin. If the child
    // is closed before it ever drains, close() (via settlePendingSends()) must remove that listener
    // rather than leave it dangling on a dead stream. A child that never reads stdin keeps the pipe
    // full, so 'drain' never fires on its own - only the explicit removal in close() can clear it.
    const client = new StdioClientTransport({
        command: process.execPath,
        args: ['-e', 'setTimeout(() => {}, 10_000)']
    });
    await client.start();

    const proc = (client as unknown as { _process: import('node:child_process').ChildProcess })._process;

    // One large back-pressured send registers the shared 'drain' wait on stdin.
    const pending = client.send({ jsonrpc: '2.0', id: 0, method: 'x', params: { blob: 'a'.repeat(256 * 1024) } });
    await vi.waitFor(() => expect(proc!.stdin!.listenerCount('drain')).toBe(1), { timeout: 3000, interval: 25 });

    await client.close();
    await pending;

    // close() settled the pending send and removed the drain listener the dead stdin will never emit.
    expect(proc!.stdin!.listenerCount('drain')).toBe(0);
}, 15_000);

test('a back-pressured send() resolves and onclose fires exactly once when the child dies', async () => {
    // Pins the settle-on-death contract: a send() still waiting on a stdin 'drain' when the child
    // dies resolves (rather than hanging or rejecting), and the connection-gone signal (onclose)
    // fires exactly once.
    let oncloseCount = 0;
    const client = new StdioClientTransport({
        command: process.execPath,
        args: ['-e', 'setTimeout(() => {}, 10_000)']
    });
    client.onclose = () => {
        oncloseCount += 1;
    };

    await client.start();

    const proc = (client as unknown as { _process: import('node:child_process').ChildProcess })._process;

    // One large back-pressured send leaves the shared 'drain' wait pending on the full pipe.
    const pending = client.send({ jsonrpc: '2.0', id: 0, method: 'x', params: { blob: 'a'.repeat(256 * 1024) } });
    await vi.waitFor(() => expect(proc!.stdin!.listenerCount('drain')).toBe(1), { timeout: 3000, interval: 25 });

    await client.close();

    // The still-pending send resolves once the child is gone...
    await pending;
    // ...and the connection-gone signal fired exactly once.
    expect(oncloseCount).toBe(1);
}, 15_000);

test('a back-pressured send() still pending when the child exits on its own resolves and onclose fires once', async () => {
    // Here the child exits on its own (not via close()), so the pending send is settled by
    // settle-on-exit (the 'exit' handler) rather than by an explicit close() call - settling at
    // the earliest point the process is gone, so a send can never wait on a 'close' that lags
    // behind exit (e.g. a grandchild inheriting the child's stdio on platforms where 'close'
    // waits for the stdio to fully close).
    let oncloseCount = 0;
    const client = new StdioClientTransport({
        command: process.execPath,
        args: ['-e', 'setTimeout(() => process.exit(0), 2000)']
    });
    client.onclose = () => {
        oncloseCount += 1;
    };

    await client.start();

    const proc = (client as unknown as { _process: import('node:child_process').ChildProcess })._process;

    // One large back-pressured send leaves the shared 'drain' wait pending on the full pipe.
    const pending = client.send({ jsonrpc: '2.0', id: 0, method: 'x', params: { blob: 'a'.repeat(256 * 1024) } });
    await vi.waitFor(() => expect(proc!.stdin!.listenerCount('drain')).toBe(1), { timeout: 3000, interval: 25 });

    // The child exits on its own at ~2s; settle-on-exit resolves the send.
    await pending;
    expect(oncloseCount).toBe(1);
    expect(proc!.stdin!.listenerCount('drain')).toBe(0);
}, 15_000);

test('close() removes the drain listener the stderr pipe added on the pass-through', async () => {
    // Regression test for #3070. child.stderr.pipe(_stderrStream) attaches a 'drain' listener to
    // _stderrStream (the pipe's destination) once it backs up. A grandchild holding the child's
    // stderr write-end means child.stderr never 'end's, so pipe() never auto-cleans that listener
    // up - close() must unpipe it. Counting 'drain' on the pass-through distinguishes fixed from
    // unfixed; counting it on the source (stderr) would always read 0.
    const HOLDING_SCRIPT = [
        String.raw`const { spawn } = require('child_process');`,
        String.raw`spawn(process.execPath, ['-e', 'setTimeout(() => {}, 3000)'], { stdio: 'inherit' });`,
        String.raw`process.stderr.write(Buffer.alloc(256 * 1024, 0x41));`
    ].join('\n');
    const client = new StdioClientTransport({
        command: process.execPath,
        args: ['-e', HOLDING_SCRIPT],
        stderr: 'pipe'
    });

    await client.start();

    const stderrStream = (client as unknown as { _stderrStream: import('node:stream').PassThrough })._stderrStream;
    if (stderrStream === null) throw new Error('expected a stderr pass-through');

    // Wait until the child's stderr back-pressure has made pipe() attach the 'drain' listener.
    await vi.waitFor(() => expect(stderrStream.listenerCount('drain')).toBeGreaterThan(0), { timeout: 3000, interval: 25 });

    await client.close();

    // The unpipe in close() removed the drain listener even though the grandchild keeps the
    // child's stderr open, so the pass-through cannot accumulate drain listeners.
    expect(stderrStream.listenerCount('drain')).toBe(0);
}, 20_000);
