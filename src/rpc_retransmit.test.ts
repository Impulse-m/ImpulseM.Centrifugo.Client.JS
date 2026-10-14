import { Centrifuge } from './centrifuge';
import { FakeCentrifugoServer, holdReply } from './fakeServer';
import { errorCodes } from './codes';
import { Options, RpcResult, State } from './types';
import { format } from 'util';
import { CodecCase, codecCases, connectClient, disconnectAll, rpcOptionsWireFlag, until } from './test_codecs';

// Tracked RPC calls: message ids, the retransmit of calls that lost their transport, the queue
// bound, acknowledgements, deadlines, cancels, legacy connections and the instance option. Every
// case runs over both codecs (see test_codecs.ts).

// A call of the application: an rpc command that is not a cancel.
function isCall(cmd: any): boolean {
  return cmd.rpc !== undefined && cmd.rpc.cancel === undefined;
}

function isRetransmit(cmd: any): boolean {
  return isCall(cmd) && cmd.rpc.retransmit === true;
}

function isCancel(cmd: any): boolean {
  return cmd.rpc !== undefined && cmd.rpc.cancel !== undefined;
}

// Keeps a rejection of a call that a test does not await from failing the run.
function quiet<T>(promise: Promise<T>): Promise<T> {
  promise.catch((): void => {});
  return promise;
}

// Makes the next write of the client's transport throw, as a broken socket does. The transport
// stays open for the client until it takes the write error as a lost connection.
function failNextWrite(c: Centrifuge): void {
  const transport: any = (c as any)._transport;
  const write: (...args: any[]) => void = transport.send.bind(transport);
  let failing: boolean = true;
  transport.send = (...args: any[]): void => {
    if (failing) {
      failing = false;
      throw new Error('write failed');
    }
    write(...args);
  };
}

// A server error that refuses the call with the retry signal of the server: 'new_msg_id' (the call did
// not run, its message id was refused) or 'later' (the call did not run, its message id is still good).
function retryError(retry: 'new_msg_id' | 'later', code: number, temporary: boolean): any {
  return {
    code: code,
    message: retry === 'later' ? 'dedup_unavailable' : 'msg_id_invalid',
    temporary: temporary,
    metadata: { 'x-centrifugo-retry': retry },
  };
}

describe.each(codecCases)('tracked RPC calls over $name', (codecCase: CodecCase) => {
  const testInstance: string = 'AbCdEfGhIjKlMnOpQrStUv';
  let server: FakeCentrifugoServer;
  const clients: Centrifuge[] = [];

  beforeEach(async (): Promise<void> => {
    server = await FakeCentrifugoServer.start(codecCase.serverCodec);
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
  });

  afterEach(async (): Promise<void> => {
    disconnectAll(clients);
    await server.close();
  });

  // Most cases run with an instance, as retransmit and cancel need one. The cases without it pass
  // instance: undefined.
  function create(options?: Partial<Options>): Centrifuge {
    const c: Centrifuge = codecCase.createClient(server.url, { instance: testInstance, ...options });
    clients.push(c);
    return c;
  }

  async function connect(c: Centrifuge): Promise<void> {
    await connectClient(c);
    expect(c.state).toBe(State.Connected);
  }

  function callCommands(): any[] {
    return server.received.filter(isCall);
  }

  // Holds the replies of the calls, retransmits included, until the test sends them.
  function holdCalls(): void {
    server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
  }

  test('a call that loses its transport runs once and completes with the same reply', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    const executed: number[] = [];
    server.onCommand = (cmd: any): any => {
      if (!isCall(cmd)) {
        return undefined;
      }
      if (cmd.rpc.retransmit) {
        // The server answers a repeated message id from the record of the first attempt.
        return { id: cmd.id, rpc: { data: codecCase.encode({ answer: 42 }), cached: true } };
      }
      executed.push(cmd.rpc.msg_id);
      return holdReply;
    };

    const call: Promise<RpcResult> = c.rpc('work', codecCase.encode({ n: 1 }));
    const first: any = await server.waitFor(isCall);
    server.closeConnection();
    const resent: any = await server.waitFor(isRetransmit);
    const result: RpcResult = await call;

    expect(codecCase.decode(result.data)).toEqual({ answer: 42 });
    expect(result.cached).toBe(true);
    expect(executed).toEqual([first.rpc.msg_id]);
    expect(resent.rpc.msg_id).toBe(first.rpc.msg_id);
    expect(resent.rpc.retransmit).toBe(true);
    expect(c.state).toBe(State.Connected);
  });

  test('a write error on an open transport sends the call again on the reconnect', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({ answer: 7 }) } } : undefined);
    // The transport stays open, but its next write throws: the next write is the call.
    failNextWrite(c);

    const result: RpcResult = await c.rpc('echo', codecCase.encode({}));

    expect(codecCase.decode(result.data)).toEqual({ answer: 7 });
    expect(callCommands()).toHaveLength(1);
    expect(callCommands()[0].rpc.retransmit).toBe(true);
    expect(c.state).toBe(State.Connected);
  });

  test('a reply that is not cached has cached false', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    const result: RpcResult = await c.rpc('echo', codecCase.encode({}));
    expect(result.cached).toBe(false);
  });

  test('calls waiting on a lost transport are sent again in message id order', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    holdCalls();
    const calls: Promise<RpcResult>[] = [
      quiet(c.rpc('a', codecCase.encode(1))),
      quiet(c.rpc('b', codecCase.encode(2))),
      quiet(c.rpc('c', codecCase.encode(3))),
    ];
    await server.waitFor((cmd: any): boolean => isCall(cmd) && cmd.rpc.method === 'c');
    const originalIds: number[] = callCommands().map((cmd: any): number => cmd.rpc.msg_id);

    server.closeConnection();
    await server.waitFor((cmd: any): boolean => isRetransmit(cmd) && cmd.rpc.method === 'c');
    const resent: any[] = server.received.filter(isRetransmit);

    expect(resent.map((cmd: any): number => cmd.rpc.msg_id)).toEqual(originalIds);
    expect(resent.map((cmd: any): string => cmd.rpc.method)).toEqual(['a', 'b', 'c']);
    expect(originalIds).toEqual([...originalIds].sort((x: number, y: number): number => x - y));

    for (const cmd of resent) {
      server.send({ id: cmd.id, rpc: { data: codecCase.encode({ method: cmd.rpc.method }) } });
    }
    const results: RpcResult[] = await Promise.all(calls);
    expect(results.map((r: RpcResult): any => codecCase.decode(r.data)))
      .toEqual([{ method: 'a' }, { method: 'b' }, { method: 'c' }]);
  });

  test('a call made during a refresh that the connection loss ends is sent on the next connection', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 2000, minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? holdReply : undefined);
    const refreshing: Promise<unknown> = quiet(c.refresh('next-token'));
    await server.waitFor((cmd: any): boolean => cmd.refresh !== undefined);

    // The call waits for the refresh in flight, which the connection loss ends.
    const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}));
    server.onCommand = (cmd: any): any => {
      if (isCall(cmd)) {
        return { id: cmd.id, rpc: { data: codecCase.encode({ done: true }) } };
      }
      return cmd.refresh !== undefined ? holdReply : undefined;
    };
    server.closeConnection();

    await expect(refreshing).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    const result: RpcResult = await call;
    expect(codecCase.decode(result.data)).toEqual({ done: true });
    // Sent once, with its message id, on the connection that followed.
    expect(callCommands()).toHaveLength(1);
    expect(callCommands()[0].rpc.msg_id).toBeDefined();
  });

  test('a call beyond maxPendingRpc rejects with limitExceeded', async (): Promise<void> => {
    const c: Centrifuge = create({ maxPendingRpc: 2 });
    await connect(c);
    server.onCommand = (cmd: any): any => {
      if (!isCall(cmd)) {
        return undefined;
      }
      return cmd.rpc.method.startsWith('hold') ? holdReply : { id: cmd.id, rpc: { data: codecCase.encode({}) } };
    };
    const first: Promise<RpcResult> = quiet(c.rpc('hold', codecCase.encode(1)));
    quiet(c.rpc('hold2', codecCase.encode(2)));
    await server.waitFor((cmd: any): boolean => isCall(cmd) && cmd.rpc.method === 'hold2');

    await expect(c.rpc('hold', codecCase.encode(3))).rejects.toMatchObject({ code: errorCodes.limitExceeded });
    expect(callCommands()).toHaveLength(2);

    // A settled call frees its place: the next call goes out.
    server.send({ id: server.received.filter(isCall)[0].id, rpc: { data: codecCase.encode({}) } });
    await first;
    await expect(c.rpc('echo', codecCase.encode(4))).resolves.toMatchObject({ cached: false });
  });

  test('the calls made while disconnected are bounded at entry: the 33rd rejects at once', async (): Promise<void> => {
    const c: Centrifuge = create();
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    const calls: Promise<RpcResult>[] = [];
    for (let i: number = 0; i < 32; i++) {
      calls.push(quiet(c.rpc('a', codecCase.encode(i))));
    }

    // Not connected yet: the call is refused at once, it does not wait for the connection.
    await expect(c.rpc('a', codecCase.encode(32))).rejects.toMatchObject({ code: errorCodes.limitExceeded });

    await connect(c);
    await expect(Promise.all(calls)).resolves.toHaveLength(32);
    expect(callCommands()).toHaveLength(32);
  });

  test('a lost call keeps its slot across the reconnect', async (): Promise<void> => {
    const c: Centrifuge = create({ maxPendingRpc: 2 });
    await connect(c);
    holdCalls();
    const first: Promise<RpcResult> = quiet(c.rpc('a', codecCase.encode(1)));
    const second: Promise<RpcResult> = quiet(c.rpc('b', codecCase.encode(2)));
    await until((): boolean => callCommands().length === 2);

    server.closeConnection();
    await expect(c.rpc('c', codecCase.encode(3))).rejects.toMatchObject({ code: errorCodes.limitExceeded });

    // The reconnect sends both calls again, and they still hold the two slots until they settle.
    await until((): boolean => server.received.filter(isRetransmit).length === 2);
    for (const cmd of server.received.filter(isRetransmit)) {
      server.send({ id: cmd.id, rpc: { data: codecCase.encode({}) } });
    }
    await Promise.all([first, second]);

    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    await expect(c.rpc('d', codecCase.encode(4))).resolves.toMatchObject({ cached: false });
  });

  test('a call on a legacy connection with an instance holds a slot and releases it on settle', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    const c: Centrifuge = create({ maxPendingRpc: 1 });
    await connect(c);
    holdCalls();
    const first: Promise<RpcResult> = quiet(c.rpc('a', codecCase.encode(1)));
    await until((): boolean => callCommands().length === 1);

    await expect(c.rpc('b', codecCase.encode(2))).rejects.toMatchObject({ code: errorCodes.limitExceeded });
    expect(callCommands()).toHaveLength(1);
    server.send({ id: callCommands()[0].id, rpc: { data: codecCase.encode({}) } });
    await first;

    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    await expect(c.rpc('c', codecCase.encode(3))).resolves.toMatchObject({ cached: false });
    expect(callCommands().every((cmd: any): boolean => cmd.rpc.msg_id === undefined)).toBe(true);
  });

  test('without an instance there is no bound on the calls', async (): Promise<void> => {
    const c: Centrifuge = create({ instance: undefined, maxPendingRpc: 1 });
    await connect(c);
    holdCalls();
    const calls: Promise<RpcResult>[] = [];
    for (let i: number = 0; i < 3; i++) {
      calls.push(quiet(c.rpc('x', codecCase.encode(i))));
    }
    await until((): boolean => callCommands().length === 3);

    for (const cmd of callCommands()) {
      server.send({ id: cmd.id, rpc: { data: codecCase.encode({}) } });
    }
    await expect(Promise.all(calls)).resolves.toHaveLength(3);
  });

  test('without an instance a call is a plain request and is not sent again on a lost connection', async (): Promise<void> => {
    const c: Centrifuge = create({ instance: undefined });
    await connect(c);
    holdCalls();
    const call: Promise<RpcResult> = quiet(c.rpc('x', codecCase.encode({})));
    const sent: any = await server.waitFor(isCall);

    expect(sent.rpc.msg_id).toBeUndefined();
    expect(sent.rpc.retransmit).toBeUndefined();
    server.closeConnection();
    // The server has no scope to tell the call apart, so it is not sent again: it may have run.
    await expect(call).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    expect(server.received.some(isRetransmit)).toBe(false);
  });

  test('without an instance a signal rejects with unsupported and nothing is sent', async (): Promise<void> => {
    const c: Centrifuge = create({ instance: undefined });
    await connect(c);
    const controller: AbortController = new AbortController();

    await expect(c.rpc('x', codecCase.encode({}), { signal: controller.signal }))
      .rejects.toMatchObject({ code: errorCodes.unsupported });
    expect(callCommands()).toHaveLength(0);
  });

  test('without an instance the headers of a call are sent with its plain request', async (): Promise<void> => {
    const c: Centrifuge = create({ instance: undefined });
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);

    await c.rpc('echo', codecCase.encode({}), { headers: { 'x-example': 'value' } });

    expect(callCommands()[0].rpc.headers).toEqual({ 'x-example': 'value' });
    expect(callCommands()[0].rpc.msg_id).toBeUndefined();
  });

  test('the ids of the replies received are sent with the next request', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);

    await c.rpc('echo', codecCase.encode(1));
    const firstId: number = callCommands()[0].rpc.msg_id;
    await c.rpc('echo', codecCase.encode(2));
    await c.rpc('echo', codecCase.encode(3));

    const calls: any[] = callCommands();
    expect(calls[0].rpc.ack).toBeUndefined();
    expect(calls[1].rpc.ack).toEqual([firstId]);
    // Each id is sent once.
    expect(calls[2].rpc.ack).toEqual([calls[1].rpc.msg_id]);
  });

  test('a call whose deadline passes while its transport is down rejects with timeout', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 300 });
    await connect(c);
    holdCalls();
    const call: Promise<RpcResult> = quiet(c.rpc('slow', codecCase.encode({})));
    await server.waitFor(isCall);

    server.closeConnection();
    await server.close();

    await expect(call).rejects.toMatchObject({ code: errorCodes.timeout });
    expect(c.state).not.toBe(State.Connected);
  });

  test('a retransmitted call keeps its original deadline', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 300 });
    await connect(c);
    holdCalls();
    const call: Promise<RpcResult> = quiet(c.rpc('slow', codecCase.encode({})));
    await server.waitFor(isCall);

    server.closeConnection();
    await server.waitFor(isRetransmit);

    // The retransmit is held too: the call still ends at its original deadline.
    await expect(call).rejects.toMatchObject({ code: errorCodes.timeout });
  });

  test('a call aborted before it is sent rejects with cancelled and sends nothing', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    const controller: AbortController = new AbortController();
    controller.abort();

    await expect(c.rpc('x', codecCase.encode({}), { signal: controller.signal }))
      .rejects.toMatchObject({ code: errorCodes.cancelled });
    expect(callCommands()).toHaveLength(0);
  });

  test('an abort after the connection is ready but before the call is tracked rejects with cancelled and sends nothing', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    const controller: AbortController = new AbortController();
    // The call is ready to go out when the abort comes: no listener of the signal runs for it any more.
    const track: any = (c as any)._rpc.track.bind((c as any)._rpc);
    (c as any)._rpc.track = (...args: any[]): Promise<RpcResult> => {
      controller.abort();
      return track(...args);
    };

    await expect(c.rpc('x', codecCase.encode({}), { signal: controller.signal }))
      .rejects.toMatchObject({ code: errorCodes.cancelled });
    expect(callCommands()).toHaveLength(0);
  });

  test('an abort while the client is not connected rejects with cancelled, and nothing is sent', async (): Promise<void> => {
    const c: Centrifuge = create();
    const controller: AbortController = new AbortController();
    // The call waits for the connection when the abort comes.
    const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    controller.abort();

    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
    expect(server.received).toHaveLength(0);
  });

  test('a call aborted after it is sent sends a cancel and rejects with cancelled', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    holdCalls();
    const controller: AbortController = new AbortController();
    const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    const sent: any = await server.waitFor(isCall);

    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });

    const cancel: any = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
    expect(cancel.rpc.method).toBeUndefined();
  });

  test('the reply of an aborted call still reaches the server as an ack', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    holdCalls();
    const controller: AbortController = new AbortController();
    const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    const sent: any = await server.waitFor(isCall);
    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });

    server.send({ id: sent.id, error: { code: 3001, message: 'cancelled' } });
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    // The late reply is handled before the reply of the next call, so the call after it carries
    // the acknowledgement.
    await c.rpc('echo', codecCase.encode(1));
    await c.rpc('echo', codecCase.encode(2));

    // The first echo was sent before the late reply arrived, its own reply is acknowledged too.
    const echoes: any[] = callCommands().filter((cmd: any): boolean => cmd.rpc.method === 'echo');
    expect(echoes[1].rpc.ack).toEqual([sent.rpc.msg_id, echoes[0].rpc.msg_id]);
  });

  test('a call aborted while its transport is down sends the cancel with the next connection', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 200, maxReconnectDelay: 200 });
    await connect(c);
    holdCalls();
    const controller: AbortController = new AbortController();
    const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    const sent: any = await server.waitFor(isCall);

    const connecting: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connecting', (): void => resolve());
    });
    server.closeConnection();
    await connecting;
    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });

    const cancel: any = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
    expect(server.received.some((cmd: any): boolean => isRetransmit(cmd) && cmd.rpc.msg_id === sent.rpc.msg_id))
      .toBe(false);
  });

  test('a cancel whose write fails is sent again with the next connection', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    holdCalls();
    const controller: AbortController = new AbortController();
    const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    const sent: any = await server.waitFor(isCall);

    // The next write is the cancel: it fails, so the server gets no cancel on this connection.
    failNextWrite(c);
    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });

    const cancel: any = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
    expect(server.received.filter(isCancel)).toHaveLength(1);
  });

  test('the acknowledgements of a command that was not written are sent with the next request', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    await c.rpc('echo', codecCase.encode(1));
    const firstId: number = callCommands()[0].rpc.msg_id;

    failNextWrite(c);
    await c.rpc('echo', codecCase.encode(2));

    // The command that carried the acknowledgement was never written: the retransmit of the call carries it.
    const last: any = callCommands()[callCommands().length - 1];
    expect(last.rpc.retransmit).toBe(true);
    expect(last.rpc.ack).toEqual([firstId]);
  });

  test('a write error with several calls in flight sends all of them again in message id order', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    holdCalls();
    const calls: Promise<RpcResult>[] = ['a', 'b', 'c'].map(
      (method: string): Promise<RpcResult> => quiet(c.rpc(method, codecCase.encode({})))
    );
    await server.waitFor((cmd: any): boolean => isCall(cmd) && cmd.rpc.method === 'c');

    // The fourth call is the next write, and it fails: the three calls before it are still waiting.
    server.onCommand = (cmd: any): any => {
      if (!isCall(cmd)) {
        return undefined;
      }
      return cmd.rpc.retransmit ? { id: cmd.id, rpc: { data: codecCase.encode({ method: cmd.rpc.method }) } } : holdReply;
    };
    failNextWrite(c);
    const fourth: Promise<RpcResult> = quiet(c.rpc('d', codecCase.encode({})));

    const results: RpcResult[] = await Promise.all([...calls, fourth]);
    expect(results.map((r: RpcResult): any => codecCase.decode(r.data)))
      .toEqual([{ method: 'a' }, { method: 'b' }, { method: 'c' }, { method: 'd' }]);
    expect(server.received.filter(isRetransmit).map((cmd: any): string => cmd.rpc.method))
      .toEqual(['a', 'b', 'c', 'd']);
  });

  test('a write error on the calls sent with a reconnect sends all of them again on the next connection', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 2000, minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    holdCalls();
    const calls: Promise<RpcResult>[] = ['a', 'b', 'c'].map(
      (method: string): Promise<RpcResult> => quiet(c.rpc(method, codecCase.encode({})))
    );
    await server.waitFor((cmd: any): boolean => isCall(cmd) && cmd.rpc.method === 'c');

    // The retransmits of a reconnect leave in one batch: its first write fails and takes the connection
    // down, so all three calls wait for the next connection.
    server.onCommand = (cmd: any): any => {
      if (!isCall(cmd)) {
        return undefined;
      }
      return cmd.rpc.retransmit ? { id: cmd.id, rpc: { data: codecCase.encode({ method: cmd.rpc.method }) } } : holdReply;
    };
    c.once('connected', (): void => failNextWrite(c));
    server.closeConnection();

    const results: RpcResult[] = await Promise.all(calls);
    expect(results.map((r: RpcResult): any => codecCase.decode(r.data)))
      .toEqual([{ method: 'a' }, { method: 'b' }, { method: 'c' }]);
    // The reconnect that followed carried the three calls, in message id order.
    const resent: any[] = server.received.filter(isRetransmit);
    expect(resent.map((cmd: any): string => cmd.rpc.method).slice(-3)).toEqual(['a', 'b', 'c']);
    expect(c.state).toBe(State.Connected);
  });

  test('a cancel whose reply is slower than a call timeout is dropped and not sent again', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100, minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    // Calls and cancels get no reply.
    server.onCommand = (cmd: any): any => (isCall(cmd) || isCancel(cmd) ? holdReply : undefined);
    const controller: AbortController = new AbortController();
    let sent: any;
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
      sent = await server.waitFor(isCall);

      controller.abort();
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
      await server.waitFor(isCancel);
      // The reply of the cancel is later than a call timeout: its callbacks are dropped, the cancel is not kept.
      jest.advanceTimersByTime(101);
    } finally {
      jest.useRealTimers();
    }
    expect((c as any)._rpc.cancels).toEqual([]);

    server.closeConnection();
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    // The reconnect carries the call only: the cancel is not sent again.
    await c.rpc('echo', codecCase.encode({}));

    const cancels: any[] = server.received.filter(isCancel);
    expect(cancels).toHaveLength(1);
    expect(cancels[0].rpc.cancel).toBe(sent.rpc.msg_id);
  });

  test('a sent call that reaches its deadline on a silent server sends a cancel for its message id', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100 });
    await connect(c);
    holdCalls();
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    let sent: any;
    try {
      const call: Promise<RpcResult> = quiet(c.rpc('silent', codecCase.encode({})));
      sent = await server.waitFor(isCall);

      // The deadline timer runs its grace callback one tick after the deadline, as the expiry test does.
      jest.advanceTimersByTime(101);
      await expect(call).rejects.toMatchObject({ code: errorCodes.timeout });
    } finally {
      jest.useRealTimers();
    }

    const cancel: any = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
  });

  test('the cancels kept while disconnected are the aborts of the sent calls, at most maxPendingRpc', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 60000, maxReconnectDelay: 60000 });
    await connect(c);
    holdCalls();
    const controllers: AbortController[] = [];
    const calls: Promise<RpcResult>[] = [];
    // The calls hold the slots of maxPendingRpc (32), so one disconnect holds at most that many sent calls.
    for (let i: number = 0; i < 32; i++) {
      const controller: AbortController = new AbortController();
      controllers.push(controller);
      calls.push(quiet(c.rpc('x', codecCase.encode(i), { signal: controller.signal })));
    }
    await until((): boolean => callCommands().length === 32);
    // The reconnect is a minute away: the calls are aborted while the client is disconnected.
    server.closeConnection();
    await until((): boolean => c.state !== State.Connected);

    for (const controller of controllers) {
      controller.abort();
    }
    for (const call of calls) {
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
    }
    const msgIds: number[] = callCommands().map((cmd: any): number => cmd.rpc.msg_id);
    expect((c as any)._rpc.cancels).toEqual(msgIds);

    // Past the bound the oldest cancels are dropped, the newest kept.
    for (let i: number = 0; i < 40; i++) {
      (c as any)._rpc.keepCancel(1000 + i);
    }
    const kept: number[] = [];
    for (let i: number = 8; i < 40; i++) {
      kept.push(1000 + i);
    }
    expect((c as any)._rpc.cancels).toEqual(kept);
  });

  test('a cancel unanswered for one call timeout drops its callbacks and is not sent again', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100 });
    await connect(c);
    // Calls and cancels get no reply.
    server.onCommand = (cmd: any): any => (isCall(cmd) || isCancel(cmd) ? holdReply : undefined);
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const controller: AbortController = new AbortController();
      const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
      await server.waitFor(isCall);
      controller.abort();
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
      await server.waitFor(isCancel);
      expect(Object.keys((c as any)._callbacks)).toHaveLength(2);

      jest.advanceTimersByTime(101);
      expect(Object.keys((c as any)._callbacks)).toHaveLength(0);
    } finally {
      jest.useRealTimers();
    }

    // The transport is lost once the cancel is forgotten: the cancel is not kept for the next connection.
    server.closeConnection();
    await until((): boolean => c.state !== State.Connected);
    expect((c as any)._rpc.cancels).toEqual([]);
    expect(server.received.filter(isCancel)).toHaveLength(1);
  });

  test('after disconnect no timer of the rpc tracker is pending', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100 });
    await connect(c);
    holdCalls();
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      // One call expires, its callbacks are kept for a call timeout; another is aborted, its cancel waits.
      const expired: Promise<RpcResult> = quiet(c.rpc('expired', codecCase.encode({})));
      await server.waitFor(isCall);
      jest.advanceTimersByTime(101);
      await expect(expired).rejects.toMatchObject({ code: errorCodes.timeout });
      const controller: AbortController = new AbortController();
      const aborted: Promise<RpcResult> = quiet(c.rpc('aborted', codecCase.encode({}), { signal: controller.signal }));
      await server.waitFor((cmd: any): boolean => isCall(cmd) && cmd.rpc.method === 'aborted');
      controller.abort();
      await expect(aborted).rejects.toMatchObject({ code: errorCodes.cancelled });

      const pendingBefore: number = jest.getTimerCount();
      c.disconnect();
      // The tracker's timers are gone: the forget timers and the timers of the cancels. The transport keeps
      // its own close timer, so the count drops by the tracker's timers, and does not rise.
      expect(jest.getTimerCount()).toBeLessThan(pendingBefore);
    } finally {
      jest.useRealTimers();
    }
  });

  test('the reply of a call that expired is acknowledged until one call timeout later, then ignored', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100 });
    await connect(c);
    holdCalls();
    // The timers of the client run on a fake clock. The socket keeps its real I/O.
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const call: Promise<RpcResult> = c.rpc('silent', codecCase.encode({}));
      const sent: any = await server.waitFor(isCall);

      jest.advanceTimersByTime(105);
      await expect(call).rejects.toMatchObject({ code: errorCodes.timeout });
      // Past one call timeout after the expiry, the client has forgotten the call.
      jest.advanceTimersByTime(200);
      server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
      server.send({ id: sent.id, rpc: { data: codecCase.encode({}) } });
      await c.rpc('echo', codecCase.encode(1));
      await c.rpc('echo', codecCase.encode(2));
    } finally {
      jest.useRealTimers();
    }

    // The first echo is sent before the late reply arrives, so the second one carries its acknowledgement
    // only: the late reply of the expired call was ignored.
    const echoes: any[] = callCommands().filter((cmd: any): boolean => cmd.rpc.method === 'echo');
    expect(echoes[1].rpc.ack).toEqual([echoes[0].rpc.msg_id]);
  });

  test('calls aborted while the client waits for a connection leave no cancel behind', async (): Promise<void> => {
    const c: Centrifuge = create();
    const controllers: AbortController[] = [];
    const calls: Promise<RpcResult>[] = [];
    // The bound of maxPendingRpc (32) applies to the calls waiting for the connection too.
    for (let i: number = 0; i < 32; i++) {
      const controller: AbortController = new AbortController();
      controllers.push(controller);
      calls.push(quiet(c.rpc('x', codecCase.encode(i), { signal: controller.signal })));
    }
    for (const controller of controllers) {
      controller.abort();
    }

    for (const call of calls) {
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
    }
    // Only tracked calls are cancelled later: a call that was never sent has nothing to cancel.
    expect((c as any)._rpc.cancels).toHaveLength(0);
    expect(server.received).toHaveLength(0);
  });

  test('message ids are strictly increasing and fit 53 bits', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    for (let i: number = 0; i < 5; i++) {
      await c.rpc('echo', codecCase.encode(i));
    }

    const ids: number[] = callCommands().map((cmd: any): number => cmd.rpc.msg_id);
    expect(ids).toHaveLength(5);
    for (let i: number = 0; i < ids.length; i++) {
      expect(Number.isSafeInteger(ids[i])).toBe(true);
      expect(ids[i]).toBeLessThanOrEqual(Number.MAX_SAFE_INTEGER);
      if (i > 0) {
        expect(ids[i]).toBeGreaterThan(ids[i - 1]);
      }
    }
    // The time part is unix seconds, close to the local clock.
    const seconds: number = Math.floor(ids[0] / 2 ** 20);
    expect(Math.abs(seconds - Date.now() / 1000)).toBeLessThan(60);
  });

  test('the message id time part follows the server time of the replies', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    const serverTimeMs: number = Date.now() + 3600 * 1000;
    server.onCommand = (cmd: any): any => (isCall(cmd)
      ? { id: cmd.id, rpc: { data: codecCase.encode({}) }, time: serverTimeMs }
      : undefined);
    await c.rpc('echo', codecCase.encode({}));
    await c.rpc('echo', codecCase.encode({}));

    const second: number = callCommands()[1].rpc.msg_id;
    const seconds: number = Math.floor(second / 2 ** 20);
    expect(Math.abs(seconds - serverTimeMs / 1000)).toBeLessThan(60);
  });

  test('a reply error is surfaced unchanged and the call is not sent again', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd)
      ? { id: cmd.id, error: { code: 3001, message: 'msg_id_reused', temporary: false } }
      : undefined);

    await expect(c.rpc('x', codecCase.encode({}))).rejects.toMatchObject({
      code: 3001,
      message: 'msg_id_reused',
    });
    expect(callCommands()).toHaveLength(1);
  });

  test('a refusal with the new message id signal is sent again with a new id on the corrected clock', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    // The server clock is two minutes behind: the refused id is outside its window.
    const serverTimeMs: number = Date.now() - 120 * 1000;
    server.onCommand = (cmd: any): any => {
      if (!isCall(cmd)) {
        return undefined;
      }
      if (callCommands().length === 1) {
        return { id: cmd.id, error: retryError('new_msg_id', 107, false), time: serverTimeMs };
      }
      return { id: cmd.id, rpc: { data: codecCase.encode({ answer: 'second' }) }, time: serverTimeMs };
    };

    const result: RpcResult = await c.rpc('work', codecCase.encode({}));

    const calls: any[] = callCommands();
    expect(codecCase.decode(result.data)).toEqual({ answer: 'second' });
    expect(calls).toHaveLength(2);
    expect(calls[1].rpc.msg_id).not.toBe(calls[0].rpc.msg_id);
    expect(calls[1].rpc.retransmit).toBeFalsy();
    const seconds: number[] = calls.map((cmd: any): number => Math.floor(cmd.rpc.msg_id / 2 ** 20));
    expect(seconds[0] - seconds[1]).toBeGreaterThan(100);

    await c.rpc('next', codecCase.encode({}));
    const next: any = callCommands()[2];
    expect(Math.abs(Math.floor(next.rpc.msg_id / 2 ** 20) - serverTimeMs / 1000)).toBeLessThan(60);
  });

  test('three refusals with the new message id signal surface the error with its metadata', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: retryError('new_msg_id', 107, false) } : undefined);

    await expect(c.rpc('x', codecCase.encode({}))).rejects.toMatchObject({
      code: 107,
      metadata: { 'x-centrifugo-retry': 'new_msg_id' },
    });
    expect(callCommands()).toHaveLength(3);
  });

  test('a refusal with the later signal is sent again with the same message id after its delay', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 10000 });
    await connect(c);
    const queue: any = (c as any)._rpc.queue;
    server.onCommand = (cmd: any): any => {
      if (!isCall(cmd)) {
        return undefined;
      }
      if (callCommands().length === 1) {
        return { id: cmd.id, error: retryError('later', 100, true) };
      }
      return { id: cmd.id, rpc: { data: codecCase.encode({ answer: 1 }) } };
    };
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const call: Promise<RpcResult> = c.rpc('work', codecCase.encode({}));
      const first: any = await server.waitFor(isCall);
      await until((): boolean => queue.waiting().length === 1);

      jest.advanceTimersByTime(249);
      expect(queue.waiting()).toHaveLength(1);
      // A call timer runs its grace callback one tick after the due time: advance past it.
      jest.advanceTimersByTime(2);
      await until((): boolean => callCommands().length === 2);
      const result: RpcResult = await call;

      expect(codecCase.decode(result.data)).toEqual({ answer: 1 });
      expect(callCommands()[1].rpc.msg_id).toBe(first.rpc.msg_id);
      expect(callCommands()[1].rpc.retransmit).toBe(true);
    } finally {
      jest.useRealTimers();
    }
  });

  test('four refusals with the later signal surface the error, after the delays of 250, 500 and 1000 ms', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 10000 });
    await connect(c);
    const queue: any = (c as any)._rpc.queue;
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: retryError('later', 100, true) } : undefined);
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const call: Promise<RpcResult> = quiet(c.rpc('work', codecCase.encode({})));
      let sends: number = 0;
      for (const delay of [250, 500, 1000]) {
        sends++;
        await until((): boolean => callCommands().length === sends && queue.waiting().length === 1);
        jest.advanceTimersByTime(delay - 1);
        expect(queue.waiting()).toHaveLength(1);
        jest.advanceTimersByTime(2);
      }
      sends++;
      await until((): boolean => callCommands().length === sends && queue.waiting().length === 0);
      await expect(call).rejects.toMatchObject({
        code: 100,
        metadata: { 'x-centrifugo-retry': 'later' },
      });
    } finally {
      jest.useRealTimers();
    }
    expect(callCommands()).toHaveLength(4);
  });

  test('a call waiting for a later attempt goes out with the reconnect, with the same message id', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    const queue: any = (c as any)._rpc.queue;
    server.onCommand = (cmd: any): any => {
      if (!isCall(cmd)) {
        return undefined;
      }
      if (callCommands().length === 1) {
        return { id: cmd.id, error: retryError('later', 100, true) };
      }
      return { id: cmd.id, rpc: { data: codecCase.encode({ answer: 2 }) } };
    };
    const call: Promise<RpcResult> = c.rpc('work', codecCase.encode({}));
    const first: any = await server.waitFor(isCall);
    await until((): boolean => queue.waiting().length === 1);

    server.closeConnection();
    const result: RpcResult = await call;

    // Sent once with the reconnect, whatever the timer of the later attempt does meanwhile.
    expect(codecCase.decode(result.data)).toEqual({ answer: 2 });
    expect(callCommands()).toHaveLength(2);
    expect(callCommands()[1].rpc.msg_id).toBe(first.rpc.msg_id);
    expect(callCommands()[1].rpc.retransmit).toBe(true);
  });

  test('a deadline that passes while a later attempt waits rejects with timeout and sends a cancel', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100 });
    await connect(c);
    const queue: any = (c as any)._rpc.queue;
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: retryError('later', 100, true) } : undefined);
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    let sent: any;
    try {
      const call: Promise<RpcResult> = c.rpc('work', codecCase.encode({}));
      sent = await server.waitFor(isCall);
      await until((): boolean => queue.waiting().length === 1);

      // The deadline timer runs its grace callback one tick after the deadline, as the expiry test does.
      jest.advanceTimersByTime(101);
      await expect(call).rejects.toMatchObject({ code: errorCodes.timeout });
    } finally {
      jest.useRealTimers();
    }

    const cancel: any = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
    expect(callCommands()).toHaveLength(1);
  });

  test('a refusal with the new message id signal that arrives past the deadline is surfaced, nothing is sent again', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100 });
    await connect(c);
    holdCalls();
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const call: Promise<RpcResult> = c.rpc('work', codecCase.encode({}));
      const first: any = await server.waitFor(isCall);

      // The deadline passed; the timer that settles the call has not run yet.
      jest.setSystemTime(Date.now() + 200);
      server.send({ id: first.id, error: retryError('new_msg_id', 107, false) });

      await expect(call).rejects.toMatchObject({ code: 107, metadata: { 'x-centrifugo-retry': 'new_msg_id' } });
    } finally {
      jest.useRealTimers();
    }
    expect(callCommands()).toHaveLength(1);
  });

  test('a call refused again after a reconnect inside its later delay is not sent once it is aborted', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 10000, minReconnectDelay: 10, maxReconnectDelay: 50 });
    await connect(c);
    const queue: any = (c as any)._rpc.queue;
    const controller: AbortController = new AbortController();
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: retryError('later', 100, true) } : undefined);
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const call: Promise<RpcResult> = c.rpc('work', codecCase.encode({}), { signal: controller.signal });
      quiet(call);
      await until((): boolean => callCommands().length === 1 && queue.waiting().length === 1);

      // The reconnect comes before the first later delay ends: the call goes out with it and is refused again.
      server.closeConnection();
      await until((): boolean => c.state !== State.Connected);
      jest.advanceTimersByTime(60);
      await until((): boolean => c.state === State.Connected && callCommands().length === 2 && queue.waiting().length === 1);

      controller.abort();
      const sentBeforeAbort: number = callCommands().length;
      // Past the first later delay: nothing of the aborted call may go out.
      jest.advanceTimersByTime(1000);
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
      await server.waitFor(isCancel);
      expect(callCommands()).toHaveLength(sentBeforeAbort);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a call with a refusal that carries no retry signal is surfaced at once, nothing is sent again', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);
    server.onCommand = (cmd: any): any => (isCall(cmd)
      ? { id: cmd.id, error: { code: 107, message: 'msg_id_invalid', temporary: false } }
      : undefined);
    await expect(c.rpc('x', codecCase.encode({}))).rejects.toMatchObject({ code: 107 });
    expect(callCommands()).toHaveLength(1);

    server.onCommand = (cmd: any): any => (isCall(cmd)
      ? { id: cmd.id, error: { code: 100, message: 'dedup_unavailable', temporary: true } }
      : undefined);
    await expect(c.rpc('y', codecCase.encode({}))).rejects.toMatchObject({ code: 100, temporary: true });
    expect(callCommands()).toHaveLength(2);
  });

  test('calls over a legacy connection are plain and fail on a lost transport as before', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    const c: Centrifuge = create();
    await connect(c);
    holdCalls();
    const call: Promise<RpcResult> = quiet(c.rpc('x', codecCase.encode({})));
    const sent: any = await server.waitFor(isCall);

    expect(sent.rpc.msg_id).toBeUndefined();
    server.closeConnection();
    await expect(call).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    expect(server.received.some(isRetransmit)).toBe(false);
  });

  test('a call tracked on a supported connection rejects with connectionClosed when the next connection is legacy', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    holdCalls();
    const call: Promise<RpcResult> = quiet(c.rpc('x', codecCase.encode({})));
    await server.waitFor(isCall);

    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    server.closeConnection();

    // The call may have run on the lost connection, so it is not reported as unsent (errorCodes.unsupported).
    await expect(call).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    expect(server.received.some(isRetransmit)).toBe(false);
  });

  test('an abort in the connected handler of a legacy connection sends no cancel, the cancel waits for a capable connection', async (): Promise<void> => {
    const c: Centrifuge = create({ minReconnectDelay: 50, maxReconnectDelay: 50 });
    await connect(c);
    holdCalls();
    const controller: AbortController = new AbortController();
    const call: Promise<RpcResult> = c.rpc('work', codecCase.encode({}), { signal: controller.signal });
    const sent: any = await server.waitFor(isCall);

    // The reconnect is to a legacy server: the application aborts in its connected handler, before the
    // waiting calls are handled.
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    c.once('connected', (): void => {
      controller.abort();
    });
    server.closeConnection();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
    expect(server.received.filter(isCancel)).toHaveLength(0);

    // The next capable connection carries the cancel.
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
    server.closeConnection();
    const cancel: any = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
  });

  test('a signal on a legacy connection rejects with unsupported without sending the call', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    const c: Centrifuge = create();
    await connect(c);
    const controller: AbortController = new AbortController();

    await expect(c.rpc('x', codecCase.encode({}), { signal: controller.signal }))
      .rejects.toMatchObject({ code: errorCodes.unsupported });
    expect(callCommands()).toHaveLength(0);
  });

  test('an already aborted signal on a legacy connection rejects with unsupported, and nothing is sent', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    const c: Centrifuge = create();
    await connect(c);
    const controller: AbortController = new AbortController();
    controller.abort();

    await expect(c.rpc('x', codecCase.encode({}), { signal: controller.signal }))
      .rejects.toMatchObject({ code: errorCodes.unsupported });
    expect(callCommands()).toHaveLength(0);
  });

  test('the instance is sent with the connect command when it is set', async (): Promise<void> => {
    const instance: string = 'AbCdEfGhIjKlMnOpQrStUv';
    const c: Centrifuge = create({ instance: instance });
    await connect(c);
    const connectCmd: any = server.received.find((cmd: any): boolean => cmd.connect !== undefined);
    expect(connectCmd.connect.instance).toBe(instance);
  });

  test('no instance is sent when none is set', async (): Promise<void> => {
    const c: Centrifuge = create({ instance: undefined });
    await connect(c);
    const connectCmd: any = server.received.find((cmd: any): boolean => cmd.connect !== undefined);
    expect(connectCmd.connect.instance).toBeUndefined();
  });

  test('an instance that does not match the pattern throws a configuration error', (): void => {
    const invalid: string[] = [
      'short',
      'a'.repeat(21),
      'a'.repeat(65),
      'AbCdEfGhIjKlMnOpQrStU+',
      'AbCdEfGhIjKlMnOpQrStU=',
    ];
    for (const instance of invalid) {
      let thrown: any = null;
      try {
        codecCase.createClient(server.url, { instance: instance });
      } catch (e) {
        thrown = e;
      }
      expect(thrown).toMatchObject({ code: errorCodes.badConfiguration });
    }
  });

  test('explicit undefined or null options take the defaults', async (): Promise<void> => {
    const undefinedOptions: Partial<Options> = { instance: undefined, maxPendingRpc: undefined };
    const nullOptions: Partial<Options> = { instance: null as unknown as string, maxPendingRpc: null as unknown as number };
    const c: Centrifuge = create(undefinedOptions);
    await connect(c);
    expect(() => codecCase.createClient(server.url, nullOptions)).not.toThrow();

    const connectCmd: any = server.received.find((cmd: any): boolean => cmd.connect !== undefined);
    expect(connectCmd.connect.instance).toBeUndefined();
  });

  test('the instance is not written to the debug log', async (): Promise<void> => {
    const instance: string = 'AbCdEfGhIjKlMnOpQrStUv';
    const debugSpy: jest.SpyInstance = jest.spyOn(console, 'debug').mockImplementation((): void => {});
    try {
      const c: Centrifuge = create({ instance: instance, debug: true });
      await connect(c);
      const logged: string = debugSpy.mock.calls.map((args: any[]): string => format(...args)).join('\n');

      expect(debugSpy.mock.calls.some((args: any[]): boolean => args[0] === 'config')).toBe(true);
      expect(logged).not.toContain(instance);
    } finally {
      debugSpy.mockRestore();
    }
  });

  test('a maxPendingRpc below 1 throws a configuration error', (): void => {
    let thrown: any = null;
    try {
      codecCase.createClient(server.url, { maxPendingRpc: 0 });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toMatchObject({ code: errorCodes.badConfiguration });
  });
});
