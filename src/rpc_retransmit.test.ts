import { Centrifuge } from './centrifuge';
import { FakeCentrifugoServer, holdReply } from './fakeServer';
import { errorCodes } from './codes';
import { Options, RpcResult, State } from './types';
import { format } from 'util';
import { CodecCase, codecCases, connectClient, disconnectAll, rpcOptionsWireFlag } from './test_codecs';

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

// Polls the condition every 10 ms until it holds, and fails when the limit passes first.
async function waitUntil(condition: () => boolean, limitMs: number): Promise<void> {
  const limit: number = Date.now() + limitMs;
  while (!condition()) {
    if (Date.now() >= limit) {
      throw new Error(`condition not met within ${limitMs} ms`);
    }
    await new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, 10);
    });
  }
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

  test('calls made while disconnected are counted when they are sent, and the excess rejects with limitExceeded', async (): Promise<void> => {
    const c: Centrifuge = create({ maxPendingRpc: 2 });
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    const calls: Promise<RpcResult>[] = [
      quiet(c.rpc('a', codecCase.encode(1))),
      quiet(c.rpc('b', codecCase.encode(2))),
      quiet(c.rpc('c', codecCase.encode(3))),
    ];

    // The calls wait for the connection. Once connected, the two first are tracked, the third is beyond the bound.
    await connect(c);
    await expect(calls[0]).resolves.toMatchObject({ cached: false });
    await expect(calls[1]).resolves.toMatchObject({ cached: false });
    await expect(calls[2]).rejects.toMatchObject({ code: errorCodes.limitExceeded });
    expect(callCommands()).toHaveLength(2);
  });

  test('a burst of calls made before a legacy connection is not bounded and is sent as plain requests', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    const c: Centrifuge = create({ maxPendingRpc: 2 });
    const calls: Promise<RpcResult>[] = [];
    for (let i: number = 0; i < 5; i++) {
      calls.push(c.rpc('echo', codecCase.encode(i)));
    }

    await connect(c);
    await expect(Promise.all(calls)).resolves.toHaveLength(5);
    expect(callCommands()).toHaveLength(5);
    expect(callCommands().every((cmd: any): boolean => cmd.rpc.msg_id === undefined)).toBe(true);
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

  test('an abort while the client is not connected rejects with cancelled at once, and nothing is sent', async (): Promise<void> => {
    const c: Centrifuge = create();
    const controller: AbortController = new AbortController();
    const started: number = Date.now();
    const call: Promise<RpcResult> = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    setTimeout((): void => controller.abort(), 100);

    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
    expect(Date.now() - started).toBeLessThan(2000);
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

  test('the callback of a call that expired on a silent connection is dropped after one call timeout', async (): Promise<void> => {
    const c: Centrifuge = create({ timeout: 100 });
    await connect(c);
    holdCalls();
    const callbackCount = (): number => Object.keys((c as any)._callbacks).length;

    await expect(c.rpc('silent', codecCase.encode({}))).rejects.toMatchObject({ code: errorCodes.timeout });
    // The late reply is still acknowledged right after the expiry, and dropped within a bounded wait
    // of several call timeouts (the timeout is 100 ms).
    expect(callbackCount()).toBe(1);
    await waitUntil((): boolean => callbackCount() === 0, 2000);
  });

  test('calls aborted while the client waits for a connection leave no cancel behind', async (): Promise<void> => {
    const c: Centrifuge = create();
    const controllers: AbortController[] = [];
    const calls: Promise<RpcResult>[] = [];
    for (let i: number = 0; i < 50; i++) {
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

  test('a signal on a legacy connection rejects with unsupported without sending the call', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    const c: Centrifuge = create();
    await connect(c);
    const controller: AbortController = new AbortController();

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
