import { Centrifuge } from './centrifuge';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { FakeCentrifugoServer, FakeCodec, holdReply } from './fakeServer';
import { errorCodes } from './codes';
import { Options, State, TransportName } from './types';
import WebSocket from 'ws';

// Tracked RPC calls: message ids, the retransmit of calls that lost their transport, the queue
// bound, acknowledgements, deadlines, cancels, legacy connections and the instance option. Every
// case runs over both codecs: the fake server speaks JSON or protobuf, and the client is the
// matching Centrifuge class.

// The connect flag of bit 32, as the wire carries it.
const rpcOptionsWireFlag: number = 4294967296;

// RPC data is raw bytes on the protobuf wire, JSON on the JSON wire.
interface CodecCase {
  name: string;
  serverCodec: FakeCodec;
  createClient: (url: string, options?: Partial<Options>) => Centrifuge;
  encode: (value: any) => any;
  decode: (data: any) => any;
}

const bytes = (value: any): Uint8Array => new TextEncoder().encode(JSON.stringify(value));
const fromBytes = (data: Uint8Array): any => JSON.parse(new TextDecoder().decode(data));

const codecCases: CodecCase[] = [
  {
    name: 'JSON',
    serverCodec: 'json',
    createClient: (url: string, options?: Partial<Options>): Centrifuge => new Centrifuge([{
      transport: 'websocket' as TransportName,
      endpoint: url,
    }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      ...options,
    }),
    encode: (value: any): any => value,
    decode: (data: any): any => data,
  },
  {
    name: 'protobuf',
    serverCodec: 'protobuf',
    createClient: (url: string, options?: Partial<Options>): Centrifuge => new ProtobufCentrifuge([{
      transport: 'websocket' as TransportName,
      endpoint: url,
    }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      ...options,
    }),
    encode: bytes,
    decode: fromBytes,
  },
];

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
  promise.catch(() => {});
  return promise;
}

describe.each(codecCases)('tracked RPC calls over $name', (codecCase: CodecCase) => {
  let server: FakeCentrifugoServer;
  const clients: Centrifuge[] = [];

  beforeEach(async () => {
    server = await FakeCentrifugoServer.start(codecCase.serverCodec);
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
  });

  afterEach(async () => {
    for (const c of clients) {
      c.disconnect();
    }
    clients.length = 0;
    await server.close();
  });

  function create(options?: Partial<Options>): Centrifuge {
    const c = codecCase.createClient(server.url, options);
    clients.push(c);
    return c;
  }

  async function connect(c: Centrifuge): Promise<void> {
    const connected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    c.connect();
    await connected;
    expect(c.state).toBe(State.Connected);
  }

  function callCommands(): any[] {
    return server.received.filter(isCall);
  }

  // Holds the replies of the calls, retransmits included, until the test sends them.
  function holdCalls(): void {
    server.onCommand = (cmd: any) => (isCall(cmd) ? holdReply : undefined);
  }

  test('a call that loses its transport runs once and completes with the same reply', async () => {
    const c = create();
    await connect(c);
    const executed: number[] = [];
    server.onCommand = (cmd: any) => {
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

    const call = c.rpc('work', codecCase.encode({ n: 1 }));
    const first = await server.waitFor(isCall);
    server.closeConnection();
    const resent = await server.waitFor(isRetransmit);
    const result = await call;

    expect(codecCase.decode(result.data)).toEqual({ answer: 42 });
    expect(result.cached).toBe(true);
    expect(executed).toEqual([first.rpc.msg_id]);
    expect(resent.rpc.msg_id).toBe(first.rpc.msg_id);
    expect(resent.rpc.retransmit).toBe(true);
    expect(c.state).toBe(State.Connected);
  });

  test('a reply that is not cached has cached false', async () => {
    const c = create();
    await connect(c);
    server.onCommand = (cmd: any) => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    const result = await c.rpc('echo', codecCase.encode({}));
    expect(result.cached).toBe(false);
  });

  test('calls waiting on a lost transport are sent again in message id order', async () => {
    const c = create();
    await connect(c);
    holdCalls();
    const calls = [
      quiet(c.rpc('a', codecCase.encode(1))),
      quiet(c.rpc('b', codecCase.encode(2))),
      quiet(c.rpc('c', codecCase.encode(3))),
    ];
    await server.waitFor(cmd => isCall(cmd) && cmd.rpc.method === 'c');
    const originalIds: number[] = callCommands().map(cmd => cmd.rpc.msg_id);

    server.closeConnection();
    await server.waitFor(cmd => isRetransmit(cmd) && cmd.rpc.method === 'c');
    const resent = server.received.filter(isRetransmit);

    expect(resent.map(cmd => cmd.rpc.msg_id)).toEqual(originalIds);
    expect(resent.map(cmd => cmd.rpc.method)).toEqual(['a', 'b', 'c']);
    expect(originalIds).toEqual([...originalIds].sort((x, y) => x - y));

    for (const cmd of resent) {
      server.send({ id: cmd.id, rpc: { data: codecCase.encode({ method: cmd.rpc.method }) } });
    }
    const results = await Promise.all(calls);
    expect(results.map(r => codecCase.decode(r.data))).toEqual([{ method: 'a' }, { method: 'b' }, { method: 'c' }]);
  });

  test('a call beyond maxPendingRpc rejects with limitExceeded', async () => {
    const c = create({ maxPendingRpc: 2 });
    await connect(c);
    server.onCommand = (cmd: any) => {
      if (!isCall(cmd)) {
        return undefined;
      }
      return cmd.rpc.method.startsWith('hold') ? holdReply : { id: cmd.id, rpc: { data: codecCase.encode({}) } };
    };
    const first = quiet(c.rpc('hold', codecCase.encode(1)));
    quiet(c.rpc('hold2', codecCase.encode(2)));
    await server.waitFor(cmd => isCall(cmd) && cmd.rpc.method === 'hold2');

    await expect(c.rpc('hold', codecCase.encode(3))).rejects.toMatchObject({ code: errorCodes.limitExceeded });
    expect(callCommands()).toHaveLength(2);

    // A settled call frees its place: the next call goes out.
    server.send({ id: server.received.filter(isCall)[0].id, rpc: { data: codecCase.encode({}) } });
    await first;
    await expect(c.rpc('echo', codecCase.encode(4))).resolves.toMatchObject({ cached: false });
  });

  test('the ids of the replies received are sent with the next request', async () => {
    const c = create();
    await connect(c);
    server.onCommand = (cmd: any) => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);

    await c.rpc('echo', codecCase.encode(1));
    const firstId: number = callCommands()[0].rpc.msg_id;
    await c.rpc('echo', codecCase.encode(2));
    await c.rpc('echo', codecCase.encode(3));

    const calls = callCommands();
    expect(calls[0].rpc.ack).toBeUndefined();
    expect(calls[1].rpc.ack).toEqual([firstId]);
    // Each id is sent once.
    expect(calls[2].rpc.ack).toEqual([calls[1].rpc.msg_id]);
  });

  test('a call whose deadline passes while its transport is down rejects with timeout', async () => {
    const c = create({ timeout: 300 });
    await connect(c);
    holdCalls();
    const call = quiet(c.rpc('slow', codecCase.encode({})));
    await server.waitFor(isCall);

    server.closeConnection();
    await server.close();

    await expect(call).rejects.toMatchObject({ code: errorCodes.timeout });
    expect(c.state).not.toBe(State.Connected);
  });

  test('a retransmitted call keeps its original deadline', async () => {
    const c = create({ timeout: 300 });
    await connect(c);
    holdCalls();
    const call = quiet(c.rpc('slow', codecCase.encode({})));
    await server.waitFor(isCall);

    server.closeConnection();
    await server.waitFor(isRetransmit);

    // The retransmit is held too: the call still ends at its original deadline.
    await expect(call).rejects.toMatchObject({ code: errorCodes.timeout });
  });

  test('a call aborted before it is sent rejects with cancelled and sends nothing', async () => {
    const c = create();
    await connect(c);
    const controller = new AbortController();
    controller.abort();

    await expect(c.rpc('x', codecCase.encode({}), { signal: controller.signal }))
      .rejects.toMatchObject({ code: errorCodes.cancelled });
    expect(callCommands()).toHaveLength(0);
  });

  test('a call aborted after it is sent sends a cancel and rejects with cancelled', async () => {
    const c = create();
    await connect(c);
    holdCalls();
    const controller = new AbortController();
    const call = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    const sent = await server.waitFor(isCall);

    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });

    const cancel = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
    expect(cancel.rpc.method).toBeUndefined();
  });

  test('the reply of an aborted call still reaches the server as an ack', async () => {
    const c = create();
    await connect(c);
    holdCalls();
    const controller = new AbortController();
    const call = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    const sent = await server.waitFor(isCall);
    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });

    server.send({ id: sent.id, error: { code: 3001, message: 'cancelled' } });
    server.onCommand = (cmd: any) => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    // The late reply is handled before the reply of the next call, so the call after it carries
    // the acknowledgement.
    await c.rpc('echo', codecCase.encode(1));
    await c.rpc('echo', codecCase.encode(2));

    // The first echo was sent before the late reply arrived, its own reply is acknowledged too.
    const echoes = callCommands().filter(cmd => cmd.rpc.method === 'echo');
    expect(echoes[1].rpc.ack).toEqual([sent.rpc.msg_id, echoes[0].rpc.msg_id]);
  });

  test('a call aborted while its transport is down sends the cancel with the next connection', async () => {
    const c = create({ minReconnectDelay: 200, maxReconnectDelay: 200 });
    await connect(c);
    holdCalls();
    const controller = new AbortController();
    const call = c.rpc('x', codecCase.encode({}), { signal: controller.signal });
    const sent = await server.waitFor(isCall);

    const connecting = new Promise<void>(resolve => c.once('connecting', () => resolve()));
    server.closeConnection();
    await connecting;
    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });

    const cancel = await server.waitFor(isCancel);
    expect(cancel.rpc.cancel).toBe(sent.rpc.msg_id);
    expect(server.received.some(cmd => isRetransmit(cmd) && cmd.rpc.msg_id === sent.rpc.msg_id)).toBe(false);
  });

  test('message ids are strictly increasing and fit 53 bits', async () => {
    const c = create();
    await connect(c);
    server.onCommand = (cmd: any) => (isCall(cmd) ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    for (let i = 0; i < 5; i++) {
      await c.rpc('echo', codecCase.encode(i));
    }

    const ids: number[] = callCommands().map(cmd => cmd.rpc.msg_id);
    expect(ids).toHaveLength(5);
    for (let i = 0; i < ids.length; i++) {
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

  test('the message id time part follows the server time of the replies', async () => {
    const c = create();
    await connect(c);
    const serverTimeMs: number = Date.now() + 3600 * 1000;
    server.onCommand = (cmd: any) => (isCall(cmd)
      ? { id: cmd.id, rpc: { data: codecCase.encode({}) }, time: serverTimeMs }
      : undefined);
    await c.rpc('echo', codecCase.encode({}));
    await c.rpc('echo', codecCase.encode({}));

    const second: number = callCommands()[1].rpc.msg_id;
    const seconds: number = Math.floor(second / 2 ** 20);
    expect(Math.abs(seconds - serverTimeMs / 1000)).toBeLessThan(60);
  });

  test('a reply error is surfaced unchanged and the call is not sent again', async () => {
    const c = create();
    await connect(c);
    server.onCommand = (cmd: any) => (isCall(cmd)
      ? { id: cmd.id, error: { code: 3001, message: 'msg_id_reused', temporary: false } }
      : undefined);

    await expect(c.rpc('x', codecCase.encode({}))).rejects.toMatchObject({
      code: 3001,
      message: 'msg_id_reused',
    });
    expect(callCommands()).toHaveLength(1);
  });

  test('calls over a legacy connection are plain and fail on a lost transport as before', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    const c = create();
    await connect(c);
    holdCalls();
    const call = quiet(c.rpc('x', codecCase.encode({})));
    const sent = await server.waitFor(isCall);

    expect(sent.rpc.msg_id).toBeUndefined();
    server.closeConnection();
    await expect(call).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    expect(server.received.some(isRetransmit)).toBe(false);
  });

  test('a signal on a legacy connection rejects with unsupported without sending the call', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25 };
    const c = create();
    await connect(c);
    const controller = new AbortController();

    await expect(c.rpc('x', codecCase.encode({}), { signal: controller.signal }))
      .rejects.toMatchObject({ code: errorCodes.unsupported });
    expect(callCommands()).toHaveLength(0);
  });

  test('the instance is sent with the connect command when it is set', async () => {
    const instance: string = 'AbCdEfGhIjKlMnOpQrStUv';
    const c = create({ instance: instance });
    await connect(c);
    const connectCmd = server.received.find(cmd => cmd.connect !== undefined);
    expect(connectCmd.connect.instance).toBe(instance);
  });

  test('no instance is sent when none is set', async () => {
    const c = create();
    await connect(c);
    const connectCmd = server.received.find(cmd => cmd.connect !== undefined);
    expect(connectCmd.connect.instance).toBeUndefined();
  });

  test('an instance that does not match the pattern throws a configuration error', () => {
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

  test('a maxPendingRpc below 1 throws a configuration error', () => {
    let thrown: any = null;
    try {
      codecCase.createClient(server.url, { maxPendingRpc: 0 });
    } catch (e) {
      thrown = e;
    }
    expect(thrown).toMatchObject({ code: errorCodes.badConfiguration });
  });
});
