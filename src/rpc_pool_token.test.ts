import WebSocket from 'ws';
import { errorCodes, disconnectedCodes } from './codes';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { RpcPool, RpcPoolOptions } from './rpc_pool';
import { tokenSourceOf } from './token_source';
import { State, TokenExchange, TokenExchangeContext, TokenExchangeResult, RpcResult } from './types';
import { UnauthorizedError } from './unauthorized_error';
import {
  WaitableFakeServer, bytes, connectClient, disconnectAll, fromBytes, holdReply, rpcOptionsWireFlag, until,
} from './testing/codecs';

// The RPC pool with tokenFrom: the pool's connections take the token of a main client, which runs a token exchange
// against a fake issuer. The main client has its own fake server, and each pool endpoint has one of its own, since a
// fake server answers on its latest socket only.

// A call of the pool: an rpc command, without a message id.
function isCall(cmd: any): boolean {
  return cmd.rpc !== undefined && cmd.rpc.cancel === undefined;
}

function callsOf(server: WaitableFakeServer): any[] {
  return server.received.filter(isCall);
}

function connectsOf(server: WaitableFakeServer): any[] {
  return server.received.filter((cmd: any): boolean => cmd.connect !== undefined);
}

function refreshesOf(server: WaitableFakeServer): any[] {
  return server.received.filter((cmd: any): boolean => cmd.refresh !== undefined);
}

// Echoes the data of a call.
function echo(cmd: any): any {
  if (!isCall(cmd)) {
    return undefined;
  }
  return { id: cmd.id, rpc: { data: cmd.rpc.data } };
}

// The identity of a refresh reply, with its ttl in seconds.
function refreshReply(cmd: any, ttl: number, user: string): any {
  return { id: cmd.id, refresh: { expires: true, ttl: ttl, user: user } };
}

// A connect refused by the server with the token expired code: the client asks for a new token.
function refuseConnect(cmd: any): any {
  return { id: cmd.id, error: { code: 109, message: 'token expired' } };
}

// A refresh refused by the server with the code which keeps the connection (103).
function refuseRefresh(cmd: any): any {
  return { id: cmd.id, error: { code: 103, message: 'identity changed' } };
}

// A refresh which fails temporarily, and is retried by the client.
function temporaryRefreshError(cmd: any): any {
  return { id: cmd.id, error: { code: 100, message: 'temporary', temporary: true } };
}

// Closes the socket of the fake server with a code, so the client reconnects. The fake keeps its socket private.
function closeSocket(server: WaitableFakeServer, code: number): void {
  (server as any).current.close(code);
}

// Resolves once the condition holds, within a real-time budget. The label names the wait in a failure.
async function eventually(condition: () => boolean, budgetMs: number = 6000, label: string = ''): Promise<void> {
  const started: number = Date.now();
  while (!condition()) {
    if (Date.now() - started > budgetMs) {
      throw new Error('condition not reached in ' + budgetMs + ' ms' + (label === '' ? '' : ': ' + label));
    }
    await new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, 10);
    });
  }
}

// Resolves after a real-time pause.
function pause(ms: number): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    setTimeout(resolve, ms);
  });
}

// The number of listeners of an event of a client: the emitter's count, which the typed client does not declare.
function listenerCountOf(c: ProtobufCentrifuge, name: string): number {
  return (c as any).listenerCount(name);
}

// Records the rejections of the requests the source waits with, as they are answered: the requests present now.
function watchRejection(source: any): any[] {
  const rejections: any[] = [];
  for (const waiter of source.waiting.values()) {
    const reject: (error: any) => void = waiter.reject;
    waiter.reject = (error: any): void => {
      rejections.push(error);
      reject(error);
    };
  }
  return rejections;
}

// Runs a function which throws, and returns what it threw.
function thrownBy(fn: () => unknown): any {
  try {
    fn();
  } catch (e) {
    return e;
  }
  return undefined;
}

// Settles a call, and returns its rejection (or 'resolved').
function outcome(promise: Promise<RpcResult>): Promise<any> {
  return promise.then((): string => 'resolved', (error: any): any => error);
}

// Keeps a rejection a test does not await from failing the run.
function quiet<T>(promise: Promise<T>): Promise<T> {
  promise.catch((): void => {});
  return promise;
}

jest.setTimeout(20000);

describe('RpcPool tokenFrom', () => {
  let main: WaitableFakeServer;
  let edgeA: WaitableFakeServer;
  let edgeB: WaitableFakeServer;
  let servers: WaitableFakeServer[] = [];
  let clients: ProtobufCentrifuge[] = [];
  let pools: RpcPool[] = [];
  // The issuer: the access tokens of the exchanges, and the refresh token the app stores.
  let stored: string;
  let issued: number;
  let exchanges: number;
  let failNext: number;
  // The ttl and the user of the main client's refresh replies.
  let mainTtl: number;
  let mainUser: string;
  // Connect of the main client held until released: its command, once held.
  let holdMainConnect: boolean;
  let heldMainConnect: any;
  // Refresh of the main client answered by this hook, when set: it may return holdReply.
  let mainRefreshHook: ((cmd: any) => any) | null;
  // Connects of the edges held until released, in arrival order.
  let holdEdgeConnects: boolean;
  let heldEdgeConnects: any[];
  // The edge endpoints refuse connects carrying this token with the token expired code.
  let refusedToken: string;

  beforeEach(async (): Promise<void> => {
    main = await WaitableFakeServer.start('protobuf');
    edgeA = await WaitableFakeServer.start('protobuf');
    edgeB = await WaitableFakeServer.start('protobuf');
    servers = [main, edgeA, edgeB];
    main.connectResult = { client: 'main-client', version: '0.0.0', ping: 0, flag: rpcOptionsWireFlag };
    // The fleet confirms the identity of the main client's token: alice, as the refresh replies of the main client say.
    edgeA.connectResult = { client: 'edge-a', version: '0.0.0', ping: 0, flag: rpcOptionsWireFlag, user: 'alice' };
    edgeB.connectResult = { client: 'edge-b', version: '0.0.0', ping: 0, flag: rpcOptionsWireFlag, user: 'alice' };
    stored = 'refresh-1';
    issued = 0;
    exchanges = 0;
    failNext = 0;
    mainTtl = 900;
    mainUser = 'alice';
    holdMainConnect = false;
    heldMainConnect = null;
    mainRefreshHook = null;
    holdEdgeConnects = false;
    heldEdgeConnects = [];
    refusedToken = '';
    main.onCommand = (cmd: any): any => {
      if (cmd.connect !== undefined && holdMainConnect) {
        heldMainConnect = cmd;
        return holdReply;
      }
      if (cmd.rpc !== undefined) {
        return { id: cmd.id, rpc: { data: bytes({}) } };
      }
      if (cmd.refresh !== undefined) {
        if (mainRefreshHook !== null) {
          return mainRefreshHook(cmd);
        }
        return refreshReply(cmd, mainTtl, mainUser);
      }
      return undefined;
    };
    for (const edge of [edgeA, edgeB]) {
      edge.onCommand = (cmd: any): any => {
        if (cmd.connect !== undefined && holdEdgeConnects) {
          heldEdgeConnects.push(cmd);
          return holdReply;
        }
        if (cmd.connect !== undefined && cmd.connect.token !== undefined && cmd.connect.token === refusedToken) {
          return refuseConnect(cmd);
        }
        if (cmd.refresh !== undefined) {
          return refreshReply(cmd, 900, 'alice');
        }
        return echo(cmd);
      };
    }
  });

  afterEach(async (): Promise<void> => {
    jest.restoreAllMocks();
    for (const pool of pools) {
      pool.close();
    }
    pools = [];
    disconnectAll(clients);
    clients = [];
    for (const server of servers) {
      await server.close();
    }
    servers = [];
  });

  // The options of the issuer: each exchange rotates the refresh token, and a failure is raised before it is issued.
  function tokenExchange(overrides: Partial<TokenExchange> = {}): TokenExchange {
    return {
      load: (): string => stored,
      store: (refreshToken: string): void => {
        stored = refreshToken;
      },
      exchange: async (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => {
        exchanges++;
        if (failNext > 0) {
          failNext--;
          throw new Error('exchange failed');
        }
        issued++;
        await ctx.rpc('exchange', bytes({ refresh: ctx.refreshToken }));
        return { token: `access-${issued}`, refreshToken: `refresh-${issued + 1}` };
      },
      ...overrides,
    };
  }

  // The main client over the main server, with the token exchange unless the options say otherwise.
  function createMain(options: Record<string, any> = {}, withExchange: boolean = true): ProtobufCentrifuge {
    const c: ProtobufCentrifuge = new ProtobufCentrifuge([{ transport: 'websocket', endpoint: main.url }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      ...(withExchange ? { tokenExchange: tokenExchange() } : {}),
      ...options,
    });
    c.on('error', (): void => {});
    clients.push(c);
    return c;
  }

  function createPool(client: ProtobufCentrifuge, extra: Partial<RpcPoolOptions> = {}): RpcPool {
    const pool: RpcPool = new RpcPool({
      endpoints: (): string[] => [edgeA.url, edgeB.url],
      connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50 },
      tokenFrom: client,
      maxConnectionsPerEndpoint: 1,
      ...extra,
    });
    pools.push(pool);
    return pool;
  }

  // Holds the calls of an edge: each call is answered by the test.
  function holdCalls(edge: WaitableFakeServer, held: any[]): void {
    edge.onCommand = (cmd: any): any => {
      if (isCall(cmd)) {
        held.push(cmd);
        return holdReply;
      }
      if (cmd.connect !== undefined && holdEdgeConnects) {
        heldEdgeConnects.push(cmd);
        return holdReply;
      }
      if (cmd.connect !== undefined && cmd.connect.token !== undefined && cmd.connect.token === refusedToken) {
        return refuseConnect(cmd);
      }
      return echo(cmd);
    };
  }

  // Answers a held call of an edge with its own data.
  function answer(edge: WaitableFakeServer, cmd: any): void {
    edge.send({ id: cmd.id, rpc: { data: cmd.rpc.data } });
  }

  // Lets a held connect of an edge through.
  function releaseEdgeConnect(edge: WaitableFakeServer, cmd: any): void {
    edge.send({ id: cmd.id, connect: edge.connectResult });
  }

  // A main client connected with the token exchange.
  async function connectedMain(options: Record<string, any> = {}): Promise<ProtobufCentrifuge> {
    const c: ProtobufCentrifuge = createMain(options);
    await connectClient(c);
    return c;
  }

  test('refusesCallWhileMainDisconnected', async (): Promise<void> => {
    const never: ProtobufCentrifuge = createMain();
    const idle: RpcPool = createPool(never);
    const failedNever: any = await outcome(idle.rpc('work', bytes({ n: 1 })));
    expect(failedNever).toMatchObject({ code: errorCodes.clientDisconnected, message: 'client disconnected' });
    expect(idle.stats()).toMatchObject({ connections: 0, inflight: 0, queued: 0 });
    const c: ProtobufCentrifuge = await connectedMain();
    const other: RpcPool = createPool(c);
    c.disconnect();
    const failedAfter: any = await outcome(other.rpc('work', bytes({ n: 2 })));
    expect(failedAfter).toMatchObject({ code: errorCodes.clientDisconnected, message: 'client disconnected' });
    expect(connectsOf(edgeA)).toHaveLength(0);
    expect(connectsOf(edgeB)).toHaveLength(0);
    expect(other.stats()).toEqual({ connections: 0, inflight: 0, queued: 0, redirects: 0 });
  });

  test('refusesCallAfterLogoutUntilConnectAttempt', async (): Promise<void> => {
    mainTtl = 1;
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const ended: Promise<number> = new Promise<number>((resolve: (code: number) => void): void => {
      c.once('disconnected', (ctx: any): void => resolve(ctx.code));
    });
    stored = '';
    expect(await ended).toBe(disconnectedCodes.unauthorized);
    const failed: any = await outcome(pool.rpc('work', bytes({ n: 1 })));
    expect(failed).toMatchObject({ code: errorCodes.clientDisconnected, message: 'unauthorized' });
    expect(connectsOf(edgeA)).toHaveLength(0);
    // The connect is held, so the call is seen while the main client connects.
    holdMainConnect = true;
    stored = 'refresh-recovered';
    mainTtl = 900;
    const reconnecting: Promise<void> = connectClient(c);
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const admitted: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
    expect(pool.stats().queued).toBe(1);
    expect(connectsOf(edgeA)).toHaveLength(0);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    await reconnecting;
    expect(fromBytes((await admitted).data)).toEqual({ n: 2 });
  });

  test('holdsUnboundCallUntilMainConnects', async (): Promise<void> => {
    holdMainConnect = true;
    const c: ProtobufCentrifuge = createMain();
    c.connect();
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const timers: jest.SpyInstance = jest.spyOn(global, 'setTimeout');
    const pending: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }));
    // The window runs on immediates, which arm no timer of their own.
    await until((): boolean => false);
    expect(connectsOf(edgeA)).toHaveLength(0);
    expect(pool.stats().queued).toBe(1);
    // The only timer armed in the window is the call's deadline.
    expect(timers.mock.calls).toHaveLength(1);
    timers.mockRestore();
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => held.length === 1)).toBe(true);
    expect(connectsOf(edgeA)).toHaveLength(1);
    answer(edgeA, held[0]);
    expect(fromBytes((await pending).data)).toEqual({ n: 1 });
  });

  test('bindsUnboundCallToIdentityConfirmedAtConnect', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    // Alice's calls go to edge A and bob's to edge B: a fake server answers on its latest socket only, so each pool
    // connection needs an edge of its own for the test to answer the connection of alice after the change.
    const pool: RpcPool = createPool(c, { endpoints: (key: string): string => (key === 'alice' ? edgeA.url : edgeB.url) });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const alice: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 0 }), { endpointKey: 'alice' }));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    // The main client switches to bob: the fleet confirms bob on its connections, and the pool connection of alice is left.
    c.disconnect();
    mainUser = 'bob';
    main.connectResult = { ...main.connectResult, user: 'bob' };
    edgeB.connectResult = { ...edgeB.connectResult, user: 'bob' };
    c.setToken('bob-token');
    holdMainConnect = true;
    c.connect();
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const bobHeld: any[] = [];
    holdCalls(edgeB, bobHeld);
    const bobCall: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 }), { endpointKey: 'bob' }));
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    await eventually((): boolean => bobHeld.length === 1, 6000, 'bindsUnbound: the bob call is written on its connection');
    expect(connectsOf(edgeB).at(-1).connect.token).toBe('bob-token');
    // The alice call was not resent under bob: edge A saw that one call only, and edge B the call of bob.
    expect(callsOf(edgeA)).toHaveLength(1);
    expect(callsOf(edgeB)).toHaveLength(1);
    answer(edgeB, bobHeld[0]);
    expect(fromBytes((await bobCall).data)).toEqual({ n: 1 });
    expect(errors).toEqual([]);
    expect(held).toHaveLength(1);
    // The alice connection is retired and holds its call in flight: the call completes on it, and the connection closes once
    // it settles, with no error. Only the bob connection is left.
    answer(edgeA, held[0]);
    expect(fromBytes((await alice).data)).toEqual({ n: 0 });
    await eventually((): boolean => pool.stats().connections === 1, 6000, 'bindsUnbound: the alice connection closes');
    expect(errors).toEqual([]);
  });

  test('placesBoundCallOnLiveConnectionWhileMainReconnects', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], maxInflightPerConnection: 2 });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    const b: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'b' })));
    expect(await until((): boolean => held.length === 2)).toBe(true);
    holdMainConnect = true;
    closeSocket(main, 3000);
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const cCall: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'c' })));
    answer(edgeA, held[0]);
    await a;
    await pause(50);
    expect(callsOf(edgeA)).toHaveLength(2);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => held.length === 3)).toBe(true);
    expect(connectsOf(edgeA)).toHaveLength(1);
    answer(edgeA, held[2]);
    answer(edgeA, held[1]);
    await b;
    expect(fromBytes((await cCall).data)).toEqual({ n: 'c' });
  });

  test('failsQueuedCallWhenIdentityChanges', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    const b: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'b' })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    mainUser = 'bob';
    await c.refresh('bob-token');
    expect(await b).toMatchObject({ code: errorCodes.clientDisconnected, message: 'identity changed' });
    expect(callsOf(edgeA)).toHaveLength(1);
    answer(edgeA, held[0]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    // The connection of alice closes once its call settled, and no new connection is open for it.
    expect(await until((): boolean => pool.stats().connections === 0)).toBe(true);
    edgeA.connectResult = { ...edgeA.connectResult, user: 'bob' };
    const next: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'c' })));
    expect(await until((): boolean => connectsOf(edgeA).length === 2)).toBe(true);
    expect(connectsOf(edgeA)[1].connect.token).toBe('bob-token');
    expect(await until((): boolean => callsOf(edgeA).length === 2)).toBe(true);
    answer(edgeA, callsOf(edgeA)[1]);
    expect(fromBytes((await next).data)).toEqual({ n: 'c' });
  });

  test('doesNotResendCallLostOnStaleConnection', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'a' })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    mainUser = 'bob';
    await c.refresh('bob-token');
    closeSocket(edgeA, 3000);
    const failed: any = await a;
    // E-loss: the connection's own close reason, not the identity change or the refusal of a token.
    expect([errorCodes.connectionClosed, errorCodes.clientDisconnected]).toContain(failed.code);
    expect(failed.message).not.toBe('identity changed');
    expect(failed.message).not.toBe('token refused');
    expect(failed.message).not.toBe('unauthorized');
    await pause(100);
    expect(callsOf(edgeA)).toHaveLength(1);
    expect(connectsOf(edgeA).filter((cmd: any): boolean => cmd.connect.token === 'access-1')).toHaveLength(1);
  });

  test('resendsCallLostWhileMainReconnectsUnderSameIdentity', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    // One retry is the whole budget: the call completes on its single resend only if the loss used exactly one.
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], retries: 1 });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    holdMainConnect = true;
    closeSocket(main, 3000);
    closeSocket(edgeA, 3000);
    // The pool connection reconnects with its stored token while the main client waits for its connect.
    expect(await until((): boolean => held.length === 2)).toBe(true);
    await eventually((): boolean => heldMainConnect !== null);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    answer(edgeA, held[1]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    expect(callsOf(edgeA)).toHaveLength(2);
  });

  test('failsResendWhenReconnectConfirmsAnotherIdentity', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, {
      endpoints: (): string[] => [edgeA.url],
      connection: { websocket: WebSocket, minReconnectDelay: 0, maxReconnectDelay: 0 },
      retries: 3,
    });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'a' })));
    let aSettled: boolean = false;
    a.then((): void => {
      aSettled = true;
    });
    expect(await until((): boolean => held.length === 1)).toBe(true);
    holdMainConnect = true;
    closeSocket(main, 3000);
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    c.setToken('bob-token');
    const connectsBefore: number = connectsOf(edgeA).length;
    const writtenBefore: number = callsOf(edgeA).length;
    // The fleet closes the pool connection for good, with a code which is not unauthorized.
    edgeA.onCommand = (cmd: any): any => (cmd.connect !== undefined ? holdReply : echo(cmd));
    edgeA.send({ push: { disconnect: { code: 3500, reason: 'terminal' } } });
    await pause(100);
    expect(connectsOf(edgeA).length).toBe(connectsBefore);
    // While the main client connects, A waits in its line: queued, not settled, and no socket opened.
    expect(pool.stats().queued).toBe(1);
    expect(aSettled).toBe(false);
    mainUser = 'bob';
    main.connectResult = { ...main.connectResult, user: 'bob' };
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await a).toMatchObject({ code: errorCodes.clientDisconnected, message: 'identity changed' });
    // Nothing is written once the identity moved on: A is never written on a bob connection.
    expect(callsOf(edgeA)).toHaveLength(writtenBefore);
  });

  test('failsQueuedCallsWhenMainDisconnects', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    const b: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'b' })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    c.disconnect();
    expect(await b).toMatchObject({ code: errorCodes.clientDisconnected, message: 'client disconnected' });
    answer(edgeA, held[0]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    expect(await outcome(pool.rpc('work', bytes({ n: 'c' })))).toMatchObject({ code: errorCodes.clientDisconnected, message: 'client disconnected' });
  });

  test('failsQueuedCallsOnLogout', async (): Promise<void> => {
    mainTtl = 1;
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    const b: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'b' })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    // The session ends as its renewal finds no token to exchange: the main client's own refresh does it.
    stored = '';
    expect(await b).toMatchObject({ code: errorCodes.clientDisconnected, message: 'unauthorized' });
    const writtenBefore: number = callsOf(edgeA).length;
    answer(edgeA, held[0]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    expect(callsOf(edgeA).length).toBe(writtenBefore);
  });

  test('maxQueuedZeroAdmitsOnlyTheOpener', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], maxQueued: 0 });
    const held: any[] = [];
    holdCalls(edgeA, held);
    holdEdgeConnects = true;
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    expect(await outcome(pool.rpc('work', bytes({ n: 'b' })))).toMatchObject({ code: errorCodes.limitExceeded });
    // A's key resolves first: its opener connects, and A waits for the connect in its line.
    await eventually((): boolean => pool.stats().connections === 1, 6000, 'maxQueued zero: the opener opens its connection');
    expect(pool.stats()).toEqual({ connections: 1, inflight: 0, queued: 1, redirects: 0 });
    // The opener's connect is written once its socket opens, which a slow loop may leave for later: the hold is lifted once
    // that connect is held, so no connect of the test goes through unheld.
    await eventually((): boolean => heldEdgeConnects.length === 1, 6000, 'maxQueued zero: the opener connect reaches the edge');
    holdEdgeConnects = false;
    releaseEdgeConnect(edgeA, heldEdgeConnects[0]);
    await pause(200);
    await eventually((): boolean => held.length === 1, 6000, 'maxQueued zero: A is written once the connect is released');
    expect(await outcome(pool.rpc('work', bytes({ n: 'b2' })))).toMatchObject({ code: errorCodes.limitExceeded });
    answer(edgeA, held[0]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    const cCall: Promise<RpcResult> = pool.rpc('work', bytes({ n: 'c' }));
    await eventually((): boolean => held.length === 2, 6000, 'maxQueued zero: C is placed after A settles');
    answer(edgeA, held[1]);
    expect(fromBytes((await cCall).data)).toEqual({ n: 'c' });
  });

  test('maxQueuedBoundsCallsWaitingForASlot', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], maxQueued: 2, retries: 3 });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    await eventually((): boolean => held.length === 1);
    const b: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'b' })));
    const cc: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'c' })));
    expect(await outcome(pool.rpc('work', bytes({ n: 'd' })))).toMatchObject({ code: errorCodes.limitExceeded });
    expect(pool.stats().queued).toBe(2);
    holdEdgeConnects = true;
    closeSocket(edgeA, 3000);
    await pause(100);
    expect(pool.stats().queued).toBe(3);
    await eventually((): boolean => heldEdgeConnects.length === 1);
    holdEdgeConnects = false;
    releaseEdgeConnect(edgeA, heldEdgeConnects[0]);
    // The resend of A joins the back of the line, behind B and C: they are written in arrival order, then A.
    await eventually((): boolean => held.length === 2);
    expect(fromBytes(held[1].rpc.data)).toEqual({ n: 'b' });
    answer(edgeA, held[1]);
    await eventually((): boolean => held.length === 3);
    expect(fromBytes(held[2].rpc.data)).toEqual({ n: 'c' });
    answer(edgeA, held[2]);
    await eventually((): boolean => held.length === 4);
    expect(fromBytes(held[3].rpc.data)).toEqual({ n: 'a' });
    answer(edgeA, held[3]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    expect(fromBytes((await b).data)).toEqual({ n: 'b' });
    expect(fromBytes((await cc).data)).toEqual({ n: 'c' });
  });

  test('lendsOnlyTheConfirmedToken', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    c.setToken('access-x');
    const result: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => connectsOf(edgeA).length === 1)).toBe(true);
    expect(connectsOf(edgeA)[0].connect.token).toBe('access-1');
    expect(await until((): boolean => callsOf(edgeA).length === 1)).toBe(true);
    expect(fromBytes((await result).data)).toEqual({ n: 1 });
    expect(connectsOf(edgeA).some((cmd: any): boolean => cmd.connect.token === 'access-x')).toBe(false);
    expect(connectsOf(edgeB)).toHaveLength(0);
  });

  test('retiresConnectionWhoseIdentityDiffersFromTheMain', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    edgeA.connectResult = { ...edgeA.connectResult, user: 'other' };
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const refreshesBefore: number = refreshesOf(main).length;
    expect(await outcome(pool.rpc('work', bytes({ n: 1 }), { timeout: 400 }))).toMatchObject({ code: errorCodes.timeout });
    expect(errors.some((ctx: any): boolean => ctx.error?.reason === 'identity mismatch')).toBe(true);
    expect(callsOf(edgeA)).toHaveLength(0);
    expect(connectsOf(edgeA)).toHaveLength(1);
    expect(connectsOf(edgeB)).toHaveLength(0);
    expect(refreshesOf(main).length).toBe(refreshesBefore);
    // The main client confirms a token the fleet agrees with: the next call's line drains then.
    edgeA.connectResult = { ...edgeA.connectResult, user: 'alice' };
    const held: any[] = [];
    holdCalls(edgeA, held);
    const second: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 }), { timeout: 4000 }));
    await c.refresh('access-2');
    expect(await until((): boolean => held.length === 1)).toBe(true);
    expect(connectsOf(edgeA)).toHaveLength(2);
    answer(edgeA, held[0]);
    expect(fromBytes((await second).data)).toEqual({ n: 2 });
  });

  test('oneRenewalPerConfirmedTokenPerMainConnection', async (): Promise<void> => {
    // The reconnects of the main client and of the pool connections wait no backoff: the case waits across them.
    const c: ProtobufCentrifuge = await connectedMain({ minReconnectDelay: 0, maxReconnectDelay: 0 });
    refusedToken = 'access-1';
    // One connection per edge: each edge answers on its own socket, so both connections reconnect when both are closed.
    const pool: RpcPool = createPool(c, {
      endpoints: (): string[] => [edgeA.url, edgeB.url],
      maxConnectionsPerEndpoint: 1,
      connection: { websocket: WebSocket, minReconnectDelay: 0, maxReconnectDelay: 0 },
    });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    const second: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
    await eventually((): boolean => callsOf(edgeA).length === 1 && callsOf(edgeB).length === 1, 8000, 'phase one: both connections carry their call after the renewal');
    expect(exchanges).toBe(2);
    expect(fromBytes((await first).data)).toEqual({ n: 1 });
    expect(fromBytes((await second).data)).toEqual({ n: 2 });
    // Phase two: the renewed token is refused too, and the issuer gives it back. The renewal it asks for brings the refused
    // token back (refusedAgain): the token requests of both connections are answered unauthorized, the source is spent
    // (3.24), and it starts no second renewal. Both connections end as unauthorized, one error each (R3.1), and no
    // connection opens again.
    refusedToken = 'access-2';
    issued = 1;
    closeSocket(edgeA, 3000);
    closeSocket(edgeB, 3000);
    await eventually((): boolean => exchanges === 3, 8000, 'phase two: the renewal of the refused token runs once');
    await eventually((): boolean => errors.filter((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized).length === 2, 8000, 'phase two: both connections end as unauthorized');
    expect(errors.filter((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized)).toHaveLength(2);
    expect(pool.stats().connections).toBe(0);
    const connectsBefore: number = connectsOf(edgeA).length;
    // A later call waits for no connection: it is refused at once, with no open and no token request.
    const fourth: any = await outcome(pool.rpc('work', bytes({ n: 4 }), { timeout: 4000 }));
    expect(fourth).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    expect(exchanges).toBe(3);
    expect(connectsOf(edgeA)).toHaveLength(connectsBefore);
  });

  test('noRenewalWhileTheMainRefreshIsInProgress', async (): Promise<void> => {
    let getterCalls: number = 0;
    let answerGetter: ((token: string) => void) | null = null;
    const c: ProtobufCentrifuge = new ProtobufCentrifuge([{ transport: 'websocket', endpoint: main.url }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      getToken: (): Promise<string> => {
        getterCalls++;
        if (getterCalls === 1) {
          return Promise.resolve('access-1');
        }
        return new Promise<string>((resolve: (token: string) => void): void => {
          answerGetter = resolve;
        });
      },
    });
    c.on('error', (): void => {});
    clients.push(c);
    main.connectResult = { ...main.connectResult, expires: true, ttl: 1, user: 'alice' };
    await connectClient(c);
    main.connectResult = { ...main.connectResult, expires: false, ttl: 0 };
    refusedToken = 'access-1';
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    await eventually((): boolean => getterCalls === 2, 8000);
    const result: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    await pause(100);
    expect(getterCalls).toBe(2);
    expect(answerGetter).not.toBe(null);
    (answerGetter as unknown as (token: string) => void)('access-2');
    expect(fromBytes((await result).data)).toEqual({ n: 1 });
    expect(getterCalls).toBe(2);
    // The busy window started no renewal of the source: r stays null, so a later renewal for the same token is not blocked.
    expect((tokenSourceOf(c) as any).renewedFor).toBe(null);
    expect(connectsOf(edgeA).at(-1).connect.token).toBe('access-2');
  });

  test('renewalRefusedWith103FailsTheWaiters', async (): Promise<void> => {
    let getterCalls: number = 0;
    const c: ProtobufCentrifuge = new ProtobufCentrifuge([{ transport: 'websocket', endpoint: main.url }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      getToken: (): Promise<string> => {
        getterCalls++;
        return Promise.resolve(getterCalls === 1 ? 'tok-1' : 'other-token');
      },
    });
    c.on('error', (): void => {});
    clients.push(c);
    // The fleet confirms alice for the main client's token, as the edges do.
    main.connectResult = { ...main.connectResult, user: 'alice' };
    await connectClient(c);
    refusedToken = 'tok-1';
    mainRefreshHook = (cmd: any): any => refuseRefresh(cmd);
    // The hold of the endpoint after the connection ends as unauthorized is 1000 ms here, so it is seen while it lasts.
    const pool: RpcPool = createPool(c, {
      endpoints: (): string[] => [edgeA.url],
      connection: { websocket: WebSocket, minReconnectDelay: 1000, maxReconnectDelay: 1000 },
    });
    const holdOf: () => unknown = (): unknown => (pool as any).endpoints.get(edgeA.url)?.hold ?? null;
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    // The opener and the call behind it wait for the connection's token; the 103 refuses the renewal of the refused token.
    const opener: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'a' }), { timeout: 4000 }));
    const behind: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'b' }), { timeout: 4000 }));
    // The source is spent: both fail as refused, at once, and neither is written.
    expect(await opener).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    expect(await behind).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    expect(refreshesOf(main)).toHaveLength(1);
    // The connection ends as unauthorized, once, and its endpoint is held.
    await eventually((): boolean => errors.some((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized), 8000);
    expect(errors.filter((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized)).toHaveLength(1);
    expect(holdOf()).not.toBeNull();
    expect(c.state).toBe(State.Connected);
    expect(callsOf(edgeA)).toHaveLength(0);
    // A call behind the spent source is refused at once, with no open and no token request.
    const connectsBefore: number = connectsOf(edgeA).length;
    const later: any = await outcome(pool.rpc('work', bytes({ n: 'c' }), { timeout: 4000 }));
    expect(later).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    expect(connectsOf(edgeA)).toHaveLength(connectsBefore);
    expect(refreshesOf(main)).toHaveLength(1);
    // Second phase: the main client reconnects with a token the fleet accepts. The connect clears the spent mark, and the
    // next call opens a connection after the endpoint's hold, and is written.
    refusedToken = 'none';
    const mainConnects: number = connectsOf(main).length;
    closeSocket(main, 3000);
    await eventually((): boolean => connectsOf(main).length > mainConnects && c.state === State.Connected, 8000);
    const next: RpcResult = await pool.rpc('work', bytes({ n: 'd' }), { timeout: 4000 });
    expect(fromBytes(next.data)).toEqual({ n: 'd' });
    expect(refreshesOf(main)).toHaveLength(1);
  });

  test('failedRefreshCallReevaluatesTheWaiters', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    // The renewals of the main client: its refresh commands other than the explicit one. The connect has sent one already.
    const renewals: () => number = (): number => refreshesOf(main).filter((cmd: any): boolean => cmd.refresh.token !== 'tY').length;
    const base: number = renewals();
    refusedToken = 'access-1';
    const held: any[] = [];
    let explicitHeld: any = null;
    mainRefreshHook = (cmd: any): any => {
      if (cmd.refresh.token === 'tY' && explicitHeld === null) {
        explicitHeld = cmd;
        return holdReply;
      }
      return refreshReply(cmd, mainTtl, mainUser);
    };
    const explicit: Promise<any> = quiet(c.refresh('tY'));
    expect(await until((): boolean => explicitHeld !== null)).toBe(true);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    holdCalls(edgeA, held);
    const result: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    await pause(100);
    // No renewal starts while the explicit refresh is in flight.
    expect(renewals()).toBe(base);
    main.send({ id: explicitHeld.id, error: { code: 100, message: 'temporary', temporary: true } });
    await explicit.catch((): void => {});
    expect(await until((): boolean => held.length === 1)).toBe(true);
    expect(renewals()).toBe(base + 1);
    answer(edgeA, held[0]);
    expect(fromBytes((await result).data)).toEqual({ n: 1 });
  });

  test('failedTimerRenewalLeavesWaitersToTheMainRetry', async (): Promise<void> => {
    let getterCalls: number = 0;
    const c: ProtobufCentrifuge = new ProtobufCentrifuge([{ transport: 'websocket', endpoint: main.url }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      getToken: (): Promise<string> => {
        getterCalls++;
        return Promise.resolve(getterCalls === 1 ? 'tok-1' : 'tok-2');
      },
    });
    c.on('error', (): void => {});
    clients.push(c);
    // The renewed token is the identity of alice, as the connect confirmed: a refresh which changed it would retire the connection.
    main.connectResult = { ...main.connectResult, user: 'alice' };
    await connectClient(c);
    refusedToken = 'tok-1';
    let refreshes: number = 0;
    mainRefreshHook = (cmd: any): any => {
      refreshes++;
      return refreshes === 1 ? temporaryRefreshError(cmd) : refreshReply(cmd, mainTtl, mainUser);
    };
    // The call waits out the client's retry of its refresh: its deadline is the connection timeout, which is longer here.
    const pool: RpcPool = createPool(c, {
      endpoints: (): string[] => [edgeA.url],
      connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50, timeout: 20000 },
    });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const result: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    // The client retries its refresh after a backoff of five to ten seconds.
    await eventually((): boolean => held.length === 1, 15000);
    expect(refreshes).toBe(2);
    answer(edgeA, held[0]);
    expect(fromBytes((await result).data)).toEqual({ n: 1 });
  });

  test('connectAnswersTheWaiterWithoutAPoolCall', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    // The main client's transport drops and its reconnect is held. The pool connection drops too, and its reconnect is
    // refused the token it holds: its request for a new one waits in the source while the main client connects.
    holdMainConnect = true;
    refusedToken = 'access-1';
    closeSocket(main, 3000);
    closeSocket(edgeA, 3000);
    const source: any = tokenSourceOf(c);
    expect(await until((): boolean => heldMainConnect !== null && source.waiting.size === 1)).toBe(true);
    // The application removed the main client's listeners in that window: the waiter is still answered at the connect.
    c.removeAllListeners();
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => source.waiting.size === 0)).toBe(true);
    // The waiter was refused the token it held, so the connect renews it, and the connection reconnects with the renewal.
    expect(await until((): boolean => connectsOf(edgeA).some((cmd: any): boolean => cmd.connect.token === 'access-2'))).toBe(true);
    expect(exchanges).toBe(2);
    // The call lost with the connection is resent on it, with no call of the application.
    expect(await until((): boolean => held.length === 2)).toBe(true);
    answer(edgeA, held[1]);
    expect(fromBytes((await first).data)).toEqual({ n: 1 });
  });

  test('sessionEndDoesNotKeepRefusingAfterAConnectAttempt', async (): Promise<void> => {
    mainTtl = 1;
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    c.removeAllListeners();
    holdMainConnect = true;
    stored = '';
    await eventually((): boolean => c.state === State.Disconnected, 8000);
    expect(await outcome(pool.rpc('work', bytes({ n: 1 })))).toMatchObject({ code: errorCodes.clientDisconnected });
    expect(connectsOf(edgeA)).toHaveLength(0);
    stored = 'refresh-recovered';
    mainTtl = 900;
    c.connect();
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const admitted: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
    expect(pool.stats().queued).toBe(1);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(fromBytes((await admitted).data)).toEqual({ n: 2 });
  });

  test('forgottenWaiterIsAnsweredUnauthorized', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const held: any[] = [];
    holdCalls(edgeA, held);
    quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    // The pool connection's reconnect is refused for the token: its request waits while the main client connects.
    refusedToken = 'access-1';
    holdMainConnect = true;
    closeSocket(main, 3000);
    closeSocket(edgeA, 3000);
    const source: any = tokenSourceOf(c);
    expect(await until((): boolean => source.waiting.size === 1)).toBe(true);
    const rejections: any[] = watchRejection(source);
    pool.close();
    expect(source.waiting.size).toBe(0);
    expect(rejections).toHaveLength(1);
    expect(rejections[0]).toBeInstanceOf(UnauthorizedError);
  });

  test('sourceReferencesOnlyPoolsWithWaitingCalls', async (): Promise<void> => {
    holdMainConnect = true;
    const c: ProtobufCentrifuge = createMain();
    c.connect();
    await eventually((): boolean => heldMainConnect !== null, 6000, 'sourceReferences: the main client connect is held');
    const source: any = tokenSourceOf(c);
    const poolA: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    // One call in flight per connection: a call behind it waits in the line.
    const poolB: RpcPool = createPool(c, { endpoints: (): string[] => [edgeB.url], maxInflightPerConnection: 1 });
    const heldB: any[] = [];
    holdCalls(edgeB, heldB);
    const bCall: Promise<RpcResult> = quiet(poolB.rpc('work', bytes({ n: 1 })));
    // Before the connect the listening set is {B}: its call waits in its line once the key resolves, and A is idle.
    // The source keeps the listener of each pool, which is the pool's own main listener.
    const listenerA: object = (poolA as any).mainListener;
    const listenerB: object = (poolB as any).mainListener;
    await eventually((): boolean => source.pools.size === 1, 6000, 'sourceReferences: B listens to the source');
    expect(source.pools.has(listenerB)).toBe(true);
    expect(source.pools.has(listenerA)).toBe(false);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    await eventually((): boolean => heldB.length === 1, 6000, 'sourceReferences: the connect places B\'s call');
    // The connect placed B's call, so the set is empty again, and A was never drained.
    expect(source.pools.size).toBe(0);
    expect((poolA as any).drainedKeys).toBe(0);
    answer(edgeB, heldB[0]);
    await bCall;
    // The next call is in flight, and the one behind it waits for the window: B is listened to again.
    const inFlight: Promise<RpcResult> = quiet(poolB.rpc('work', bytes({ n: 2 })));
    await eventually((): boolean => heldB.length === 2, 6000, 'sourceReferences: the next call is in flight');
    const waiting: Promise<any> = outcome(poolB.rpc('work', bytes({ n: 3 })));
    await eventually((): boolean => source.pools.size === 1, 6000, 'sourceReferences: B listens while its call waits');
    expect(source.pools.has(listenerB)).toBe(true);
    // A pool closed with a waiting call leaves the set, and the waiting call settles cancelled.
    poolB.close();
    expect(source.pools.size).toBe(0);
    expect(await waiting).toMatchObject({ code: errorCodes.cancelled });
    await inFlight.catch((): void => {});
  });

  test('mainEventDrainsOnlyKeysWithWaitingCalls', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, {
      endpoints: (): string[] => [edgeA.url],
      maxInflightPerConnection: 1000,
      maxQueued: 1000,
    });
    for (let i: number = 0; i < 1000; i++) {
      await pool.rpc('work', bytes({ i: i }), { endpointKey: 'k' + i });
    }
    c.disconnect();
    holdMainConnect = true;
    c.connect();
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const held: any[] = [];
    holdCalls(edgeA, held);
    const waiting: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ key: 'k999' }), { endpointKey: 'k999' }));
    const drainedBefore: number = (pool as any).drainedKeys;
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => held.length === 1)).toBe(true);
    expect((pool as any).drainedKeys - drainedBefore).toBe(1);
    answer(edgeA, held[0]);
    await waiting;
  });

  test('connectionEventDrainsOnlyKeysWithWaitingCalls', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, {
      endpoints: (): string[] => [edgeA.url],
      maxInflightPerConnection: 1000,
      maxQueued: 1000,
    });
    await pool.rpc('work', bytes({ warm: true }), { endpointKey: 'warm' });
    const before: number = (pool as any).drainedKeys;
    const calls: Promise<RpcResult>[] = [];
    for (let i: number = 0; i < 1000; i++) {
      calls.push(pool.rpc('work', bytes({ i: i }), { endpointKey: 'k' + i }));
    }
    await Promise.all(calls);
    expect((pool as any).drainedKeys - before).toBe(0);
  });

  test('staleConnectionIsRetiredWhenItReconnects', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    await pool.rpc('work', bytes({ n: 1 }));
    expect(connectsOf(edgeA)).toHaveLength(1);
    mainUser = 'bob';
    await c.refresh('bob-token');
    closeSocket(edgeA, 3000);
    await pause(200);
    expect(connectsOf(edgeA)).toHaveLength(1);
    expect(pool.stats().connections).toBe(0);
  });

  test('staticTokenRefusalFailsWithoutAGetter', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain({ token: 'static-token' }, false);
    main.connectResult = { ...main.connectResult, user: 'alice' };
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50 } });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const inFlight: Promise<any> = outcome(pool.rpc('work', bytes({ n: 1 }), { timeout: 4000 }));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    const behind: Promise<any> = outcome(pool.rpc('work', bytes({ n: 2 }), { timeout: 4000 }));
    // The fleet drops the connection with a reconnectable code and refuses the static token on the reconnect. The main client
    // has no getter, so the refusal spends the source (3.24): the lost call and the call behind it fail as refused, and the
    // lost call is written once in total.
    refusedToken = 'static-token';
    closeSocket(edgeA, 3000);
    expect(await inFlight).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    expect(await behind).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    await eventually((): boolean => errors.some((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized), 8000);
    expect(errors.filter((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized)).toHaveLength(1);
    expect(refreshesOf(main)).toHaveLength(0);
    // No connection opens after the refused one, and the call after it fails as refused at once, with no getter called.
    const connectsBefore: number = connectsOf(edgeA).length;
    expect(await outcome(pool.rpc('work', bytes({ n: 3 }), { timeout: 4000 }))).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    expect(connectsOf(edgeA)).toHaveLength(connectsBefore);
    expect(callsOf(edgeA)).toHaveLength(1);
  });

  test('closeSettlesAndUnlistens', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url, edgeB.url] });
    const heldA: any[] = [];
    const heldB: any[] = [];
    holdCalls(edgeA, heldA);
    holdCalls(edgeB, heldB);
    const inFlight: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'in-flight' })));
    const other: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'other' })));
    expect(await until((): boolean => heldA.length === 1 && heldB.length === 1)).toBe(true);
    const queued: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'queued' })));
    // The main client's transport goes away, and the connection of edge B loses its token: its request waits for the connect.
    refusedToken = 'access-1';
    holdMainConnect = true;
    closeSocket(main, 3000);
    closeSocket(edgeB, 3000);
    const source: any = tokenSourceOf(c);
    await eventually((): boolean => source.waiting.size === 1);
    expect(source.pools.size).toBe(1);
    const rejections: any[] = watchRejection(source);
    pool.close();
    expect(rejections).toHaveLength(1);
    expect(rejections[0]).toBeInstanceOf(UnauthorizedError);
    expect(await queued).toMatchObject({ code: errorCodes.cancelled });
    expect(await inFlight).toMatchObject({ code: errorCodes.connectionClosed });
    // The call on the edge B connection was lost when its connection dropped, and waits for a connection: close cancels it.
    expect(await other).toMatchObject({ code: errorCodes.cancelled });
    expect(source.waiting.size).toBe(0);
    expect(source.pools.size).toBe(0);
  });

  test('identityChangeRetiresAndCompletesInFlight', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    edgeA.connectResult = { ...edgeA.connectResult, expires: true, ttl: 2 };
    // The next call has its own key, so its connection opens on edge B, while the calls in flight stay on edge A (window 2).
    const pool: RpcPool = createPool(c, {
      endpoints: (key: string): string => (key === 'next' ? edgeB.url : edgeA.url),
      maxInflightPerConnection: 2,
    });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    // B is in flight on the same connection, and the fleet holds its reply past the token's expiry.
    const b: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'b' }), { timeout: 4000 }));
    expect(await until((): boolean => held.length === 2)).toBe(true);
    mainUser = 'bob';
    await c.refresh('bob-token');
    edgeA.connectResult = { ...edgeA.connectResult, user: 'bob' };
    edgeB.connectResult = { ...edgeB.connectResult, user: 'bob' };
    const next: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'next' }), { endpointKey: 'next' }));
    await eventually((): boolean => connectsOf(edgeB).length === 1);
    expect(connectsOf(edgeB)[0].connect.token).toBe('bob-token');
    // The call in flight completes on its connection, before that connection's token expires.
    answer(edgeA, held[0]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    const refreshesBefore: number = refreshesOf(main).length;
    const exchangesBefore: number = exchanges;
    // The expiry of the retired connection's token is answered unauthorized by the source: no renewal of the main client,
    // no call of its getter or exchange. The connection ends as unauthorized, and reports nothing and holds no open.
    expect([errorCodes.connectionClosed, errorCodes.clientDisconnected]).toContain((await b).code);
    expect(refreshesOf(main).length).toBe(refreshesBefore);
    expect(exchanges).toBe(exchangesBefore);
    expect(errors).toEqual([]);
    // Only the connection of edge B is left, and the call behind the change went there with bob's token.
    expect(pool.stats().connections).toBe(1);
    expect(callsOf(edgeA)).toHaveLength(2);
    expect(await until((): boolean => callsOf(edgeB).length === 1)).toBe(true);
    answer(edgeB, callsOf(edgeB)[0]);
    expect(fromBytes((await next).data)).toEqual({ n: 'next' });
  });

  test('replacedTokenRequestEndsNothing', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    let renewalHeld: any = null;
    mainRefreshHook = (cmd: any): any => {
      if (renewalHeld === null) {
        renewalHeld = cmd;
        return holdReply;
      }
      return refreshReply(cmd, mainTtl, mainUser);
    };
    // P1 connects with an expiring token, so its refresh at expiry is its first token request (2.5 step 9 starts the renewal).
    edgeA.connectResult = { ...edgeA.connectResult, expires: true, ttl: 2 };
    const held: any[] = [];
    holdCalls(edgeA, held);
    const result: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    // The connection does not expire again: its reconnect is the replacing request, and it carries no further refresh.
    edgeA.connectResult = { ...edgeA.connectResult, expires: false, ttl: 0 };
    const source: any = tokenSourceOf(c);
    await eventually((): boolean => renewalHeld !== null, 8000);
    const rejections: any[] = watchRejection(source);
    // The fleet drops the pool connection, and refuses its reconnect: it asks for a token again, which replaces the first.
    refusedToken = 'access-1';
    closeSocket(edgeA, 3000);
    expect(await until((): boolean => rejections.length === 1)).toBe(true);
    expect(rejections[0]).toBeInstanceOf(UnauthorizedError);
    await pause(100);
    main.send({ id: renewalHeld.id, refresh: { expires: true, ttl: mainTtl, user: 'alice' } });
    refusedToken = '';
    expect(await until((): boolean => held.length === 2)).toBe(true);
    answer(edgeA, held[1]);
    expect(fromBytes((await result).data)).toEqual({ n: 1 });
    expect(errors).toEqual([]);
    const endpoint: any = (pool as any).endpoints.get(edgeA.url);
    expect(endpoint === undefined || endpoint.hold === null).toBe(true);
    expect(connectsOf(edgeA).at(-1).connect.token).toBe('access-2');
  });

  test('stateInvalidatedDoesNotLendThePresentedTokenBack', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    expect(connectsOf(edgeA)[0].connect.token).toBe('access-1');
    const refreshesBefore: number = refreshesOf(main).length;
    const connectsBefore: number = connectsOf(edgeA).length;
    // The fleet invalidates the state of P1: its client clears its token and reconnects with a token request. The main client
    // still confirms access-1, which P1 presented last (R3.3), so the source renews and does not lend access-1 back.
    edgeA.send({ push: { disconnect: { code: disconnectedCodes.stateInvalidated, reason: 'state' } } });
    expect(await until((): boolean => refreshesOf(main).length === refreshesBefore + 1)).toBe(true);
    // The renewal brings access-2, and P1 connects with it. No connect carries access-1 again after the invalidation.
    await eventually((): boolean => connectsOf(edgeA).some((cmd: any): boolean => cmd.connect.token === 'access-2'), 8000);
    expect(connectsOf(edgeA).slice(connectsBefore).some((cmd: any): boolean => cmd.connect.token === 'access-1')).toBe(false);
  });

  test('resendOfAnOldBindingFailsBeforeItJoinsALine', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], retries: 3 });
    // A is refused with the later signal: its slot is free, and its resend waits a delay near its maximum.
    jest.spyOn(Math, 'random').mockReturnValue(0.99);
    let edgeCalls: number = 0;
    const held: any[] = [];
    edgeA.onCommand = (cmd: any): any => {
      if (cmd.connect !== undefined && holdEdgeConnects) {
        heldEdgeConnects.push(cmd);
        return holdReply;
      }
      if (!isCall(cmd)) {
        return echo(cmd);
      }
      edgeCalls++;
      if (edgeCalls === 1) {
        return { id: cmd.id, error: { code: 100, message: 'later', metadata: { 'x-centrifugo-retry': 'later' } } };
      }
      held.push(cmd);
      return holdReply;
    };
    const a: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'a' })));
    await eventually((): boolean => edgeCalls === 1);
    // C takes the slot A freed; B waits in the line behind it.
    const cCall: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'c' })));
    await eventually((): boolean => held.length === 1);
    const b: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'b' })));
    // A waits its later delay, which holds no slot and is not a call in a line: only B counts in queued.
    expect(pool.stats()).toEqual({ connections: 1, inflight: 1, queued: 1, redirects: 0 });
    // The identity moves on before A's delay ends. The notification fails B, and D opens a connection of bob's own:
    // C's connection is retired at once, its slot is C's to finish.
    mainUser = 'bob';
    await c.refresh('bob-token');
    expect(await b).toMatchObject({ code: errorCodes.clientDisconnected, message: 'identity changed' });
    edgeA.connectResult = { ...edgeA.connectResult, user: 'bob' };
    // C completes on its connection: the fake server answers on its latest socket only, so C's reply comes before D's connection.
    answer(edgeA, held[0]);
    expect(fromBytes((await cCall).data)).toEqual({ n: 'c' });
    holdEdgeConnects = true;
    const d: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'd' })));
    await eventually((): boolean => heldEdgeConnects.length === 1);
    expect(callsOf(edgeA)).toHaveLength(2);
    // A's delay ends while D still waits for its connect: A is refused its identity and does not join the line behind D.
    const failedA: any = await a;
    expect(failedA).toMatchObject({ code: errorCodes.clientDisconnected, message: 'identity changed' });
    expect(callsOf(edgeA)).toHaveLength(2);
    holdEdgeConnects = false;
    await eventually((): boolean => heldEdgeConnects.length === 1);
    releaseEdgeConnect(edgeA, heldEdgeConnects[0]);
    await eventually((): boolean => callsOf(edgeA).length === 3);
    answer(edgeA, callsOf(edgeA)[2]);
    expect(fromBytes((await d).data)).toEqual({ n: 'd' });
  });

  test('sourceMadeAfterLogoutReadsDisconnected', async (): Promise<void> => {
    mainTtl = 1;
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const ended: Promise<number> = new Promise<number>((resolve: (code: number) => void): void => {
      c.once('disconnected', (ctx: any): void => resolve(ctx.code));
    });
    stored = '';
    expect(await ended).toBe(disconnectedCodes.unauthorized);
    // The pool is made after the logout: its source has not heard of it, so the call reads the main client as disconnected.
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(await outcome(pool.rpc('work', bytes({ n: 1 })))).toMatchObject({ code: errorCodes.clientDisconnected, message: 'client disconnected' });
    expect(connectsOf(edgeA)).toHaveLength(0);
    expect(pool.stats()).toEqual({ connections: 0, inflight: 0, queued: 0, redirects: 0 });
    holdMainConnect = true;
    stored = 'refresh-recovered';
    mainTtl = 900;
    const reconnecting: Promise<void> = connectClient(c);
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const admitted: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
    expect(pool.stats().queued).toBe(1);
    expect(connectsOf(edgeA)).toHaveLength(0);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    await reconnecting;
    expect(fromBytes((await admitted).data)).toEqual({ n: 2 });
    // A pool made while another main client is connected, and named by no pool before, lends that client's confirmed token
    // at once: the source takes the confirmation it missed, and no renewal starts.
    const other: ProtobufCentrifuge = await connectedMain();
    const exchangesBefore: number = exchanges;
    const second: RpcPool = createPool(other, { endpoints: (): string[] => [edgeB.url] });
    expect(fromBytes((await second.rpc('work', bytes({ n: 3 }))).data)).toEqual({ n: 3 });
    expect(exchanges).toBe(exchangesBefore);
    expect(connectsOf(edgeB).at(-1).connect.token).toBe((other as any)._token);
  });

  test('staleConnectionEndReportsNoErrorAndHoldsNoOpen', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    await pool.rpc('work', bytes({ n: 1 }));
    expect(connectsOf(edgeA)).toHaveLength(1);
    // The main client moves to bob: the connection of alice is stale, and the fleet ends it for good before any other event.
    mainUser = 'bob';
    edgeA.connectResult = { ...edgeA.connectResult, user: 'bob' };
    await c.refresh('bob-token');
    edgeA.send({ push: { disconnect: { code: 3501, reason: 'terminal' } } });
    await eventually((): boolean => pool.stats().connections === 0);
    expect(errors).toEqual([]);
    // The next call opens a connection at once, with bob's token, and the endpoint holds no open.
    const next: RpcResult = await pool.rpc('work', bytes({ n: 2 }));
    expect(fromBytes(next.data)).toEqual({ n: 2 });
    expect(connectsOf(edgeA).at(-1).connect.token).toBe('bob-token');
    expect(errors).toEqual([]);
    const endpoint: any = (pool as any).endpoints.get(edgeA.url);
    expect(endpoint === undefined || endpoint.hold === null).toBe(true);
  });

  test('spentSourceStillPlacesOnALiveConnection', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain({ token: 'static-token' }, false);
    main.connectResult = { ...main.connectResult, user: 'alice' };
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (key: string): string => (key === 'b' ? edgeB.url : edgeA.url) });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    // P1 on key a is live with A in flight, and its token expires ahead. B waits behind A.
    edgeA.connectResult = { ...edgeA.connectResult, expires: true, ttl: 2 };
    const heldA: any[] = [];
    holdCalls(edgeA, heldA);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' }), { endpointKey: 'a' }));
    expect(await until((): boolean => heldA.length === 1)).toBe(true);
    const b: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'b' }), { endpointKey: 'a', timeout: 4000 }));
    // C on key b opens a connection, which the fleet refuses with the static token: the source is spent, and C fails as refused.
    refusedToken = 'static-token';
    const cCall: Promise<any> = outcome(pool.rpc('work', bytes({ n: 'c' }), { endpointKey: 'b', timeout: 4000 }));
    expect(await cCall).toMatchObject({ code: errorCodes.clientDisconnected, message: 'token refused' });
    expect(connectsOf(edgeB)).toHaveLength(1);
    // B keeps waiting, since P1 is live with the identity of the call, and is written when A replies.
    answer(edgeA, heldA[0]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    expect(await until((): boolean => heldA.length === 2)).toBe(true);
    // P1's token expires while B is in flight: its request is answered unauthorized, P1 ends with an error, and B settles
    // with the connection loss, not resent.
    await eventually((): boolean => errors.some((ctx: any): boolean => ctx.endpoint === edgeA.url
      && ctx.error?.code === disconnectedCodes.unauthorized), 8000);
    expect([errorCodes.connectionClosed, errorCodes.clientDisconnected]).toContain((await b).code);
    expect(callsOf(edgeA)).toHaveLength(2);
    expect(errors.filter((ctx: any): boolean => ctx.endpoint === edgeA.url)).toHaveLength(1);
    // D on key a finds no live connection of its generation, and fails as refused at once, with no open.
    const connectsBefore: number = connectsOf(edgeA).length;
    expect(await outcome(pool.rpc('work', bytes({ n: 'd' }), { endpointKey: 'a', timeout: 4000 }))).toMatchObject({
      code: errorCodes.clientDisconnected,
      message: 'token refused',
    });
    expect(connectsOf(edgeA)).toHaveLength(connectsBefore);
    expect(connectsOf(edgeB)).toHaveLength(1);
  });

  test('connectsWithTheMainTokenAtOneExchange', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c);
    const heldA: any[] = [];
    const heldB: any[] = [];
    holdCalls(edgeA, heldA);
    holdCalls(edgeB, heldB);
    const calls: Promise<RpcResult>[] = [quiet(pool.rpc('work', bytes({ n: 1 }))), quiet(pool.rpc('work', bytes({ n: 2 })))];
    expect(await until((): boolean => heldA.length === 1 && heldB.length === 1)).toBe(true);
    expect(connectsOf(edgeA)[0].connect.token).toBe('access-1');
    expect(connectsOf(edgeB)[0].connect.token).toBe('access-1');
    expect(exchanges).toBe(1);
    answer(edgeA, heldA[0]);
    answer(edgeB, heldB[0]);
    await Promise.all(calls);
  });

  test('refreshAtExpiryTakesTheRenewedToken', async (): Promise<void> => {
    edgeA.connectResult = { ...edgeA.connectResult, expires: true, ttl: 2 };
    mainTtl = 2;
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    await eventually((): boolean => refreshesOf(edgeA).length >= 1, 8000);
    expect(refreshesOf(edgeA)[0].refresh.token).toBe('access-2');
    expect(exchanges).toBe(2);
    expect(connectsOf(edgeA).length).toBe(1);
  });

  test('twoRefusalsCauseOneRenewal', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c);
    const heldA: any[] = [];
    const heldB: any[] = [];
    holdCalls(edgeA, heldA);
    holdCalls(edgeB, heldB);
    const calls: Promise<RpcResult>[] = [quiet(pool.rpc('work', bytes({ n: 1 }))), quiet(pool.rpc('work', bytes({ n: 2 })))];
    expect(await until((): boolean => heldA.length === 1 && heldB.length === 1)).toBe(true);
    answer(edgeA, heldA[0]);
    answer(edgeB, heldB[0]);
    await Promise.all(calls);
    expect(exchanges).toBe(1);
    // Both connections are refused for access-1, and their sockets close so that each reconnects with its token.
    refusedToken = 'access-1';
    edgeA.onCommand = (cmd: any): any => (cmd.connect !== undefined && cmd.connect.token === 'access-1' ? refuseConnect(cmd) : echo(cmd));
    edgeB.onCommand = edgeA.onCommand;
    closeSocket(edgeA, 1001);
    closeSocket(edgeB, 1001);
    await eventually((): boolean => connectsOf(edgeA).length >= 2 && connectsOf(edgeB).length >= 2, 8000);
    await eventually((): boolean => connectsOf(edgeA).some((cmd: any): boolean => cmd.connect.token === 'access-2')
      && connectsOf(edgeB).some((cmd: any): boolean => cmd.connect.token === 'access-2'), 8000);
    expect(exchanges).toBe(2);
  });

  test('refusedTokenNoLongerHeldIsAnsweredWithTheCurrent', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    await c.refresh();
    expect(exchanges).toBe(2);
    refusedToken = 'access-1';
    closeSocket(edgeA, 1001);
    await eventually((): boolean => connectsOf(edgeA).length >= 2, 8000);
    await eventually((): boolean => connectsOf(edgeA).some((cmd: any): boolean => cmd.connect.token === 'access-2'), 8000);
    expect(exchanges).toBe(2);
  });

  test('anonymousMainGivesAnonymousConnections', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain({}, false);
    // The fleet confirms the anonymous identity of the main client, which has no user.
    edgeA.connectResult = { ...edgeA.connectResult, user: '' };
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    expect(connectsOf(edgeA)[0].connect.token ?? '').toBe('');
    mainUser = 'alice';
    edgeA.connectResult = { ...edgeA.connectResult, user: 'alice' };
    await c.refresh('alice-token');
    expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
    expect(connectsOf(edgeA)[1].connect.token).toBe('alice-token');
    expect(exchanges).toBe(0);
  });

  test('configurationOfTokenFrom', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    const exclusive: any = thrownBy((): RpcPool => new RpcPool({
      endpoints: (): string => edgeA.url,
      connection: { token: 'copied' },
      tokenFrom: c,
    }));
    expect(exclusive).toMatchObject({
      code: errorCodes.badConfiguration,
      message: 'tokenFrom is exclusive with connection.token and connection.getToken',
    });
    const getter: any = thrownBy((): RpcPool => new RpcPool({
      endpoints: (): string => edgeA.url,
      connection: { getToken: (): Promise<string> => Promise.resolve('t') },
      tokenFrom: c,
    }));
    expect(getter).toMatchObject({ code: errorCodes.badConfiguration });
    const exchange: any = thrownBy((): RpcPool => new RpcPool({
      endpoints: (): string => edgeA.url,
      connection: { tokenExchange: tokenExchange() } as any,
    }));
    expect(exchange).toMatchObject({
      code: errorCodes.badConfiguration,
      message: 'tokenExchange is not accepted by the rpc pool: pass the client which owns the token as tokenFrom',
    });
    const notClient: any = thrownBy((): RpcPool => new RpcPool({
      endpoints: (): string => edgeA.url,
      tokenFrom: {} as any,
    }));
    expect(notClient).toMatchObject({ code: errorCodes.badConfiguration, message: 'tokenFrom must be a Centrifuge client' });
  });

  test('poolsOfOneClientShareOneSource', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    const unused: ProtobufCentrifuge = createMain();
    const first: RpcPool = createPool(c);
    const after: number = listenerCountOf(c, 'disconnected');
    const second: RpcPool = createPool(c);
    expect(listenerCountOf(c, 'disconnected')).toBe(after);
    expect(after).toBe(1);
    expect((first as any).source).toBe((second as any).source);
    expect((first as any).source).toBe(tokenSourceOf(c));
    expect(listenerCountOf(unused, 'disconnected')).toBe(0);
    expect((unused as any)['_host'].onRefreshAccepted).toBeNull();
  });

  test('getTokenIsCalledForEachConnection', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const source: any = tokenSourceOf(c);
    const spy: jest.SpyInstance = jest.spyOn(source, 'token');
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url, edgeB.url] });
    const heldA: any[] = [];
    const heldB: any[] = [];
    holdCalls(edgeA, heldA);
    holdCalls(edgeB, heldB);
    const calls: Promise<RpcResult>[] = [quiet(pool.rpc('work', bytes({ n: 1 }))), quiet(pool.rpc('work', bytes({ n: 2 })))];
    expect(await until((): boolean => heldA.length === 1 && heldB.length === 1)).toBe(true);
    expect(spy).toHaveBeenCalledTimes(2);
    answer(edgeA, heldA[0]);
    answer(edgeB, heldB[0]);
    await Promise.all(calls);
  });
});
