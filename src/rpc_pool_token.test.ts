import WebSocket from 'ws';
import { errorCodes, disconnectedCodes } from './codes';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { RpcPool, RpcPoolOptions } from './rpc_pool';
import { tokenSourceOf } from './token_source';
import { State, TokenExchange, TokenExchangeContext, TokenExchangeResult, RpcResult } from './types';
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

// Resolves once the condition holds, within a real-time budget.
async function eventually(condition: () => boolean, budgetMs: number = 6000): Promise<void> {
  const started: number = Date.now();
  while (!condition()) {
    if (Date.now() - started > budgetMs) {
      throw new Error('condition not reached in ' + budgetMs + ' ms');
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
    const pending: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }));
    await pause(50);
    expect(connectsOf(edgeA)).toHaveLength(0);
    expect(pool.stats().queued).toBe(1);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => held.length === 1)).toBe(true);
    expect(connectsOf(edgeA)).toHaveLength(1);
    answer(edgeA, held[0]);
    expect(fromBytes((await pending).data)).toEqual({ n: 1 });
  });

  test('bindsUnboundCallToIdentityConfirmedAtConnect', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 0 })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    // The main client switches to bob: the fleet confirms bob on its connections, and the pool connection of alice is left.
    c.disconnect();
    mainUser = 'bob';
    main.connectResult = { ...main.connectResult, user: 'bob' };
    edgeA.connectResult = { ...edgeA.connectResult, user: 'bob' };
    c.setToken('bob-token');
    holdMainConnect = true;
    c.connect();
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const bobHeld: any[] = [];
    holdCalls(edgeA, bobHeld);
    const bobCall: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => bobHeld.length === 1)).toBe(true);
    expect(connectsOf(edgeA).at(-1).connect.token).toBe('bob-token');
    answer(edgeA, bobHeld[0]);
    expect(fromBytes((await bobCall).data)).toEqual({ n: 1 });
    expect(errors).toEqual([]);
    expect(first).toBeDefined();
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
    expect([errorCodes.connectionClosed, errorCodes.clientDisconnected]).toContain(failed.code);
    await pause(100);
    expect(callsOf(edgeA)).toHaveLength(1);
    expect(connectsOf(edgeA).filter((cmd: any): boolean => cmd.connect.token === 'access-1')).toHaveLength(1);
  });

  test('resendsCallLostWhileMainReconnectsUnderSameIdentity', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], retries: 3 });
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
    await eventually((): boolean => pool.stats().connections === 1);
    expect(pool.stats()).toEqual({ connections: 1, inflight: 0, queued: 1, redirects: 0 });
    holdEdgeConnects = false;
    await eventually((): boolean => heldEdgeConnects.length === 1);
    releaseEdgeConnect(edgeA, heldEdgeConnects[0]);
    await pause(200);
    await eventually((): boolean => held.length === 1);
    expect(await outcome(pool.rpc('work', bytes({ n: 'b2' })))).toMatchObject({ code: errorCodes.limitExceeded });
    answer(edgeA, held[0]);
    expect(fromBytes((await a).data)).toEqual({ n: 'a' });
    const cCall: Promise<RpcResult> = pool.rpc('work', bytes({ n: 'c' }));
    await eventually((): boolean => held.length === 2);
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
    const c: ProtobufCentrifuge = await connectedMain();
    refusedToken = 'access-1';
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url], maxConnectionsPerEndpoint: 2 });
    const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    const second: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
    await eventually((): boolean => callsOf(edgeA).length === 2);
    expect(exchanges).toBe(2);
    expect(fromBytes((await first).data)).toEqual({ n: 1 });
    expect(fromBytes((await second).data)).toEqual({ n: 2 });
    // Phase two: the renewed token is refused too, and the issuer gives it back: both connections reconnect with it, the
    // renewal it asks for is refused, and both waiters fail as unauthorized (refusedAgain), with one renewal.
    refusedToken = 'access-2';
    issued = 1;
    closeSocket(edgeA, 3000);
    await eventually((): boolean => exchanges === 3, 8000);
    await pause(300);
    expect(exchanges).toBe(3);
    // A later connection is refused the same token, and starts no second renewal: its request waits.
    const fourth: Promise<any> = quiet(pool.rpc('work', bytes({ n: 4 })));
    await pause(300);
    expect(exchanges).toBe(3);
    expect(fourth).toBeDefined();
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
    await connectClient(c);
    refusedToken = 'tok-1';
    mainRefreshHook = (cmd: any): any => refuseRefresh(cmd);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    expect(await outcome(pool.rpc('work', bytes({ n: 1 })))).toMatchObject({ code: errorCodes.clientDisconnected });
    expect(refreshesOf(main)).toHaveLength(1);
    expect(errors.some((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized)).toBe(true);
    expect(c.state).toBe(State.Connected);
    expect(callsOf(edgeA)).toHaveLength(0);
  });

  test('failedRefreshCallReevaluatesTheWaiters', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
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
    const renewals: () => number = (): number => refreshesOf(main).filter((cmd: any): boolean => cmd.refresh.token !== 'tY').length;
    const base: number = renewals();
    expect(base).toBe(renewals());
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
    holdMainConnect = true;
    const c: ProtobufCentrifuge = createMain();
    c.connect();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    c.removeAllListeners();
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => connectsOf(edgeA).length === 1)).toBe(true);
    expect(await until((): boolean => callsOf(edgeA).length === 1)).toBe(true);
    expect(connectsOf(edgeA)).toHaveLength(1);
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
    pool.close();
    expect(source.waiting.size).toBe(0);
  });

  test('sourceReferencesOnlyPoolsWithWaitingCalls', async (): Promise<void> => {
    holdMainConnect = true;
    const c: ProtobufCentrifuge = createMain();
    c.connect();
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    const source: any = tokenSourceOf(c);
    const poolA: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const poolB: RpcPool = createPool(c, { endpoints: (): string[] => [edgeB.url] });
    const heldB: any[] = [];
    holdCalls(edgeB, heldB);
    const bCall: Promise<RpcResult> = quiet(poolB.rpc('work', bytes({ n: 1 })));
    await eventually((): boolean => source.pools.size === 1);
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(await until((): boolean => heldB.length === 1)).toBe(true);
    expect(source.pools.size).toBe(0);
    expect((poolA as any).drainedKeys).toBe(0);
    answer(edgeB, heldB[0]);
    await bCall;
    const second: Promise<RpcResult> = quiet(poolB.rpc('work', bytes({ n: 2 })));
    expect(await until((): boolean => heldB.length === 2)).toBe(true);
    expect(source.pools.size).toBe(0);
    poolB.close();
    expect(source.pools.size).toBe(0);
    await second.catch((): void => {});
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
    const inFlight: Promise<any> = outcome(pool.rpc('work', bytes({ n: 1 }), { timeout: 1500 }));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    // The fleet refuses the static token on the reconnect: the main client has no getter, so the connection ends as unauthorized.
    refusedToken = 'static-token';
    closeSocket(edgeA, 3000);
    await pause(300);
    await eventually((): boolean => errors.some((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized), 8000);
    expect(refreshesOf(main)).toHaveLength(0);
    expect(await inFlight).toMatchObject({ code: errorCodes.timeout });
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
    pool.close();
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
    // The next call has its own key, so its connection opens on edge B, while the call in flight stays on edge A.
    const pool: RpcPool = createPool(c, { endpoints: (key: string): string => (key === 'next' ? edgeB.url : edgeA.url) });
    const held: any[] = [];
    holdCalls(edgeA, held);
    const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
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
    // The expiry of the retired connection's token is answered unauthorized, without a renewal of the main client.
    await pause(2500);
    expect(refreshesOf(main).length).toBe(refreshesBefore);
    expect(await until((): boolean => callsOf(edgeB).length === 1)).toBe(true);
    answer(edgeB, callsOf(edgeB)[0]);
    expect(fromBytes((await next).data)).toEqual({ n: 'next' });
  });

  test('replacedTokenRequestEndsNothing', async (): Promise<void> => {
    const c: ProtobufCentrifuge = await connectedMain();
    refusedToken = 'access-1';
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
    const held: any[] = [];
    holdCalls(edgeA, held);
    const result: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => renewalHeld !== null)).toBe(true);
    // The fleet drops the pool connection, and refuses its reconnect: it asks for a token again.
    closeSocket(edgeA, 3000);
    await pause(100);
    main.send({ id: renewalHeld.id, refresh: { expires: true, ttl: mainTtl, user: 'alice' } });
    refusedToken = '';
    expect(await until((): boolean => held.length === 1)).toBe(true);
    answer(edgeA, held[0]);
    expect(fromBytes((await result).data)).toEqual({ n: 1 });
    expect(errors).toEqual([]);
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
