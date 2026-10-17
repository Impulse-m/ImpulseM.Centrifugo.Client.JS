import WebSocket from 'ws';
import { errorCodes, disconnectedCodes } from './codes';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { RpcPool, RpcPoolOptions } from './rpc_pool';
import { tokenSourceOf } from './token_source';
import { TokenExchange, TokenExchangeContext, TokenExchangeResult, RpcResult } from './types';
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
  return { id: cmd.id, refresh: { expires: true, ttl: ttl, user: user, auth_class: 'authenticated' } };
}

// A connect refused by the server with the token expired code: the client asks for a new token.
function refuseConnect(cmd: any): any {
  return { id: cmd.id, error: { code: 109, message: 'token expired' } };
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
  // The edge endpoints refuse connects carrying this token with the token expired code.
  let refusedToken: string;

  beforeEach(async (): Promise<void> => {
    main = await WaitableFakeServer.start('protobuf');
    edgeA = await WaitableFakeServer.start('protobuf');
    edgeB = await WaitableFakeServer.start('protobuf');
    servers = [main, edgeA, edgeB];
    main.connectResult = { client: 'main-client', version: '0.0.0', ping: 0, flag: rpcOptionsWireFlag };
    edgeA.connectResult = { client: 'edge-a', version: '0.0.0', ping: 0, flag: rpcOptionsWireFlag };
    edgeB.connectResult = { client: 'edge-b', version: '0.0.0', ping: 0, flag: rpcOptionsWireFlag };
    stored = 'refresh-1';
    issued = 0;
    exchanges = 0;
    failNext = 0;
    mainTtl = 900;
    mainUser = 'alice';
    holdMainConnect = false;
    heldMainConnect = null;
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
        return refreshReply(cmd, mainTtl, mainUser);
      }
      return undefined;
    };
    for (const edge of [edgeA, edgeB]) {
      edge.onCommand = (cmd: any): any => {
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
      return echo(cmd);
    };
  }

  test('pool connections connect with the main access token, and two pool connections cost one exchange', async (): Promise<void> => {
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
    edgeA.send({ id: heldA[0].id, rpc: { data: heldA[0].rpc.data } });
    edgeB.send({ id: heldB[0].id, rpc: { data: heldB[0].rpc.data } });
    await Promise.all(calls);
  });

  test('a pool connection refreshed at its expiry gets the renewed token of the main client: two exchanges in all', async (): Promise<void> => {
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

  test('two pool connections refused for the token the main client holds cause one renewal, and both reconnect with the new token', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c);
    const heldA: any[] = [];
    const heldB: any[] = [];
    holdCalls(edgeA, heldA);
    holdCalls(edgeB, heldB);
    const calls: Promise<RpcResult>[] = [quiet(pool.rpc('work', bytes({ n: 1 }))), quiet(pool.rpc('work', bytes({ n: 2 })))];
    expect(await until((): boolean => heldA.length === 1 && heldB.length === 1)).toBe(true);
    edgeA.send({ id: heldA[0].id, rpc: { data: heldA[0].rpc.data } });
    edgeB.send({ id: heldB[0].id, rpc: { data: heldB[0].rpc.data } });
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

  test('a refused token the main client no longer holds is answered with its current token, without a renewal', async (): Promise<void> => {
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

  test('while the main client connects, a pool connection waits and then connects with the new token, arming no timer', async (): Promise<void> => {
    holdMainConnect = true;
    const c: ProtobufCentrifuge = createMain();
    c.connect();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const timers: jest.SpyInstance = jest.spyOn(global, 'setTimeout');
    const pending: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => heldMainConnect !== null)).toBe(true);
    await new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, 100);
    });
    expect(connectsOf(edgeA).length).toBe(0);
    expect(timers.mock.calls.filter((call: any[]): boolean => call[1] === 0).length).toBe(0);
    timers.mockRestore();
    holdMainConnect = false;
    main.send({ id: heldMainConnect.id, connect: main.connectResult });
    expect(fromBytes((await pending).data)).toBeDefined();
    expect(connectsOf(edgeA)[0].connect.token).toBe('access-1');
    expect(exchanges).toBe(1);
  });

  test('a failed renewal leaves the waiting connection to the main client retry, and starts no second renewal', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    failNext = 1;
    refusedToken = 'access-1';
    closeSocket(edgeA, 1001);
    await eventually((): boolean => exchanges === 3 && connectsOf(edgeA).some((cmd: any): boolean => cmd.connect.token === 'access-2'), 15000);
    expect(exchanges).toBe(3);
  });

  test('after the main client disconnects, its valid token is still lent; a refusal ends the connection as unauthorized, and the call is not resent', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c);
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const heldA: any[] = [];
    holdCalls(edgeA, heldA);
    const first: Promise<any> = pool.rpc('work', bytes({ n: 1 })).then((): string => 'resolved', (error: any): any => error);
    expect(await until((): boolean => heldA.length === 1)).toBe(true);
    c.disconnect();
    // Edge A is busy, so the next call opens a connection on edge B with the token the main client still lends.
    edgeB.onCommand = echo;
    expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
    expect(connectsOf(edgeB)[0].connect.token).toBe('access-1');
    // Edge A refuses the token now, and its socket closes with a call in flight. The call is settled with the loss of the
    // connection, not sent again: the reconnect is refused, the connection ends as unauthorized, and the call fails with it.
    refusedToken = 'access-1';
    edgeA.onCommand = (cmd: any): any => (cmd.connect !== undefined && cmd.connect.token === 'access-1' ? refuseConnect(cmd) : echo(cmd));
    closeSocket(edgeA, 1001);
    expect(await first).toMatchObject({ code: errorCodes.clientDisconnected });
    expect(errors.some((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized)).toBe(true);
    expect(callsOf(edgeA).length).toBe(1);
  });

  test('logout: a renewal whose load is empty ends the session, and rpc fails at once and opens no connection until the client connects again', async (): Promise<void> => {
    mainTtl = 1;
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const ended: Promise<number> = new Promise<number>((resolve: (code: number) => void): void => {
      c.once('disconnected', (ctx: any): void => resolve(ctx.code));
    });
    stored = '';
    expect(await ended).toBe(disconnectedCodes.unauthorized);
    const connectsBefore: number = connectsOf(edgeA).length;
    const failed: any = await pool.rpc('work', bytes({ n: 1 })).then((): string => 'resolved', (error: any): any => error);
    expect(failed).toMatchObject({ code: errorCodes.clientDisconnected, message: 'unauthorized' });
    expect(connectsOf(edgeA).length).toBe(connectsBefore);
    stored = 'refresh-recovered';
    mainTtl = 900;
    await connectClient(c);
    expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
  });

  test('an identity change: the next call opens a connection with the new identity, and the old one is closed', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    expect(connectsOf(edgeA)[0].connect.token).toBe('access-1');
    mainUser = 'bob';
    await c.refresh('new-token');
    expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
    expect(connectsOf(edgeA).length).toBe(2);
    expect(connectsOf(edgeA)[1].connect.token).toBe('new-token');
    expect(pool.stats().connections).toBe(1);
  });

  test('an anonymous main client gives anonymous pool connections, retired once it logs in', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain({}, false);
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    expect(connectsOf(edgeA)[0].connect.token ?? '').toBe('');
    mainUser = 'alice';
    await c.refresh('alice-token');
    expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
    expect(connectsOf(edgeA)[1].connect.token).toBe('alice-token');
    expect(exchanges).toBe(0);
  });

  test('a static-token main client: a refusal ends the pool connection as unauthorized, and no renewal reaches the main server', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain({ token: 'static-token' }, false);
    await connectClient(c);
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    const errors: any[] = [];
    pool.on('error', (ctx: any): void => {
      errors.push(ctx);
    });
    const held: any[] = [];
    holdCalls(edgeA, held);
    quiet(pool.rpc('work', bytes({ n: 1 })));
    expect(await until((): boolean => held.length === 1)).toBe(true);
    expect(connectsOf(edgeA)[0].connect.token).toBe('static-token');
    refusedToken = 'static-token';
    edgeA.onCommand = (cmd: any): any => (cmd.connect !== undefined && cmd.connect.token === 'static-token' ? refuseConnect(cmd) : echo(cmd));
    closeSocket(edgeA, 1001);
    await eventually((): boolean => errors.some((ctx: any): boolean => ctx.error?.code === disconnectedCodes.unauthorized), 8000);
    expect(refreshesOf(main).length).toBe(0);
  });

  test('configuration: tokenFrom is exclusive with the token of the connection, tokenExchange is refused, and tokenFrom must be a client', async (): Promise<void> => {
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

  test('two pools on one client share one source, and a client never used as tokenFrom holds none', async (): Promise<void> => {
    const c: ProtobufCentrifuge = createMain();
    const unused: ProtobufCentrifuge = createMain();
    createPool(c);
    const after: number = listenerCountOf(c, 'connected');
    createPool(c);
    expect(listenerCountOf(c, 'connected')).toBe(after);
    expect(after).toBe(1);
    expect(tokenSourceOf(c)).toBe(tokenSourceOf(c));
    expect(listenerCountOf(unused, 'connected')).toBe(0);
    expect((unused as any)['_host'].onRefreshAccepted).toBeNull();
  });

  test('a pool connection which leaves while it waits leaves no waiting request', async (): Promise<void> => {
    holdMainConnect = true;
    const c: ProtobufCentrifuge = createMain();
    c.connect();
    const pool: RpcPool = createPool(c, { endpoints: (): string[] => [edgeA.url] });
    quiet(pool.rpc('work', bytes({ n: 1 })));
    const waiting: Map<object, unknown> = (tokenSourceOf(c) as any).waiting;
    expect(await until((): boolean => waiting.size === 1)).toBe(true);
    pool.close();
    expect(waiting.size).toBe(0);
  });
});
