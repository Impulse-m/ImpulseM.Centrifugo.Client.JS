import { Readable } from 'stream';
import WebSocket from 'ws';
import { errorCodes, disconnectedCodes } from './codes';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { RpcPool, RpcPoolOptions } from './rpc_pool';
import { fetchRedirect } from './rpc_pool_redirect';
import { RpcResult } from './types';
import {
  WaitableFakeServer, bytes, fromBytes, holdReply, rpcOptionsWireFlag, until,
} from './testing/codecs';

// The RPC pool against fake servers: the scheduling of connections and calls, the routing by endpoint key and
// affinity, the server's moves (reroute, redirect), the retries, the signal and the close. The pool speaks protobuf,
// so every fake server speaks protobuf too. No live server is needed.

// A call of the pool: an rpc command, the message id of a tracked call is absent.
function isCall(cmd: any): boolean {
  return cmd.rpc !== undefined && cmd.rpc.cancel === undefined;
}

function isConnect(cmd: any): boolean {
  return cmd.connect !== undefined;
}

function callsOf(server: WaitableFakeServer): any[] {
  return server.received.filter(isCall);
}

function connectsOf(server: WaitableFakeServer): any[] {
  return server.received.filter(isConnect);
}

function sleep(ms: number): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    setTimeout(resolve, ms);
  });
}

// Keeps a rejection a test does not await from failing the run.
function quiet<T>(promise: Promise<T>): Promise<T> {
  promise.catch((): void => {});
  return promise;
}

// Replies to a call with data, or with an error object, as the server does.
function reply(server: WaitableFakeServer, cmd: any, data: any): void {
  server.send({ id: cmd.id, rpc: { data: bytes(data) } });
}

// A refusal of the server with the later signal: the call did not run, its id is still good.
function laterError(): any {
  return {
    code: 111,
    message: 'too many requests',
    temporary: true,
    metadata: { 'x-centrifugo-retry': 'later' },
  };
}

// A server error which moves the call to the endpoint key named in its metadata.
function moveError(key: string): any {
  return { code: 100, message: 'moved', metadata: { 'x-moved': key } };
}

// Echoes the data of every call.
function echo(cmd: any): any {
  if (!isCall(cmd)) {
    return undefined;
  }
  return { id: cmd.id, rpc: { data: cmd.rpc.data } };
}

// Closes the socket of the fake server with a code, as a proxy or the peer does. The fake keeps its socket private.
function closeSocket(server: WaitableFakeServer, code: number): void {
  (server as any).current.close(code);
}

describe('RpcPool', () => {
  let servers: WaitableFakeServer[] = [];
  let pools: RpcPool[] = [];

  afterEach(async (): Promise<void> => {
    jest.restoreAllMocks();
    for (const pool of pools) {
      pool.close();
    }
    pools = [];
    for (const server of servers) {
      await server.close();
    }
    servers = [];
  });

  async function startServer(): Promise<WaitableFakeServer> {
    const server: WaitableFakeServer = await WaitableFakeServer.start('protobuf');
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
    servers.push(server);
    return server;
  }

  type Resolver = RpcPoolOptions['endpoints'];

  function createPool(endpoints: Resolver, extra: Partial<RpcPoolOptions> = {}): RpcPool {
    const pool: RpcPool = new RpcPool({
      endpoints: endpoints,
      connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50 },
      ...extra,
    });
    pools.push(pool);
    return pool;
  }

  describe('scheduling', () => {
    test('fills a connection before opening the next, opens up to maxConnectionsPerEndpoint and queues beyond', async (): Promise<void> => {
      // One server per endpoint: the fake server answers on its latest socket, so each connection gets its own.
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      const heldA: any[] = [];
      const heldB: any[] = [];
      serverA.onCommand = (cmd: any): any => (isCall(cmd) ? (heldA.push(cmd), holdReply) : undefined);
      serverB.onCommand = (cmd: any): any => (isCall(cmd) ? (heldB.push(cmd), holdReply) : undefined);
      const pool: RpcPool = createPool((): string[] => [serverA.url, serverB.url], {
        maxConnectionsPerEndpoint: 1,
        maxInflightPerConnection: 2,
        maxQueued: 8,
      });
      const calls: Promise<RpcResult>[] = [1, 2, 3, 4, 5].map((n: number): Promise<RpcResult> => quiet(pool.rpc('work', bytes({ n: n }))));
      expect(await until((): boolean => heldA.length === 2 && heldB.length === 2)).toBe(true);
      expect(pool.stats()).toEqual({ connections: 2, inflight: 4, queued: 1, redirects: 0 });
      expect(fromBytes(heldA[1].rpc.data)).toEqual({ n: 2 });

      reply(serverA, heldA[0], { n: 1 });
      expect(fromBytes((await calls[0]).data)).toEqual({ n: 1 });
      expect(await until((): boolean => heldA.length === 3)).toBe(true);
      expect(fromBytes(heldA[2].rpc.data)).toEqual({ n: 5 });
      expect(pool.stats().queued).toBe(0);
    });

    test('refuses a call beyond maxQueued with limitExceeded', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url, { maxConnectionsPerEndpoint: 1, maxQueued: 1 });
      quiet(pool.rpc('work', bytes({ n: 1 })));
      quiet(pool.rpc('work', bytes({ n: 2 })));
      let refused: any;
      try {
        await pool.rpc('work', bytes({ n: 3 }));
      } catch (e) {
        refused = e;
      }
      expect(refused.code).toBe(errorCodes.limitExceeded);
      expect(pool.stats().queued).toBe(1);
    });

    test('counts a call waiting for its endpoint key against maxQueued, so a slow resolver cannot admit calls without a bound', async (): Promise<void> => {
      const pool: RpcPool = createPool((): Promise<string[]> => new Promise<string[]>((): void => {}), { maxQueued: 1 });
      quiet(pool.rpc('work', bytes({ n: 1 })));
      let refused: any;
      try {
        await pool.rpc('work', bytes({ n: 2 }));
      } catch (e) {
        refused = e;
      }
      expect(refused.code).toBe(errorCodes.limitExceeded);
      expect(pool.stats().queued).toBe(1);
    });

    test('a call which times out while it waits leaves its line at once, and the line keeps no settled call', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url, { maxConnectionsPerEndpoint: 1, maxQueued: 8 });
      quiet(pool.rpc('work', bytes({ n: 1 })));
      const waited: any = await pool.rpc('work', bytes({ n: 2 }), { timeout: 100 }).then((): string => 'resolved', (error: any): any => error);
      expect(waited.code).toBe(errorCodes.timeout);
      expect((pool as any).groups.get('').head()).toBeNull();
      expect(pool.stats().queued).toBe(0);
    });

    test('a release places the calls of its own key, and a call settled while it waits is dropped at the head', async (): Promise<void> => {
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      const heldA: any[] = [];
      const heldB: any[] = [];
      serverA.onCommand = (cmd: any): any => (isCall(cmd) ? (heldA.push(cmd), holdReply) : undefined);
      serverB.onCommand = (cmd: any): any => (isCall(cmd) ? (heldB.push(cmd), holdReply) : undefined);
      const pool: RpcPool = createPool((key: string): string => (key === 'a' ? serverA.url : serverB.url), {
        maxConnectionsPerEndpoint: 1,
        maxQueued: 8,
      });
      const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 }), { endpointKey: 'a' }));
      quiet(pool.rpc('work', bytes({ n: 2 }), { endpointKey: 'a' }));
      const controller: AbortController = new AbortController();
      const aborted: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 3 }), { endpointKey: 'a', signal: controller.signal }));
      quiet(pool.rpc('work', bytes({ n: 4 }), { endpointKey: 'a' }));
      const other: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 5 }), { endpointKey: 'b' }));
      expect(await until((): boolean => heldA.length === 1 && heldB.length === 1)).toBe(true);
      expect(pool.stats()).toEqual({ connections: 2, inflight: 2, queued: 3, redirects: 0 });

      controller.abort();
      await expect(aborted).rejects.toMatchObject({ code: errorCodes.cancelled });
      expect(pool.stats().queued).toBe(2);

      // The release of key b places nothing of key a: its line keeps the calls waiting for the slot of key a.
      reply(serverB, heldB[0], { n: 5 });
      expect(fromBytes((await other).data)).toEqual({ n: 5 });
      await sleep(50);
      expect(heldA.length).toBe(1);
      expect(pool.stats().queued).toBe(2);

      // The release of key a sends the second call, drops the settled third and keeps the fourth in its line.
      reply(serverA, heldA[0], { n: 1 });
      expect(fromBytes((await first).data)).toEqual({ n: 1 });
      expect(await until((): boolean => heldA.length === 2)).toBe(true);
      expect(fromBytes(heldA[1].rpc.data)).toEqual({ n: 2 });
      expect(pool.stats()).toEqual({ connections: 2, inflight: 1, queued: 1, redirects: 0 });
    });

    test('a burst opens one connection while it connects', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let connectCmd: any = null;
      server.onCommand = (cmd: any): any => {
        // Only the first connect is held: the connections opened after it connect at once. The calls are not answered:
        // the fake server answers on its latest socket, and this case reads the placement of the calls only.
        if (isConnect(cmd) && connectCmd === null) {
          connectCmd = cmd;
          return holdReply;
        }
        return isCall(cmd) ? holdReply : undefined;
      };
      const pool: RpcPool = createPool((): string => server.url, { maxConnectionsPerEndpoint: 4, maxQueued: 8 });
      const calls: Promise<RpcResult>[] = [1, 2, 3, 4].map((n: number): Promise<RpcResult> => quiet(pool.rpc('work', bytes({ n: n }))));
      expect(await until((): boolean => connectCmd !== null)).toBe(true);
      await sleep(100);
      expect(connectsOf(server).length).toBe(1);
      expect(pool.stats().connections).toBe(1);
      expect(pool.stats().queued).toBe(3);

      server.send({ id: connectCmd.id, connect: server.connectResult });
      expect(await until((): boolean => callsOf(server).length === 4)).toBe(true);
      expect(connectsOf(server).length).toBe(4);
      expect(pool.stats()).toEqual({ connections: 4, inflight: 4, queued: 0, redirects: 0 });
      expect(calls.length).toBe(4);
    });

    test('a connection with no call closes after idleTimeout and reopens on the next call', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, { idleTimeout: 100 });
      expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
      expect(pool.stats().connections).toBe(1);
      await sleep(250);
      expect(pool.stats().connections).toBe(0);
      expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
      expect(connectsOf(server).length).toBe(2);
    });

    test('an idleTimeout beyond the longest timer is refused with badConfiguration', (): void => {
      let thrown: any;
      try {
        createPool((): string => 'ws://127.0.0.1:1', { idleTimeout: 2147483648 });
      } catch (e) {
        thrown = e;
      }
      expect(thrown).toMatchObject({ code: errorCodes.badConfiguration });
    });

    test('an idleTimeout at the longest timer arms one check per period, not a loop', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, { idleTimeout: 2147483647 });
      expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
      const timers: jest.SpyInstance = jest.spyOn(global, 'setTimeout');
      await sleep(300);
      // The sleep is one timer of its own; a check re-armed in a loop would add one per millisecond.
      expect(timers.mock.calls.length).toBeLessThanOrEqual(2);
      expect(pool.stats().connections).toBe(1);
      timers.mockRestore();
    });

    test('idleTimeout 0 keeps the connection open', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, { idleTimeout: 0 });
      await pool.rpc('work', bytes({ n: 1 }));
      await sleep(150);
      expect(pool.stats().connections).toBe(1);
      expect(connectsOf(server).length).toBe(1);
    });

    test('a connection used call after call arms one idle check, not one per call', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, { idleTimeout: 5123 });
      const timers: jest.SpyInstance = jest.spyOn(global, 'setTimeout');
      for (let n: number = 0; n < 20; n++) {
        expect(fromBytes((await pool.rpc('work', bytes({ n: n }))).data)).toEqual({ n: n });
      }
      expect(connectsOf(server).length).toBe(1);
      const armed: any[] = timers.mock.calls.filter((args: any[]): boolean => Math.abs(Number(args[1]) - 5123) <= 5);
      expect(armed.length).toBeLessThanOrEqual(1);
      pool.close();
      timers.mockRestore();
    });

    test('a busy connection arms no idle check per call', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url, {
        idleTimeout: 5123,
        maxConnectionsPerEndpoint: 1,
        maxInflightPerConnection: 2,
      });
      const timers: jest.SpyInstance = jest.spyOn(global, 'setTimeout');
      const pending: Array<Promise<RpcResult>> = [
        quiet(pool.rpc('work', bytes({ n: 0 }))),
        quiet(pool.rpc('work', bytes({ n: 1 }))),
      ];
      expect(await until((): boolean => callsOf(server).length === 2)).toBe(true);
      for (let n: number = 2; n < 12; n++) {
        reply(server, callsOf(server)[n - 2], { n: n - 2 });
        pending.push(quiet(pool.rpc('work', bytes({ n: n }))));
        expect(await until((): boolean => callsOf(server).length === n + 1)).toBe(true);
      }
      expect(pool.stats()).toEqual({ connections: 1, inflight: 2, queued: 0, redirects: 0 });
      const armed: any[] = timers.mock.calls.filter((args: any[]): boolean => Math.abs(Number(args[1]) - 5123) <= 5);
      expect(armed.length).toBe(0);
      pool.close();
      timers.mockRestore();
      await Promise.allSettled(pending);
    });

    test('a connection used within each idle period stays open, and closes after a period without calls', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, { idleTimeout: 100 });
      for (let n: number = 0; n < 5; n++) {
        await pool.rpc('work', bytes({ n: n }));
        await sleep(60);
        expect(pool.stats().connections).toBe(1);
      }
      expect(connectsOf(server).length).toBe(1);
      await sleep(250);
      expect(pool.stats().connections).toBe(0);
    });

    test('a connect which throws leaves no connection in its group, and the next call opens a new one', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      jest.spyOn(ProtobufCentrifuge.prototype, 'connect').mockImplementationOnce((): void => {
        throw new Error('connect refused');
      });
      const pool: RpcPool = createPool((): string => server.url);
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toThrow('connect refused');
      expect(pool.stats().connections).toBe(0);
      expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
      expect(pool.stats().connections).toBe(1);
    });
  });

  describe('routing', () => {
    test('two endpoint keys map to two endpoints, each with its own connections', async (): Promise<void> => {
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      serverA.onCommand = echo;
      serverB.onCommand = echo;
      const pool: RpcPool = createPool((key: string): string => (key === 'a' ? serverA.url : serverB.url));
      await pool.rpc('work', bytes({ key: 'a' }), { endpointKey: 'a' });
      await pool.rpc('work', bytes({ key: 'b' }), { endpointKey: 'b' });
      expect(callsOf(serverA).map((cmd: any): any => fromBytes(cmd.rpc.data))).toEqual([{ key: 'a' }]);
      expect(callsOf(serverB).map((cmd: any): any => fromBytes(cmd.rpc.data))).toEqual([{ key: 'b' }]);
      expect(pool.stats().connections).toBe(2);
    });

    test('a resolver returning a promise routes the call', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool(async (): Promise<string> => server.url);
      expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    });

    test('a resolver throwing rejects the call with its error', async (): Promise<void> => {
      const pool: RpcPool = createPool((): string => {
        throw new Error('no route for the key');
      });
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toThrow('no route for the key');
    });

    test('a resolver giving no endpoint rejects the call with badConfiguration', async (): Promise<void> => {
      const pool: RpcPool = createPool((): string[] => []);
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toMatchObject({ code: errorCodes.badConfiguration });
    });

    test('the affinity hint goes out as the affinity header on every call', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, { affinityHeader: 'x-affinity' });
      await pool.rpc('work', bytes({ n: 1 }), { affinity: 'hint-1', headers: { 'x-trace': 't' } });
      await pool.rpc('work', bytes({ n: 2 }), { affinity: 'hint-1' });
      const calls: any[] = callsOf(server);
      expect(calls[0].rpc.headers).toEqual({ 'x-affinity': 'hint-1', 'x-trace': 't' });
      expect(calls[1].rpc.headers).toEqual({ 'x-affinity': 'hint-1' });
    });

    test('affinity without affinityHeader rejects with badConfiguration', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url);
      await expect(pool.rpc('work', bytes({ n: 1 }), { affinity: 'hint-1' })).rejects.toMatchObject({ code: errorCodes.badConfiguration });
      expect(callsOf(server).length).toBe(0);
    });

    test('the affinity header given in headers rejects with badConfiguration', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, { affinityHeader: 'x-affinity' });
      await expect(pool.rpc('work', bytes({ n: 1 }), { headers: { 'X-Affinity': 'mine' } })).rejects.toMatchObject({
        code: errorCodes.badConfiguration,
      });
      expect(callsOf(server).length).toBe(0);
    });

    test('a connection option other than the accepted ones throws badConfiguration, instance included', (): void => {
      let thrown: any = null;
      try {
        createPool((): string => 'ws://localhost:1/connection/websocket', {
          connection: { instance: 'AbCdEfGhIjKlMnOpQrStUv' } as any,
        });
      } catch (e) {
        thrown = e;
      }
      expect(thrown.code).toBe(errorCodes.badConfiguration);
    });
  });

  describe('reroute', () => {
    test('a move resends the call on the endpoint of the new key, with the same data and affinity', async (): Promise<void> => {
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      serverA.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: moveError('b') } : undefined);
      serverB.onCommand = echo;
      const pool: RpcPool = createPool((key: string): string => (key === 'a' ? serverA.url : serverB.url), {
        affinityHeader: 'x-affinity',
        reroute: (error: any): string | undefined => error.metadata?.['x-moved'],
      });
      const result: RpcResult = await pool.rpc('work', bytes({ n: 7 }), { endpointKey: 'a', affinity: 'hint' });
      expect(fromBytes(result.data)).toEqual({ n: 7 });
      expect(callsOf(serverA).length).toBe(1);
      expect(callsOf(serverB).length).toBe(1);
      expect(callsOf(serverB)[0].rpc.data).toEqual(callsOf(serverA)[0].rpc.data);
      expect(callsOf(serverB)[0].rpc.headers).toEqual({ 'x-affinity': 'hint' });
    });

    test('retries bounds a hook which always moves, and the last error is surfaced', async (): Promise<void> => {
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      serverA.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: moveError('b') } : undefined);
      serverB.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: moveError('a') } : undefined);
      const pool: RpcPool = createPool((key: string): string => (key === 'a' ? serverA.url : serverB.url), {
        retries: 2,
        reroute: (error: any): string | undefined => error.metadata?.['x-moved'],
      });
      await expect(pool.rpc('work', bytes({ n: 1 }), { endpointKey: 'a' })).rejects.toMatchObject({ code: 100 });
      expect(callsOf(serverA).length + callsOf(serverB).length).toBe(3);
    });

    test('the hook is not called on a lost connection, and the call is resent', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let sent: number = 0;
      server.onCommand = (cmd: any): any => {
        if (!isCall(cmd)) {
          return undefined;
        }
        sent++;
        if (sent === 1) {
          closeSocket(server, 1000);
          return holdReply;
        }
        return echo(cmd);
      };
      const reroute: jest.Mock = jest.fn((): string | undefined => undefined);
      const pool: RpcPool = createPool((): string => server.url, { reroute: reroute });
      expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
      expect(reroute).not.toHaveBeenCalled();
      expect(sent).toBe(2);
    });

    test('the hook is not called on a timeout', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const reroute: jest.Mock = jest.fn((): string | undefined => undefined);
      const pool: RpcPool = createPool((): string => server.url, { reroute: reroute });
      await expect(pool.rpc('work', bytes({ n: 1 }), { timeout: 100 })).rejects.toMatchObject({ code: errorCodes.timeout });
      expect(reroute).not.toHaveBeenCalled();
    });
  });

  describe('retries', () => {
    test('a call lost with its connection is sent again on a connection, and not marked as a retransmit', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let sent: number = 0;
      server.onCommand = (cmd: any): any => {
        if (!isCall(cmd)) {
          return undefined;
        }
        sent++;
        if (sent === 1) {
          closeSocket(server, 1000);
          return holdReply;
        }
        return echo(cmd);
      };
      const pool: RpcPool = createPool((): string => server.url);
      await pool.rpc('work', bytes({ n: 5 }));
      const calls: any[] = callsOf(server);
      expect(calls.length).toBe(2);
      expect(calls[1].rpc.data).toEqual(calls[0].rpc.data);
      expect(calls[1].rpc.retransmit).toBeUndefined();
      expect(calls[1].rpc.msg_id).toBeUndefined();
    });

    test('a later refusal waits a jittered delay that grows with the attempt, and four refusals surface the error', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      const times: number[] = [];
      server.onCommand = (cmd: any): any => {
        if (!isCall(cmd)) {
          return undefined;
        }
        times.push(Date.now());
        return { id: cmd.id, error: laterError() };
      };
      // A random source at the top of its range takes the bound of each delay: 250, 500 and 1000 ms.
      jest.spyOn(Math, 'random').mockReturnValue(0.9999999);
      const pool: RpcPool = createPool((): string => server.url);
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toMatchObject({ code: 111 });
      expect(times.length).toBe(4);
      expect(times[1] - times[0]).toBeGreaterThanOrEqual(240);
      expect(times[2] - times[1]).toBeGreaterThanOrEqual(490);
      expect(times[3] - times[2]).toBeGreaterThanOrEqual(990);
    });

    test('close cancels a call waiting out its later delay, which holds no slot', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: laterError() } : undefined);
      jest.spyOn(Math, 'random').mockReturnValue(0.9999999);
      const pool: RpcPool = createPool((): string => server.url);
      const call: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }));
      expect(await until((): boolean => pool.stats().inflight === 0 && callsOf(server).length === 1)).toBe(true);
      pool.close();
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
      await sleep(300);
      expect(callsOf(server).length).toBe(1);
    });

    test('a later refusal with a zero draw is sent again at once', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let sent: number = 0;
      server.onCommand = (cmd: any): any => {
        if (!isCall(cmd)) {
          return undefined;
        }
        sent++;
        return sent === 1 ? { id: cmd.id, error: laterError() } : echo(cmd);
      };
      jest.spyOn(Math, 'random').mockReturnValue(0);
      const pool: RpcPool = createPool((): string => server.url);
      expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
      expect(sent).toBe(2);
    });

    test('a later refusal after a lost connection waits the delay of its first later attempt', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      const times: number[] = [];
      server.onCommand = (cmd: any): any => {
        if (!isCall(cmd)) {
          return undefined;
        }
        times.push(Date.now());
        if (times.length === 1) {
          closeSocket(server, 1000);
          return holdReply;
        }
        if (times.length === 2) {
          return { id: cmd.id, error: laterError() };
        }
        return echo(cmd);
      };
      // A random source at the top of its range takes the bound of the delay: 250 ms for the first later attempt.
      jest.spyOn(Math, 'random').mockReturnValue(0.9999999);
      const pool: RpcPool = createPool((): string => server.url);
      expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
      expect(times.length).toBe(3);
      // The lost connection is not counted in the exponent of the later delay: 500 ms would be the bound of attempt 1.
      expect(times[2] - times[1]).toBeLessThan(400);
    });

    test('a lost connection, a later refusal and a move count against the one retries', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      const reroute: jest.Mock = jest.fn((error: any): string | undefined => error.metadata?.['x-moved']);
      let sent: number = 0;
      server.onCommand = (cmd: any): any => {
        if (!isCall(cmd)) {
          return undefined;
        }
        sent++;
        if (sent === 1) {
          return { id: cmd.id, error: laterError() };
        }
        if (sent === 2) {
          closeSocket(server, 1000);
          return holdReply;
        }
        return { id: cmd.id, error: moveError('b') };
      };
      jest.spyOn(Math, 'random').mockReturnValue(0);
      const pool: RpcPool = createPool((): string => server.url, { retries: 2, reroute: reroute });
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toMatchObject({ code: 100 });
      expect(sent).toBe(3);
      // The hook is asked about the first refusal only: with the budget spent, the move is surfaced without a question.
      expect(reroute).toHaveBeenCalledTimes(1);
      expect(reroute.mock.calls[0][0]).toMatchObject({ code: 111 });
    });

    test('a connection the server closed for message size settles its calls without a resend, and emits an error', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let sent: number = 0;
      server.onCommand = (cmd: any): any => {
        if (!isCall(cmd)) {
          return undefined;
        }
        sent++;
        closeSocket(server, 1009);
        return holdReply;
      };
      const pool: RpcPool = createPool((): string => server.url);
      const errors: any[] = [];
      pool.on('error', (ctx: any): void => {
        errors.push(ctx);
      });
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toMatchObject({ code: errorCodes.connectionClosed });
      expect(sent).toBe(1);
      expect(errors.length).toBe(1);
      expect(errors[0].error.code).toBe(disconnectedCodes.messageSizeLimit);
      expect(pool.stats().connections).toBe(0);

      server.onCommand = echo;
      expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
      expect(connectsOf(server).length).toBe(2);
    });

    test('a call whose timeout passes is not sent again', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let sent: number = 0;
      server.onCommand = (cmd: any): any => {
        if (isCall(cmd)) {
          sent++;
          return holdReply;
        }
        return undefined;
      };
      const pool: RpcPool = createPool((): string => server.url);
      await expect(pool.rpc('work', bytes({ n: 1 }), { timeout: 150 })).rejects.toMatchObject({ code: errorCodes.timeout });
      expect(sent).toBe(1);
    });
  });

  describe('signal', () => {
    test('abort rejects with cancelled, frees the slot on the late reply, and sends no cancel', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url);
      const controller: AbortController = new AbortController();
      const call: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }), { signal: controller.signal });
      const held: any = await server.waitFor(isCall);
      controller.abort();
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
      expect(pool.stats().inflight).toBe(1);

      reply(server, held, { late: true });
      expect(await until((): boolean => pool.stats().inflight === 0)).toBe(true);
      expect(server.received.some((cmd: any): boolean => cmd.rpc?.cancel !== undefined)).toBe(false);
    });

    test('a call timeout which is not a positive number rejects with badConfiguration before anything is sent', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url);
      await expect(pool.rpc('work', bytes({ n: 1 }), { timeout: Number.NaN })).rejects.toMatchObject({
        code: errorCodes.badConfiguration,
      });
      await expect(pool.rpc('work', bytes({ n: 1 }), { timeout: 0 })).rejects.toMatchObject({
        code: errorCodes.badConfiguration,
      });
      expect(callsOf(server).length).toBe(0);
    });

    test('a call timeout above connection.timeout is refused before anything is sent', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, {
        connection: { websocket: WebSocket, timeout: 200, minReconnectDelay: 10, maxReconnectDelay: 50 },
      });
      await expect(pool.rpc('work', bytes({ n: 1 }), { timeout: 300 })).rejects.toMatchObject({
        code: errorCodes.badConfiguration,
      });
      expect(callsOf(server).length).toBe(0);
      expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }), { timeout: 200 })).data)).toEqual({ n: 2 });
    });

    test('an aborted signal rejects before anything is sent', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url);
      const controller: AbortController = new AbortController();
      controller.abort();
      await expect(pool.rpc('work', bytes({ n: 1 }), { signal: controller.signal })).rejects.toMatchObject({
        code: errorCodes.cancelled,
      });
      expect(callsOf(server).length).toBe(0);
    });
  });

  describe('redirect', () => {
    // A fetch stub which answers with the given body and status.
    function fetchReturning(body: Uint8Array, status: number): jest.Mock {
      return jest.fn(async (): Promise<any> => new Response(body as any, { status: status }));
    }

    // A fetch stub which never answers: it fails when the signal it is given aborts.
    function hangingFetch(): jest.Mock {
      return jest.fn((...args: any[]): Promise<any> => {
        const signal: AbortSignal = args[1].signal;
        return new Promise<any>((resolve: (value: any) => void, reject: (error: any) => void): void => {
          void resolve;
          signal.addEventListener('abort', (): void => reject(new Error('aborted')));
        });
      });
    }

    test('a reply the hook maps to a URL is fetched, and the promise resolves with the body', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: bytes({ url: 'https://cdn.example/chunk' }) } } : undefined);
      const fetchFunction: jest.Mock = fetchReturning(new Uint8Array([1, 2, 3]), 200);
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchFunction as any,
        redirect: (reply: RpcResult | null): any => (reply !== null ? { url: fromBytes(reply.data).url } : undefined),
      });
      const result: RpcResult = await pool.rpc('work', bytes({ n: 1 }));
      expect(Array.from(result.data)).toEqual([1, 2, 3]);
      expect(fetchFunction).toHaveBeenCalledWith('https://cdn.example/chunk', expect.objectContaining({ redirect: 'manual' }));
    });

    test('maxRedirectsInflight bounds the fetches in flight over the pool', async (): Promise<void> => {
      const servers: WaitableFakeServer[] = [await startServer(), await startServer(), await startServer()];
      for (const server of servers) {
        server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: bytes({}) } } : undefined);
      }
      let running: number = 0;
      let peak: number = 0;
      const fetchFunction: jest.Mock = jest.fn(async (): Promise<any> => {
        running++;
        peak = Math.max(peak, running);
        await sleep(30);
        running--;
        return new Response(new Uint8Array([9]), { status: 200 });
      });
      const pool: RpcPool = createPool((): string[] => servers.map((server: WaitableFakeServer): string => server.url), {
        maxRedirectsInflight: 1,
        maxConnectionsPerEndpoint: 1,
        maxInflightPerConnection: 1,
        fetch: fetchFunction as any,
        redirect: (): any => ({ url: 'https://cdn.example/x' }),
      });
      const calls: Promise<RpcResult>[] = [1, 2, 3].map((n: number): Promise<RpcResult> => pool.rpc('work', bytes({ n: n })));
      expect(await until((): boolean => pool.stats().redirects === 1)).toBe(true);
      for (const call of calls) {
        await call;
      }
      expect(peak).toBe(1);
      expect(fetchFunction).toHaveBeenCalledTimes(3);
    });

    test('a hook which returns null, as undefined does, is not a redirect', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const fetchFunction: jest.Mock = fetchReturning(new Uint8Array([1]), 200);
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchFunction as any,
        redirect: (): any => null,
      });
      expect(fromBytes((await pool.rpc('work', bytes({ n: 3 }))).data)).toEqual({ n: 3 });
      expect(fetchFunction).not.toHaveBeenCalled();
    });

    test('the global fetch is read when a redirect is fetched, so a fetch installed after the pool is made is used', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: bytes({ url: 'https://cdn.example/late' }) } } : undefined);
      const original: any = (globalThis as any).fetch;
      const pool: RpcPool = createPool((): string => server.url, {
        redirect: (reply: RpcResult | null): any => (reply !== null ? { url: fromBytes(reply.data).url } : undefined),
      });
      (globalThis as any).fetch = fetchReturning(new Uint8Array([6]), 200);
      try {
        const result: RpcResult = await pool.rpc('work', bytes({ n: 1 }));
        expect(Array.from(result.data)).toEqual([6]);
      } finally {
        (globalThis as any).fetch = original;
      }
    });

    test('a non-2xx status rejects with redirectFailed', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchReturning(new Uint8Array([]), 404) as any,
        redirect: (): any => ({ url: 'https://cdn.example/missing' }),
      });
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toMatchObject({ code: errorCodes.redirectFailed });
    });

    test('a non-2xx body is cancelled, so that its connection is released', async (): Promise<void> => {
      const cancel: jest.Mock = jest.fn();
      const body: ReadableStream<Uint8Array> = new ReadableStream<Uint8Array>({
        pull(controller: ReadableStreamDefaultController<Uint8Array>): void {
          controller.enqueue(new Uint8Array([1]));
        },
        cancel: cancel,
      });
      await expect(fetchRedirect(
        jest.fn(async (): Promise<any> => new Response(body, { status: 404 })) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/missing' },
        new AbortController().signal
      )).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(cancel).toHaveBeenCalledTimes(1);
    });

    test('a body over maxBytes rejects with redirectFailed', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchReturning(new Uint8Array(10), 200) as any,
        redirect: (): any => ({ url: 'https://cdn.example/big', maxBytes: 4 }),
      });
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toMatchObject({ code: errorCodes.redirectFailed });
    });

    test('a maxBytes which is not a whole number rejects with badConfiguration, and nothing is fetched', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const fetchFunction: jest.Mock = fetchReturning(new Uint8Array([1]), 200);
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchFunction as any,
        redirect: (): any => ({ url: 'https://cdn.example/x', maxBytes: Number.NaN }),
      });
      await expect(pool.rpc('work', bytes({ n: 1 }))).rejects.toMatchObject({ code: errorCodes.badConfiguration });
      expect(fetchFunction).not.toHaveBeenCalled();
    });

    test('a Node stream body over maxBytes is cut off as it arrives, and one within it resolves', async (): Promise<void> => {
      const chunks: Uint8Array[] = [new Uint8Array(3), new Uint8Array(3), new Uint8Array(3)];
      const over: Promise<Uint8Array> = fetchRedirect(
        jest.fn(async (): Promise<any> => ({ ok: true, status: 200, headers: new Headers(), body: Readable.from(chunks) })) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/stream', maxBytes: 4 },
        new AbortController().signal
      );
      await expect(over).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      const within: Uint8Array = await fetchRedirect(
        jest.fn(async (): Promise<any> => ({ ok: true, status: 200, headers: new Headers(), body: Readable.from(chunks) })) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/stream', maxBytes: 9 },
        new AbortController().signal
      );
      expect(within.length).toBe(9);
    });

    test('a declared length over maxBytes is refused before the body is read', async (): Promise<void> => {
      const arrayBuffer: jest.Mock = jest.fn();
      const response: any = { ok: true, status: 200, headers: new Headers({ 'content-length': '10' }), body: null, arrayBuffer: arrayBuffer };
      await expect(fetchRedirect(
        jest.fn(async (): Promise<any> => response) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/big', maxBytes: 4 },
        new AbortController().signal
      )).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(arrayBuffer).not.toHaveBeenCalled();
    });

    test('a body with no stream is refused without being buffered, declared length or not', async (): Promise<void> => {
      const arrayBuffer: jest.Mock = jest.fn();
      const response: any = { ok: true, status: 200, headers: new Headers(), body: {}, arrayBuffer: arrayBuffer };
      await expect(fetchRedirect(
        jest.fn(async (): Promise<any> => response) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/unbounded', maxBytes: 4 },
        new AbortController().signal
      )).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(arrayBuffer).not.toHaveBeenCalled();
    });

    test('a URL with a fragment is not taken for a redirect followed by the runtime, which drops the fragment', async (): Promise<void> => {
      const response: any = {
        ok: true,
        status: 200,
        url: 'https://cdn.example/part/1',
        headers: new Headers(),
        body: null,
        arrayBuffer: async (): Promise<ArrayBuffer> => new Uint8Array([7]).buffer,
      };
      const body: Uint8Array = await fetchRedirect(
        jest.fn(async (): Promise<any> => response) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/part/1#p2' },
        new AbortController().signal
      );
      expect(Array.from(body)).toEqual([7]);
    });

    test('a redirect URL of the wrong scheme for the endpoint is refused without a fetch', async (): Promise<void> => {
      const fetchFunction: jest.Mock = jest.fn();
      await expect(fetchRedirect(fetchFunction as any, 'wss://edge.example/connection/websocket', {
        url: 'http://cdn.example/x',
      }, new AbortController().signal)).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(fetchFunction).not.toHaveBeenCalled();
    });

    test('a redirect followed to an http URL on a wss endpoint is refused, and its body is not read', async (): Promise<void> => {
      const arrayBuffer: jest.Mock = jest.fn(async (): Promise<ArrayBuffer> => new ArrayBuffer(1));
      const fetchFunction: jest.Mock = jest.fn(async (): Promise<any> => ({
        ok: true,
        status: 200,
        url: 'http://edge.example/part/1',
        headers: new Headers(),
        body: null,
        arrayBuffer: arrayBuffer,
      }));
      await expect(fetchRedirect(fetchFunction as any, 'wss://edge.example/connection/websocket', {
        url: 'https://cdn.example/part/1',
      }, new AbortController().signal)).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(arrayBuffer).not.toHaveBeenCalled();
    });

    test('a redirect of the server is not followed: its status is refused, its body is cancelled and its target is not fetched', async (): Promise<void> => {
      const cancel: jest.Mock = jest.fn();
      const body: ReadableStream<Uint8Array> = new ReadableStream<Uint8Array>({
        pull(controller: ReadableStreamDefaultController<Uint8Array>): void {
          controller.enqueue(new Uint8Array([1]));
        },
        cancel: cancel,
      });
      const fetchFunction: jest.Mock = jest.fn(async (): Promise<any> => new Response(body, {
        status: 302,
        headers: { location: 'http://edge.example/part/1' },
      }));
      await expect(fetchRedirect(fetchFunction as any, 'wss://edge.example/connection/websocket', {
        url: 'https://cdn.example/part/1',
      }, new AbortController().signal)).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(fetchFunction).toHaveBeenCalledTimes(1);
      expect(fetchFunction).toHaveBeenCalledWith('https://cdn.example/part/1', expect.objectContaining({ redirect: 'error' }));
      expect(cancel).toHaveBeenCalledTimes(1);
    });

    test('a response for the requested URL is read on a wss endpoint', async (): Promise<void> => {
      const fetchFunction: jest.Mock = jest.fn(async (): Promise<any> => ({
        ok: true,
        status: 200,
        url: 'https://cdn.example/part/1',
        headers: new Headers(),
        body: null,
        arrayBuffer: async (): Promise<ArrayBuffer> => new Uint8Array([5]).buffer,
      }));
      const body: Uint8Array = await fetchRedirect(fetchFunction as any, 'wss://edge.example/connection/websocket', {
        url: 'https://cdn.example/part/1',
      }, new AbortController().signal);
      expect(Array.from(body)).toEqual([5]);
    });

    test('a network failure while the body is read rejects with redirectFailed', async (): Promise<void> => {
      const body: ReadableStream<Uint8Array> = new ReadableStream<Uint8Array>({
        start(controller: ReadableStreamDefaultController<Uint8Array>): void {
          controller.enqueue(new Uint8Array([1, 2]));
        },
        pull(controller: ReadableStreamDefaultController<Uint8Array>): void {
          controller.error(new TypeError('terminated'));
        },
      });
      await expect(fetchRedirect(
        jest.fn(async (): Promise<any> => new Response(body, { status: 200 })) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/broken' },
        new AbortController().signal
      )).rejects.toMatchObject({ code: errorCodes.redirectFailed });
    });

    test('an http URL is fetched on a ws endpoint, and never on wss', async (): Promise<void> => {
      const fetchFunction: jest.Mock = fetchReturning(new Uint8Array([4]), 200);
      const body: Uint8Array = await fetchRedirect(fetchFunction as any, 'ws://localhost/connection/websocket', {
        url: 'http://cdn.example/x',
      }, new AbortController().signal);
      expect(Array.from(body)).toEqual([4]);
      await expect(fetchRedirect(fetchFunction as any, 'wss://edge.example/connection/websocket', {
        url: 'http://cdn.example/y',
      }, new AbortController().signal)).rejects.toMatchObject({ code: errorCodes.redirectFailed });
    });

    test('an http URL is refused on an https endpoint, which is a secure scheme, and nothing is fetched', async (): Promise<void> => {
      const fetchFunction: jest.Mock = jest.fn();
      await expect(fetchRedirect(fetchFunction as any, 'https://edge.example/connection/websocket', {
        url: 'http://cdn.example/part/1',
      }, new AbortController().signal)).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(fetchFunction).not.toHaveBeenCalled();
    });

    test('an https URL is fetched on an https endpoint, which is a secure scheme', async (): Promise<void> => {
      const fetchFunction: jest.Mock = fetchReturning(new Uint8Array([8]), 200);
      const body: Uint8Array = await fetchRedirect(fetchFunction as any, 'https://edge.example/connection/websocket', {
        url: 'https://cdn.example/part/1',
      }, new AbortController().signal);
      expect(Array.from(body)).toEqual([8]);
      expect(fetchFunction).toHaveBeenCalledWith('https://cdn.example/part/1', expect.objectContaining({ redirect: 'error' }));
    });

    test('a redirect followed to an http URL on an https endpoint is refused, and its body is not read', async (): Promise<void> => {
      const arrayBuffer: jest.Mock = jest.fn(async (): Promise<ArrayBuffer> => new ArrayBuffer(1));
      const fetchFunction: jest.Mock = jest.fn(async (): Promise<any> => ({
        ok: true,
        status: 200,
        url: 'http://edge.example/part/1',
        headers: new Headers(),
        body: null,
        arrayBuffer: arrayBuffer,
      }));
      await expect(fetchRedirect(fetchFunction as any, 'https://edge.example/connection/websocket', {
        url: 'https://cdn.example/part/1',
      }, new AbortController().signal)).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      expect(arrayBuffer).not.toHaveBeenCalled();
    });

    test('an abort cancels a redirect fetch in progress', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const fetchFunction: jest.Mock = hangingFetch();
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchFunction as any,
        redirect: (): any => ({ url: 'https://cdn.example/slow' }),
      });
      const controller: AbortController = new AbortController();
      const call: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }), { signal: controller.signal });
      expect(await until((): boolean => fetchFunction.mock.calls.length === 1)).toBe(true);
      controller.abort();
      await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
      expect(pool.stats().redirects).toBe(0);
    });

    test('close rejects a call whose redirect fetch is in progress with connectionClosed, as the server served it', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const fetchFunction: jest.Mock = hangingFetch();
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchFunction as any,
        redirect: (): any => ({ url: 'https://cdn.example/slow' }),
      });
      const call: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }));
      expect(await until((): boolean => fetchFunction.mock.calls.length === 1)).toBe(true);
      pool.close();
      await expect(call).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    });

    test('the deadline of the call covers the redirect fetch', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const fetchFunction: jest.Mock = hangingFetch();
      const pool: RpcPool = createPool((): string => server.url, {
        fetch: fetchFunction as any,
        redirect: (): any => ({ url: 'https://cdn.example/never' }),
      });
      await expect(pool.rpc('work', bytes({ n: 1 }), { timeout: 150 })).rejects.toMatchObject({ code: errorCodes.timeout });
      expect(pool.stats().redirects).toBe(0);
    });
  });

  describe('close and tokens', () => {
    test('close rejects waiting calls with cancelled and calls in flight with connectionClosed', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url, { maxConnectionsPerEndpoint: 1 });
      const inFlight: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }));
      const waiting: Promise<RpcResult> = pool.rpc('work', bytes({ n: 2 }));
      await server.waitFor(isCall);
      pool.close();
      await expect(inFlight).rejects.toMatchObject({ code: errorCodes.connectionClosed });
      await expect(waiting).rejects.toMatchObject({ code: errorCodes.cancelled });
      await expect(pool.rpc('work', bytes({ n: 3 }))).rejects.toMatchObject({ code: errorCodes.clientClosed });
      expect(pool.stats().connections).toBe(0);
    });

    test('a call in flight at close arms no idle timer once it settles', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url, { idleTimeout: 5123 });
      const inFlight: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }));
      await server.waitFor(isCall);
      const timers: jest.SpyInstance = jest.spyOn(global, 'setTimeout');
      pool.close();
      await expect(inFlight).rejects.toMatchObject({ code: errorCodes.connectionClosed });
      await sleep(20);
      const armed: any[] = timers.mock.calls.filter((args: any[]): boolean => Math.abs(Number(args[1]) - 5123) <= 5);
      expect(armed.length).toBe(0);
    });

    test('a connection lost with a call in flight arms no idle timer, and close leaves none', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url, { idleTimeout: 5123 });
      const timers: jest.SpyInstance = jest.spyOn(global, 'setTimeout');
      const inFlight: Promise<RpcResult> = pool.rpc('work', bytes({ n: 1 }));
      await server.waitFor(isCall);
      closeSocket(server, 1009);
      await expect(inFlight).rejects.toMatchObject({ code: errorCodes.connectionClosed });
      pool.close();
      await sleep(20);
      const armed: any[] = timers.mock.calls.filter((args: any[]): boolean => Math.abs(Number(args[1]) - 5123) <= 5);
      expect(armed.length).toBe(0);
    });

    test('getToken is called for each connection of the pool', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let held: number = 0;
      server.onCommand = (cmd: any): any => {
        if (isCall(cmd)) {
          held++;
          return holdReply;
        }
        return undefined;
      };
      const getToken: jest.Mock = jest.fn(async (): Promise<string> => 'token');
      const pool: RpcPool = createPool((): string => server.url, {
        maxConnectionsPerEndpoint: 2,
        connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50, getToken: getToken },
      });
      const calls: Promise<RpcResult>[] = [pool.rpc('work', bytes({ n: 1 })), pool.rpc('work', bytes({ n: 2 }))];
      expect(await until((): boolean => held === 2)).toBe(true);
      expect(getToken).toHaveBeenCalledTimes(2);
      expect(pool.stats().connections).toBe(2);
      pool.close();
      for (const call of calls) {
        await expect(call).rejects.toMatchObject({ code: errorCodes.connectionClosed });
      }
    });
  });
});
