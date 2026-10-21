import { Readable } from 'stream';
import WebSocket from 'ws';
import { errorCodes, disconnectedCodes } from './codes';
import { RpcPool, RpcPoolOptions } from './rpc_pool';
import { fetchRedirect } from './rpc_pool_redirect';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { RpcResult, State } from './types';
import { holdFetchUntilAborted } from './testing/fetch';
import {
  WaitableFakeServer, bytes, fromBytes, holdReply, rpcOptionsWireFlag, until,
} from './testing/codecs';

// Polls a condition over real time, for a state which waits for the round trips of the fake server; false when the
// budget runs out first.
async function within(condition: () => boolean, budgetMs: number): Promise<boolean> {
  const started: number = Date.now();
  while (!condition()) {
    if (Date.now() - started > budgetMs) {
      return false;
    }
    await new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, 10);
    });
  }
  return true;
}

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

// A response which reports the URL it was fetched from, as a runtime does when it follows no hop (I7).
function responseAt(url: string, body: BodyInit | null, init: ResponseInit): Response {
  const response: Response = new Response(body, init);
  Object.defineProperty(response, 'url', { value: url });
  return response;
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
      // Which endpoint goes live first is not fixed: its connection takes the head of the line, two calls, and the other
      // connection takes the next two.
      const firstServer: WaitableFakeServer = heldA.some((cmd: any): boolean => fromBytes(cmd.rpc.data).n === 1) ? serverA : serverB;
      const firstHeld: any[] = firstServer === serverA ? heldA : heldB;
      const otherHeld: any[] = firstServer === serverA ? heldB : heldA;
      expect(fromBytes(firstHeld[1].rpc.data)).toEqual({ n: 2 });
      expect(otherHeld.map((cmd: any): any => fromBytes(cmd.rpc.data))).toEqual([{ n: 3 }, { n: 4 }]);

      reply(firstServer, firstHeld[0], { n: 1 });
      expect(fromBytes((await calls[0]).data)).toEqual({ n: 1 });
      expect(await until((): boolean => firstHeld.length === 3)).toBe(true);
      expect(fromBytes(firstHeld[2].rpc.data)).toEqual({ n: 5 });
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
      expect((pool as any).lines.get('').head()).toBeNull();
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
      // The opener waits for its connect in its line, so all four calls count in queued and none in flight.
      expect(pool.stats().queued).toBe(4);
      expect(pool.stats().inflight).toBe(0);

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

    test('a connection timeout beyond the longest timer is refused with badConfiguration', (): void => {
      let thrown: any;
      try {
        createPool((): string => 'ws://127.0.0.1:1', { connection: { websocket: WebSocket, timeout: 2147483648 } });
      } catch (e) {
        thrown = e;
      }
      expect(thrown).toMatchObject({ code: errorCodes.badConfiguration });
    });

    test('maxQueued 0 admits the first call of a key not resolved yet, and its connection opens once the key resolves', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const pool: RpcPool = createPool((): Promise<string[]> => Promise.resolve([server.url]), { maxQueued: 0 });
      expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
    });

    test('maxQueued 0 refuses a second call for a key still resolving', async (): Promise<void> => {
      const pool: RpcPool = createPool((): Promise<string[]> => new Promise<string[]>((): void => {}), { maxQueued: 0 });
      quiet(pool.rpc('work', bytes({ n: 1 })));
      await expect(pool.rpc('work', bytes({ n: 2 }))).rejects.toMatchObject({ code: errorCodes.limitExceeded });
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
      // The connection arms its idle check once, at its open, and no check per call.
      expect(armed.length).toBeLessThanOrEqual(1);
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

    test('connectThatThrowsLeavesNoConnection', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      // The runtime has no WebSocket: the connect of the first connection throws.
      const savedWebSocket: any = (globalThis as any).WebSocket;
      (globalThis as any).WebSocket = undefined;
      const pool: RpcPool = new RpcPool({
        endpoints: (): string => server.url,
        connection: { minReconnectDelay: 100, maxReconnectDelay: 100 },
        maxConnectionsPerEndpoint: 1,
      });
      const errors: any[] = [];
      pool.on('error', (ctx: any): void => {
        errors.push(ctx);
      });
      try {
        const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
        expect(await within((): boolean => errors.length === 1, 5000)).toBe(true);
        expect(errors[0].error).toBeInstanceOf(Error);
        expect(pool.stats()).toEqual({ connections: 0, inflight: 0, queued: 1, redirects: 0 });
        (globalThis as any).WebSocket = savedWebSocket;
        // The next open waits the hold of the endpoint: the call stays queued until it ends.
        const started: number = Date.now();
        const second: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
        expect(fromBytes((await first).data)).toEqual({ n: 1 });
        expect(Date.now() - started).toBeGreaterThanOrEqual(90);
        expect(fromBytes((await second).data)).toEqual({ n: 2 });
      } finally {
        (globalThis as any).WebSocket = savedWebSocket;
        pool.close();
      }
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

    test('a move that also carries the later signal is taken as a move, at once', async (): Promise<void> => {
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      // The later signal would resend to key a, at once with a zero draw: the move wins, so the call goes to key b.
      serverA.onCommand = (cmd: any): any => (isCall(cmd)
        ? { id: cmd.id, error: { ...laterError(), metadata: { 'x-centrifugo-retry': 'later', 'x-moved': 'b' } } }
        : undefined);
      serverB.onCommand = echo;
      jest.spyOn(Math, 'random').mockReturnValue(0);
      const pool: RpcPool = createPool((key: string): string => (key === 'a' ? serverA.url : serverB.url), {
        reroute: (error: any): string | undefined => error.metadata?.['x-moved'],
      });
      const result: RpcResult = await pool.rpc('work', bytes({ n: 3 }), { endpointKey: 'a' });
      expect(fromBytes(result.data)).toEqual({ n: 3 });
      expect(callsOf(serverA)).toHaveLength(1);
      expect(callsOf(serverB)).toHaveLength(1);
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
      return jest.fn(async (url: string): Promise<any> => responseAt(url, body as any, { status: status }));
    }

    // A fetch stub which never answers: it fails when the signal it is given aborts.
    function hangingFetch(): jest.Mock {
      return jest.fn((...args: any[]): Promise<any> => holdFetchUntilAborted(args[1].signal));
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
      const fetchFunction: jest.Mock = jest.fn(async (url: string): Promise<any> => {
        running++;
        peak = Math.max(peak, running);
        await sleep(30);
        running--;
        return responseAt(url, new Uint8Array([9]), { status: 200 });
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

    test('a fetch waiting for a slot counts in stats().queued, and the total of waiting calls and fetches stays at maxQueued', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: bytes({}) } } : undefined);
      const pool: RpcPool = createPool((): string => server.url, {
        maxQueued: 3,
        maxRedirectsInflight: 1,
        // One connection: the fake server answers on its latest socket only, and the calls here read the fetch slots.
        maxConnectionsPerEndpoint: 1,
        fetch: hangingFetch() as any,
        redirect: (): any => ({ url: 'https://cdn.example/x' }),
      });
      const refused: any[] = [];
      const settled: Promise<any>[] = [1, 2, 3, 4].map((n: number): Promise<any> => pool.rpc('work', bytes({ n: n })).then((): string => 'resolved', (error: any): any => {
        refused.push(error);
        return error;
      }));
      // One fetch holds the slot, two wait for it, and the fourth call is refused: the waiting fetches are in maxQueued.
      expect(await within((): boolean => refused.length === 1 && pool.stats().redirects === 1 && pool.stats().queued === 2, 5000)).toBe(true);
      expect(refused[0]).toMatchObject({ code: errorCodes.limitExceeded });
      pool.close();
      const outcomes: any[] = await Promise.all(settled);
      expect(outcomes.filter((outcome: any): boolean => outcome.code === errorCodes.connectionClosed)).toHaveLength(3);
    });

    test('a fetch which gets a free slot is not counted in queued', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, rpc: { data: bytes({}) } } : undefined);
      const fetchFunction: jest.Mock = hangingFetch();
      const pool: RpcPool = createPool((): string => server.url, {
        maxQueued: 1,
        maxRedirectsInflight: 1,
        fetch: fetchFunction as any,
        redirect: (): any => ({ url: 'https://cdn.example/x' }),
      });
      const first: Promise<any> = pool.rpc('work', bytes({ n: 1 })).then((): string => 'resolved', (error: any): any => error);
      expect(await until((): boolean => pool.stats().redirects === 1)).toBe(true);
      expect(pool.stats().queued).toBe(0);
      pool.close();
      await first;
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
        jest.fn(async (url: string): Promise<any> => ({ ok: true, status: 200, url: url, headers: new Headers(), body: Readable.from(chunks) })) as any,
        'wss://edge.example/connection/websocket',
        { url: 'https://cdn.example/stream', maxBytes: 4 },
        new AbortController().signal
      );
      await expect(over).rejects.toMatchObject({ code: errorCodes.redirectFailed });
      const within: Uint8Array = await fetchRedirect(
        jest.fn(async (url: string): Promise<any> => ({ ok: true, status: 200, url: url, headers: new Headers(), body: Readable.from(chunks) })) as any,
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
        jest.fn(async (url: string): Promise<any> => responseAt(url, body, { status: 200 })) as any,
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
      // The fetch slot is freed when the aborted fetch rejects, which follows the settlement of the call.
      expect(await until((): boolean => pool.stats().redirects === 0)).toBe(true);
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
      // The fetch slot is freed when the expired fetch rejects, which follows the settlement of the call.
      expect(await until((): boolean => pool.stats().redirects === 0)).toBe(true);
    });
  });

  describe('behaviour table', () => {
    // The error of a call, or 'resolved' when it resolved.
    function settledOf(promise: Promise<RpcResult>): Promise<any> {
      return promise.then((): string => 'resolved', (error: any): any => error);
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

    test('callWaitsForItsConnectionToConnect', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      let connectCmd: any = null;
      server.onCommand = (cmd: any): any => {
        if (isConnect(cmd) && connectCmd === null) {
          connectCmd = cmd;
          return holdReply;
        }
        return isCall(cmd) ? holdReply : undefined;
      };
      const pool: RpcPool = createPool((): string => server.url);
      const pending: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
      expect(await within((): boolean => connectCmd !== null, 5000)).toBe(true);
      await sleep(50);
      expect(callsOf(server)).toHaveLength(0);
      expect(pool.stats()).toEqual({ connections: 1, inflight: 0, queued: 1, redirects: 0 });
      server.send({ id: connectCmd.id, connect: server.connectResult });
      expect(await within((): boolean => callsOf(server).length === 1, 5000)).toBe(true);
      expect(pool.stats()).toEqual({ connections: 1, inflight: 1, queued: 0, redirects: 0 });
      reply(server, callsOf(server)[0], { n: 1 });
      expect(fromBytes((await pending).data)).toEqual({ n: 1 });
    });

    test('connectionsAreSharedByKeysOfOneEndpoint', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((): string => server.url, { maxConnectionsPerEndpoint: 1, maxInflightPerConnection: 2 });
      const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' }), { endpointKey: 'a' }));
      const b: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'b' }), { endpointKey: 'b' }));
      expect(await within((): boolean => callsOf(server).length === 2, 5000)).toBe(true);
      expect(connectsOf(server)).toHaveLength(1);
      expect(pool.stats().connections).toBe(1);
      reply(server, callsOf(server)[0], { n: 'a' });
      reply(server, callsOf(server)[1], { n: 'b' });
      expect(fromBytes((await a).data)).toEqual({ n: 'a' });
      expect(fromBytes((await b).data)).toEqual({ n: 'b' });
    });

    test('reroutedCallGoesToTheFrontOfItsNewLine', async (): Promise<void> => {
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      serverA.onCommand = (cmd: any): any => (isCall(cmd) ? { id: cmd.id, error: moveError('b') } : undefined);
      serverB.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      const pool: RpcPool = createPool((key: string): string => (key === 'a' ? serverA.url : serverB.url), {
        maxConnectionsPerEndpoint: 1,
        reroute: (error: any): string | undefined => error?.metadata?.['x-moved'],
      });
      const b0: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'b0' }), { endpointKey: 'b' }));
      expect(await within((): boolean => callsOf(serverB).length === 1, 5000)).toBe(true);
      const b1: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'b1' }), { endpointKey: 'b' }));
      const a: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a' }), { endpointKey: 'a' }));
      expect(await within((): boolean => callsOf(serverA).length === 1, 5000)).toBe(true);
      expect(await within((): boolean => pool.stats().queued === 2, 5000)).toBe(true);
      reply(serverB, callsOf(serverB)[0], { n: 'b0' });
      expect(await within((): boolean => callsOf(serverB).length === 2, 5000)).toBe(true);
      expect(fromBytes(callsOf(serverB)[1].rpc.data)).toEqual({ n: 'a' });
      reply(serverB, callsOf(serverB)[1], { n: 'a' });
      expect(await within((): boolean => callsOf(serverB).length === 3, 5000)).toBe(true);
      expect(fromBytes(callsOf(serverB)[2].rpc.data)).toEqual({ n: 'b1' });
      reply(serverB, callsOf(serverB)[2], { n: 'b1' });
      await Promise.all([b0, a, b1]);
    });

    test('refusesTimerDurationsAboveTheLimit', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const endpoints: () => string = (): string => server.url;
      expect(thrownBy((): RpcPool => new RpcPool({ endpoints: endpoints, idleTimeout: 2147483648 }))).toMatchObject({ code: errorCodes.badConfiguration });
      expect(thrownBy((): RpcPool => new RpcPool({ endpoints: endpoints, connection: { timeout: 2147483648 } }))).toMatchObject({ code: errorCodes.badConfiguration });
      expect(thrownBy((): RpcPool => new RpcPool({ endpoints: endpoints, connection: { maxReconnectDelay: 2147483648 } }))).toMatchObject({ code: errorCodes.badConfiguration });
      const pool: RpcPool = createPool(endpoints, { connection: { websocket: WebSocket, timeout: 5000 } });
      const refused: any = await settledOf(pool.rpc('work', bytes({ n: 1 }), { timeout: 5001 }));
      expect(refused).toMatchObject({ code: errorCodes.badConfiguration });
      expect(connectsOf(server)).toHaveLength(0);
    });

    test('saturatedKeyDoesNotHoldBackAnotherKey', async (): Promise<void> => {
      const serverA: WaitableFakeServer = await startServer();
      const serverB: WaitableFakeServer = await startServer();
      serverA.onCommand = (cmd: any): any => (isCall(cmd) ? holdReply : undefined);
      serverB.onCommand = echo;
      const pool: RpcPool = createPool((key: string): string => (key === 'a' ? serverA.url : serverB.url), { maxConnectionsPerEndpoint: 1 });
      const a1: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a1' }), { endpointKey: 'a' }));
      expect(await within((): boolean => callsOf(serverA).length === 1, 5000)).toBe(true);
      const a2: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 'a2' }), { endpointKey: 'a' }));
      expect(pool.stats().queued).toBe(1);
      const b: Promise<RpcResult> = pool.rpc('work', bytes({ n: 'b' }), { endpointKey: 'b' });
      expect(fromBytes((await b).data)).toEqual({ n: 'b' });
      expect(pool.stats().queued).toBe(1);
      // The reply of B's connection finds no key waiting on its endpoint: key a's line is not looked up.
      const drainedBefore: number = (pool as any).drainedKeys;
      const b2: Promise<RpcResult> = pool.rpc('work', bytes({ n: 'b2' }), { endpointKey: 'b' });
      expect(fromBytes((await b2).data)).toEqual({ n: 'b2' });
      expect((pool as any).drainedKeys - drainedBefore).toBe(0);
      expect(pool.stats().queued).toBe(1);
      reply(serverA, callsOf(serverA)[0], { n: 'a1' });
      expect(fromBytes((await a1).data)).toEqual({ n: 'a1' });
      expect(await within((): boolean => callsOf(serverA).length === 2, 5000)).toBe(true);
      reply(serverA, callsOf(serverA)[1], { n: 'a2' });
      expect(fromBytes((await a2).data)).toEqual({ n: 'a2' });
    });

    test('firstCallOfAnUnresolvedKeyIsAdmittedAtAnyBound', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      let resolveKey: (url: string) => void = (): void => {};
      const pool: RpcPool = createPool((): Promise<string> => new Promise<string>((resolve: (url: string) => void): void => {
        resolveKey = resolve;
      }), { maxQueued: 0 });
      const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
      const second: any = await settledOf(pool.rpc('work', bytes({ n: 2 })));
      expect(second).toMatchObject({ code: errorCodes.limitExceeded });
      await sleep(20);
      resolveKey(server.url);
      expect(fromBytes((await first).data)).toEqual({ n: 1 });
      expect(connectsOf(server)).toHaveLength(1);
    });

    test('terminalCloseHoldsTheEndpointOpens', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      const errors: any[] = [];
      const pool: RpcPool = createPool((): string => server.url, {
        connection: { websocket: WebSocket, minReconnectDelay: 100, maxReconnectDelay: 100 },
      });
      pool.on('error', (ctx: any): void => {
        errors.push(ctx);
      });
      expect(fromBytes((await pool.rpc('work', bytes({ n: 1 }))).data)).toEqual({ n: 1 });
      const endpoint: any = (pool as any).endpoints.get(server.url);
      // The fleet closes the connection for good: the error is reported, and the next open of the endpoint waits the hold.
      server.send({ push: { disconnect: { code: 3501, reason: 'closed' } } });
      expect(await within((): boolean => errors.length === 1, 5000)).toBe(true);
      expect(endpoint.closes).toBe(1);
      const started: number = Date.now();
      expect(fromBytes((await pool.rpc('work', bytes({ n: 2 }))).data)).toEqual({ n: 2 });
      expect(Date.now() - started).toBeGreaterThanOrEqual(80);
      expect(connectsOf(server)).toHaveLength(2);
      // The connected event of the next connection resets the count: the next close holds the opens of step 0 again.
      expect(endpoint.closes).toBe(0);
      server.send({ push: { disconnect: { code: 3501, reason: 'closed' } } });
      expect(await within((): boolean => errors.length === 2, 5000)).toBe(true);
      expect(endpoint.closes).toBe(1);
    });

    test('retiredConnectionCloseReportsNoErrorAndHoldsNoOpen', async (): Promise<void> => {
      const main: WaitableFakeServer = await startServer();
      main.connectResult = { client: 'main-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag, user: 'alice' };
      main.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? { id: cmd.id, refresh: { expires: true, ttl: 900, user: 'bob' } } : undefined);
      const client: ProtobufCentrifuge = new ProtobufCentrifuge([{ transport: 'websocket', endpoint: main.url }], {
        websocket: WebSocket,
        token: 'alice-token',
        minReconnectDelay: 10,
        maxReconnectDelay: 50,
      });
      client.on('error', (): void => {});
      try {
        client.connect();
        expect(await within((): boolean => client.state === State.Connected, 5000)).toBe(true);
        const edge: WaitableFakeServer = await startServer();
        edge.connectResult = { client: 'edge', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag, user: 'alice' };
        const held: any[] = [];
        edge.onCommand = (cmd: any): any => {
          if (isCall(cmd)) {
            held.push(cmd);
            return holdReply;
          }
          return undefined;
        };
        // One connection of two calls in flight: the first stays in flight, the second settles it after the change.
        const pool: RpcPool = createPool((): string => edge.url, { tokenFrom: client, maxConnectionsPerEndpoint: 1, maxInflightPerConnection: 2 });
        const errors: any[] = [];
        pool.on('error', (ctx: any): void => {
          errors.push(ctx);
        });
        const inFlight: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
        const settling: Promise<RpcResult> = pool.rpc('work', bytes({ n: 3 }));
        expect(await within((): boolean => callsOf(edge).length === 2, 5000)).toBe(true);
        // The main client changes identity: the connection of alice is stale. Its second call settles, and the slot freed
        // retires the connection, with its first call still in flight.
        await client.refresh('bob-token');
        edge.connectResult = { ...edge.connectResult, user: 'bob' };
        edge.send({ id: held[1].id, rpc: { data: held[1].rpc.data } });
        expect(fromBytes((await settling).data)).toEqual({ n: 3 });
        // The fleet closes the retired connection for good: no error is reported, and the next call opens a connection at once.
        edge.send({ push: { disconnect: { code: 3500, reason: 'closed' } } });
        await inFlight.catch((): void => {});
        // The retired close holds no open: the endpoint has no hold timer, right after it.
        expect((pool as any).endpoints.get(edge.url).hold).toBeNull();
        const started: number = Date.now();
        const next: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
        expect(await within((): boolean => connectsOf(edge).length === 2, 1000)).toBe(true);
        expect(Date.now() - started).toBeLessThan(1000);
        expect(errors).toEqual([]);
        expect(next).toBeDefined();
      } finally {
        client.disconnect();
      }
    });

    test('callsWaitingForAResolutionCountInQueued', async (): Promise<void> => {
      const server: WaitableFakeServer = await startServer();
      server.onCommand = echo;
      let resolveKey: (url: string) => void = (): void => {};
      const pool: RpcPool = createPool((): Promise<string> => new Promise<string>((resolve: (url: string) => void): void => {
        resolveKey = resolve;
      }), { maxQueued: 3, maxConnectionsPerEndpoint: 1 });
      const first: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 1 })));
      const second: Promise<RpcResult> = quiet(pool.rpc('work', bytes({ n: 2 })));
      expect(pool.stats().queued).toBe(2);
      await sleep(20);
      resolveKey(server.url);
      expect(fromBytes((await first).data)).toEqual({ n: 1 });
      expect(fromBytes((await second).data)).toEqual({ n: 2 });
      expect(pool.stats().queued).toBe(0);
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
      // The connection arms its idle check once, at its open; the loss does not arm another one.
      expect(armed.length).toBeLessThanOrEqual(1);
    });
  });
});
