import WebSocket from 'ws';
import { errorCodes } from './codes';
import { RpcPool } from './rpc_pool';
import { fetchRedirect, RedirectGate } from './rpc_pool_redirect';
import { RpcResult } from './types';
import { WaitableFakeServer, rpcOptionsWireFlag, within } from './testing/codecs';
import { holdFetchUntilAborted } from './testing/fetch';

describe('RedirectGate', () => {
  test('a fetch waits for a slot without a bound of its own: the pool bounds the waiters', async (): Promise<void> => {
    const gate: RedirectGate = new RedirectGate(1);
    const release: () => void = await gate.acquire(new AbortController().signal);
    expect(gate.full).toBe(true);
    const waiting: Promise<() => void> = gate.acquire(new AbortController().signal);
    const second: Promise<() => void> = gate.acquire(new AbortController().signal);
    release();
    const granted: () => void = await waiting;
    expect(gate.inflight).toBe(1);
    granted();
    const grantedSecond: () => void = await second;
    expect(gate.inflight).toBe(1);
    grantedSecond();
    expect(gate.inflight).toBe(0);
    expect(gate.full).toBe(false);
  });

  test('an aborted waiter leaves the line without taking a slot, and the others are granted in arrival order', async (): Promise<void> => {
    const gate: RedirectGate = new RedirectGate(1);
    const release: () => void = await gate.acquire(new AbortController().signal);
    const first: Promise<() => void> = gate.acquire(new AbortController().signal);
    const abort: AbortController = new AbortController();
    const aborted: Promise<() => void> = gate.acquire(abort.signal);
    const last: Promise<() => void> = gate.acquire(new AbortController().signal);
    abort.abort();
    await expect(aborted).rejects.toMatchObject({ code: errorCodes.cancelled });
    release();
    const releaseFirst: () => void = await first;
    expect(gate.inflight).toBe(1);
    releaseFirst();
    const releaseLast: () => void = await last;
    expect(gate.inflight).toBe(1);
    releaseLast();
    expect(gate.inflight).toBe(0);
  });
});


describe('fetchRedirect', () => {
  test('refuses a response with no body property before it is buffered', async (): Promise<void> => {
    let buffered: boolean = false;
    const fetchFunction: any = async (): Promise<any> => ({
      ok: true,
      status: 200,
      url: 'https://files.example.test/part',
      headers: new Map<string, string>(),
      body: undefined,
      arrayBuffer: async (): Promise<ArrayBuffer> => {
        buffered = true;
        return new ArrayBuffer(8);
      },
    });
    const failed: any = await fetchRedirect(fetchFunction, 'wss://edge.example.test', { url: 'https://files.example.test/part' }, new AbortController().signal)
      .then((): string => 'resolved', (error: any): any => error);
    expect(failed.code).toBe(errorCodes.redirectFailed);
    expect(buffered).toBe(false);
  });

  test('redirectFetchCarriesNoApplicationCredentials', async (): Promise<void> => {
    const calls: any[] = [];
    const fetchFunction: any = async (url: string, init: any): Promise<any> => {
      calls.push({ url: url, init: init });
      return { ok: true, status: 200, url: 'https://files.example.test/a', headers: new Map<string, string>(), body: null, arrayBuffer: async (): Promise<ArrayBuffer> => new ArrayBuffer(0) };
    };
    await fetchRedirect(fetchFunction, 'wss://edge.example.test', { url: 'https://files.example.test/a', headers: { 'x-part': '1' } }, new AbortController().signal);
    expect(calls).toHaveLength(1);
    expect(calls[0].init.credentials).toBe('omit');
    expect(calls[0].init.redirect).toBe('error');
    expect(calls[0].init.headers).toEqual({ 'x-part': '1' });
  });

  test('redirect3xxFailsTheFetch', async (): Promise<void> => {
    const calls: string[] = [];
    const fetchFunction: any = async (url: string): Promise<any> => {
      calls.push(url);
      return { ok: false, status: 302, url: url, headers: new Map<string, string>(), body: null, arrayBuffer: async (): Promise<ArrayBuffer> => new ArrayBuffer(0) };
    };
    const failed: any = await fetchRedirect(fetchFunction, 'wss://edge.example.test', { url: 'https://files.example.test/a' }, new AbortController().signal)
      .then((): string => 'resolved', (error: any): any => error);
    expect(failed.code).toBe(errorCodes.redirectFailed);
    expect(calls).toHaveLength(1);
  });

  test('httpRedirectIsRefusedFromASecureEndpoint', async (): Promise<void> => {
    const calls: string[] = [];
    const fetchFunction: any = async (url: string): Promise<any> => {
      calls.push(url);
      return { ok: true, status: 200, url: url, headers: new Map<string, string>(), body: null, arrayBuffer: async (): Promise<ArrayBuffer> => new ArrayBuffer(0) };
    };
    const target: string = 'http://files.example.test/a';
    for (const endpoint of ['wss://edge.example.test', 'https://edge.example.test']) {
      const failed: any = await fetchRedirect(fetchFunction, endpoint, { url: target }, new AbortController().signal)
        .then((): string => 'resolved', (error: any): any => error);
      expect(failed.code).toBe(errorCodes.redirectFailed);
    }
    expect(calls).toHaveLength(0);
    await fetchRedirect(fetchFunction, 'ws://edge.example.test', { url: target }, new AbortController().signal);
    expect(calls).toEqual([target]);
  });

  test('redirectFailureKeepsItsCauseAndReleasesTheBody', async (): Promise<void> => {
    const boom: Error = new Error('boom');
    let cancelled: number = 0;
    const brokenBody: any = {
      getReader: (): any => {
        let chunks: number = 0;
        return {
          read: async (): Promise<any> => {
            chunks++;
            if (chunks === 1) {
              return { done: false, value: new Uint8Array([1, 2]) };
            }
            throw boom;
          },
          cancel: async (): Promise<void> => {
            cancelled++;
          },
        };
      },
      cancel: async (): Promise<void> => {
        cancelled++;
      },
    };
    const declared: (length: string) => any = (length: string): any => new Map<string, string>([['content-length', length]]);
    const brokenResponse: any = { ok: true, status: 200, url: 'https://files.example.test/a', headers: declared('10'), body: brokenBody };
    const failedRead: any = await fetchRedirect(async (): Promise<any> => brokenResponse, 'wss://edge.example.test', { url: 'https://files.example.test/a', maxBytes: 100 }, new AbortController().signal)
      .then((): string => 'resolved', (error: any): any => error);
    expect(failedRead.code).toBe(errorCodes.redirectFailed);
    expect(failedRead.cause).toBe(boom);
    expect(cancelled).toBe(1);

    cancelled = 0;
    const overDeclared: any = { ok: true, status: 200, url: 'https://files.example.test/a', headers: declared('100'), body: brokenBody };
    const failedDeclared: any = await fetchRedirect(async (): Promise<any> => overDeclared, 'wss://edge.example.test', { url: 'https://files.example.test/a', maxBytes: 10 }, new AbortController().signal)
      .then((): string => 'resolved', (error: any): any => error);
    expect(failedDeclared.code).toBe(errorCodes.redirectFailed);
    expect(cancelled).toBe(1);

    cancelled = 0;
    const chunked: any = {
      getReader: (): any => {
        const parts: Uint8Array[] = [new Uint8Array(6), new Uint8Array(6)];
        return {
          read: async (): Promise<any> => (parts.length > 0 ? { done: false, value: parts.shift() } : { done: true }),
          cancel: async (): Promise<void> => {
            cancelled++;
          },
        };
      },
    };
    // The declared length is within the limit; the bytes read pass it.
    const overLimit: any = { ok: true, status: 200, url: 'https://files.example.test/a', headers: declared('6'), body: chunked };
    const failedChunks: any = await fetchRedirect(async (): Promise<any> => overLimit, 'wss://edge.example.test', { url: 'https://files.example.test/a', maxBytes: 10 }, new AbortController().signal)
      .then((): string => 'resolved', (error: any): any => error);
    expect(failedChunks.code).toBe(errorCodes.redirectFailed);
    expect(cancelled).toBe(1);
  });

  test('runtimeFollowedHopIsRefusedBeforeTheBody', async (): Promise<void> => {
    let cancelled: number = 0;
    let read: number = 0;
    const response: any = {
      ok: true,
      status: 200,
      url: 'https://other.example.test/x',
      headers: new Map<string, string>(),
      body: {
        getReader: (): any => ({ read: async (): Promise<any> => { read++; return { done: true }; }, cancel: async (): Promise<void> => { cancelled++; } }),
        cancel: async (): Promise<void> => { cancelled++; },
      },
      arrayBuffer: async (): Promise<ArrayBuffer> => { read++; return new ArrayBuffer(0); },
    };
    const failed: any = await fetchRedirect(async (): Promise<any> => response, 'wss://edge.example.test', { url: 'https://files.example.test/a' }, new AbortController().signal)
      .then((): string => 'resolved', (error: any): any => error);
    expect(failed.code).toBe(errorCodes.redirectFailed);
    expect(read).toBe(0);
    expect(cancelled).toBeGreaterThan(0);
  });

  test('aRuntimeReportingNoResponseUrlIsAccepted', async (): Promise<void> => {
    // A runtime with an empty response URL (a fetch polyfill) is read, as a compliant runtime follows no hop.
    const response: any = {
      ok: true,
      status: 200,
      url: '',
      headers: new Map<string, string>(),
      body: null,
      arrayBuffer: async (): Promise<ArrayBuffer> => new ArrayBuffer(0),
    };
    const data: Uint8Array = await fetchRedirect(async (): Promise<any> => response, 'wss://edge.example.test', { url: 'https://files.example.test/a' }, new AbortController().signal);
    expect(data).toHaveLength(0);
  });

  test('aRuntimeReportingARedirectIsRefusedWhateverItsUrl', async (): Promise<void> => {
    // A response with redirected true is refused before the body is read, with an empty URL or the requested one.
    const requested: string = 'https://files.example.test/a';
    for (const reportedUrl of ['', requested]) {
      let read: number = 0;
      let cancelled: number = 0;
      const response: any = {
        ok: true,
        status: 200,
        url: reportedUrl,
        redirected: true,
        headers: new Map<string, string>(),
        body: {
          getReader: (): any => {
            read++;
            return {};
          },
          cancel: async (): Promise<void> => {
            cancelled++;
          },
        },
        arrayBuffer: async (): Promise<ArrayBuffer> => {
          read++;
          return new ArrayBuffer(0);
        },
      };
      const failed: any = await fetchRedirect(async (): Promise<any> => response, 'wss://edge.example.test', { url: requested }, new AbortController().signal)
        .then((): string => 'resolved', (error: any): any => error);
      expect(failed.code).toBe(errorCodes.redirectFailed);
      expect(read).toBe(0);
      expect(cancelled).toBe(1);
    }
  });

  test('aRuntimeReportingNoRedirectAndNoUrlIsReadAsBefore', async (): Promise<void> => {
    const response: any = {
      ok: true,
      status: 200,
      url: '',
      redirected: false,
      headers: new Map<string, string>(),
      body: null,
      arrayBuffer: async (): Promise<ArrayBuffer> => new ArrayBuffer(0),
    };
    const data: Uint8Array = await fetchRedirect(async (): Promise<any> => response, 'wss://edge.example.test', { url: 'https://files.example.test/a' }, new AbortController().signal);
    expect(data).toHaveLength(0);
  });

  test('redirectBodyWithoutAStreamIsRefused', async (): Promise<void> => {
    let buffered: number = 0;
    const arrayBuffer: () => Promise<ArrayBuffer> = async (): Promise<ArrayBuffer> => {
      buffered++;
      return new ArrayBuffer(4);
    };
    const withoutBody: any = { ok: true, status: 200, url: 'https://files.example.test/a', headers: new Map<string, string>(), arrayBuffer: arrayBuffer };
    const notAStream: any = { ok: true, status: 200, url: 'https://files.example.test/a', headers: new Map<string, string>(), body: 42, arrayBuffer: arrayBuffer };
    for (const response of [withoutBody, notAStream]) {
      const failed: any = await fetchRedirect(async (): Promise<any> => response, 'wss://edge.example.test', { url: 'https://files.example.test/a' }, new AbortController().signal)
        .then((): string => 'resolved', (error: any): any => error);
      expect(failed.code).toBe(errorCodes.redirectFailed);
    }
    expect(buffered).toBe(0);
    const empty: any = { ok: true, status: 200, url: 'https://files.example.test/a', headers: new Map<string, string>(), body: null, arrayBuffer: async (): Promise<ArrayBuffer> => new ArrayBuffer(0) };
    const data: Uint8Array = await fetchRedirect(async (): Promise<any> => empty, 'wss://edge.example.test', { url: 'https://files.example.test/a' }, new AbortController().signal);
    expect(data.length).toBe(0);
  });
});


describe('RpcPool redirect waiters', () => {
  test('redirectWaiterCountsInMaxQueued', async (): Promise<void> => {
    const okResponse: () => any = (): any => ({ ok: true, status: 200, url: 'https://cdn.example.test/x',headers: new Map<string, string>(), body: null, arrayBuffer: async (): Promise<ArrayBuffer> => new ArrayBuffer(0) });
    const servers: WaitableFakeServer[] = [await WaitableFakeServer.start('protobuf'), await WaitableFakeServer.start('protobuf')];
    try {
      for (const server of servers) {
        server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
        server.onCommand = (cmd: any): any => (cmd.rpc !== undefined ? { id: cmd.id, rpc: { data: cmd.rpc.data } } : undefined);
      }
      // The first fetch holds the one redirect slot until the test releases it; a later fetch answers at once.
      let releaseFirst: () => void = (): void => {};
      let fetches: number = 0;
      const fetchFunction: any = (): Promise<any> => {
        fetches++;
        if (fetches === 1) {
          return new Promise<any>((resolve: (response: any) => void): void => {
            releaseFirst = (): void => resolve(okResponse());
          });
        }
        return Promise.resolve(okResponse());
      };
      const make = (server: WaitableFakeServer, maxQueued: number): RpcPool => new RpcPool({
        endpoints: (): string => server.url,
        connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50 },
        maxConnectionsPerEndpoint: 1,
        maxInflightPerConnection: 2,
        maxQueued: maxQueued,
        maxRedirectsInflight: 1,
        fetch: fetchFunction,
        redirect: (): any => ({ url: 'https://cdn.example.test/x' }),
      });
      // With maxQueued 0 the second redirect, which would wait for the slot, is refused.
      const strict: RpcPool = make(servers[0], 0);
      const first: Promise<RpcResult> = strict.rpc('work', new TextEncoder().encode('{}'));
      expect(await within((): boolean => fetches === 1, 5000)).toBe(true);
      const refused: Promise<any> = strict.rpc('work', new TextEncoder().encode('{"n":2}')).then((): string => 'resolved', (error: any): any => error);
      expect(await refused).toMatchObject({ code: errorCodes.limitExceeded });
      releaseFirst();
      await first;
      // With maxQueued 1 the second redirect waits for the slot, is counted in queued, and starts when the first ends.
      fetches = 0;
      const waiting: RpcPool = make(servers[1], 1);
      const holder: Promise<RpcResult> = waiting.rpc('work', new TextEncoder().encode('{}'));
      expect(await within((): boolean => fetches === 1, 5000)).toBe(true);
      const second: Promise<RpcResult> = waiting.rpc('work', new TextEncoder().encode('{"n":2}'));
      expect(await within((): boolean => waiting.stats().queued === 1, 5000)).toBe(true);
      releaseFirst();
      await holder;
      await second;
      strict.close();
      waiting.close();
    } finally {
      for (const server of servers) {
        await server.close();
      }
    }
  });

  test('closeSettlesAWaitingFetchWithTheClosedError', async (): Promise<void> => {
    const server: WaitableFakeServer = await WaitableFakeServer.start('protobuf');
    try {
      server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
      server.onCommand = (cmd: any): any => (cmd.rpc !== undefined ? { id: cmd.id, rpc: { data: cmd.rpc.data } } : undefined);
      // The first fetch holds the one redirect slot until the pool closes it. The second redirect waits for that slot.
      let fetches: number = 0;
      const fetchFunction: any = (...args: any[]): Promise<any> => {
        fetches++;
        return holdFetchUntilAborted(args[1].signal);
      };
      const pool: RpcPool = new RpcPool({
        endpoints: (): string => server.url,
        connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50 },
        maxConnectionsPerEndpoint: 1,
        maxInflightPerConnection: 2,
        maxRedirectsInflight: 1,
        fetch: fetchFunction,
        redirect: (): any => ({ url: 'https://cdn.example.test/x' }),
      });
      const first: Promise<RpcResult> = pool.rpc('work', new TextEncoder().encode('{"n":1}'));
      const second: Promise<RpcResult> = pool.rpc('work', new TextEncoder().encode('{"n":2}'));
      expect(await within((): boolean => fetches === 1 && pool.stats().queued === 1, 5000)).toBe(true);
      pool.close();
      await expect(first).rejects.toMatchObject({ code: errorCodes.connectionClosed });
      await expect(second).rejects.toMatchObject({ code: errorCodes.connectionClosed });
      // The waiting redirect was settled by the close and never requested its fetch.
      expect(fetches).toBe(1);
    } finally {
      await server.close();
    }
  });
});
