import { errorCodes } from './codes';
import { fetchRedirect, RedirectGate } from './rpc_pool_redirect';

describe('RedirectGate', () => {
  test('refuses a fetch which would wait beyond maxWaiters with limitExceeded', async (): Promise<void> => {
    const gate: RedirectGate = new RedirectGate(1, 1);
    const release: () => void = await gate.acquire(new AbortController().signal);
    const waiting: Promise<() => void> = gate.acquire(new AbortController().signal);
    let refused: any;
    try {
      await gate.acquire(new AbortController().signal);
    } catch (e) {
      refused = e;
    }
    expect(refused.code).toBe(errorCodes.limitExceeded);
    release();
    const granted: () => void = await waiting;
    expect(gate.inflight).toBe(1);
    granted();
    expect(gate.inflight).toBe(0);
  });

  test('a zero maxWaiters still grants a free slot, and refuses only a fetch which would wait', async (): Promise<void> => {
    const gate: RedirectGate = new RedirectGate(1, 0);
    const release: () => void = await gate.acquire(new AbortController().signal);
    expect(gate.full).toBe(true);
    let refused: any;
    try {
      await gate.acquire(new AbortController().signal);
    } catch (e) {
      refused = e;
    }
    expect(refused.code).toBe(errorCodes.limitExceeded);
    release();
    expect(gate.inflight).toBe(0);
    expect(gate.full).toBe(false);
    const again: () => void = await gate.acquire(new AbortController().signal);
    expect(gate.inflight).toBe(1);
    again();
  });

  test('an aborted waiter leaves the line without taking a slot, and the others are granted in arrival order', async (): Promise<void> => {
    const gate: RedirectGate = new RedirectGate(1, 8);
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
      url: '',
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
});
