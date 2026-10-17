import { errorCodes } from './codes';
import { fetchRedirect, RedirectGate } from './rpc_pool_redirect';

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
