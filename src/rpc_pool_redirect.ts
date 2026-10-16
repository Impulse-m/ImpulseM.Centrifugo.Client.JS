import { errorCodes } from './codes';
import type { RpcPoolRedirect } from './rpc_pool';
import { poolError } from './rpc_pool_endpoint';

/** The default body limit of a redirect fetch, in bytes. */
export const defaultRedirectMaxBytes: number = 16 * 1024 * 1024;

// A waiter of the gate: its start takes a slot, and it is removed when its signal aborts first.
interface RedirectWaiter {
  start: () => void;
}


/**
 * Bounds the redirect fetches in flight over one pool. A fetch waits for a slot; a waiter whose signal aborts
 * leaves the line without taking one. No timer: a waiter waits for a release only.
 */
export class RedirectGate {
  private limit: number;
  private running: number = 0;
  private waiters: RedirectWaiter[] = [];


  constructor(limit: number) {
    this.limit = limit;
  }


  /** The fetches holding a slot. */
  get inflight(): number {
    return this.running;
  }


  /** Resolves with the release of a slot once one is free; rejects when the signal aborts first. */
  acquire(signal: AbortSignal): Promise<() => void> {
    return new Promise<() => void>((resolve: (release: () => void) => void, reject: (error: any) => void): void => {
      if (signal.aborted) {
        reject(poolError(errorCodes.cancelled, 'cancelled'));
        return;
      }
      const waiter: RedirectWaiter = {
        start: (): void => {
          signal.removeEventListener('abort', onAbort);
          this.running++;
          resolve(this.releaser());
        },
      };
      const onAbort: () => void = (): void => {
        this.waiters = this.waiters.filter((other: RedirectWaiter): boolean => other !== waiter);
        reject(poolError(errorCodes.cancelled, 'cancelled'));
      };
      signal.addEventListener('abort', onAbort, { once: true });
      this.waiters.push(waiter);
      this.grant();
    });
  }


  private releaser(): () => void {
    let held: boolean = true;
    return (): void => {
      if (held) {
        held = false;
        this.running--;
        this.grant();
      }
    };
  }


  private grant(): void {
    while (this.running < this.limit && this.waiters.length > 0) {
      const waiter: RedirectWaiter | undefined = this.waiters.shift();
      if (waiter === undefined) {
        return;
      }
      waiter.start();
    }
  }
}


/**
 * Fetches the bytes of a redirect within the signal. The URL must be https, or http with a ws endpoint (development).
 * A non-2xx status, a network failure or a body over maxBytes rejects with redirectFailed; the pool does not retry.
 */
export async function fetchRedirect(
  fetchFunction: typeof fetch | undefined,
  endpoint: string,
  redirect: RpcPoolRedirect,
  signal: AbortSignal
): Promise<Uint8Array> {
  const url: string = redirectUrl(endpoint, redirect.url);
  const maxBytes: number = redirect.maxBytes ?? defaultRedirectMaxBytes;
  if (!Number.isInteger(maxBytes) || maxBytes < 0) {
    throw poolError(errorCodes.badConfiguration, 'maxBytes must be an integer of at least 0');
  }
  if (fetchFunction === undefined) {
    throw redirectFailure('no fetch function for the redirect');
  }
  let response: any;
  try {
    response = await fetchFunction(url, { headers: redirect.headers, signal: signal });
  } catch (e) {
    throw redirectFailure('redirect fetch failed');
  }
  if (!response.ok) {
    throw redirectFailure('redirect fetch status ' + response.status);
  }
  // fetch follows the redirects of the server: the URL the body came from is checked as the hook's URL was.
  if (typeof response.url === 'string' && response.url !== '') {
    redirectUrl(endpoint, response.url);
  }
  try {
    return await readBounded(response, maxBytes);
  } catch (e: any) {
    if (e?.code === errorCodes.redirectFailed) {
      throw e;
    }
    throw redirectFailure('redirect body read failed');
  }
}


// The URL of a redirect, checked against the scheme of the endpoint it came from.
function redirectUrl(endpoint: string, target: string): string {
  let parsed: URL;
  try {
    parsed = new URL(target);
  } catch (e) {
    throw redirectFailure('redirect URL is not absolute');
  }
  if (parsed.protocol === 'https:') {
    return parsed.href;
  }
  if (parsed.protocol === 'http:' && !/^wss:/i.test(endpoint)) {
    return parsed.href;
  }
  throw redirectFailure('redirect URL scheme is not allowed for the endpoint');
}


// The body as bytes, read chunk by chunk so that a body over the limit is cut off as it arrives. A declared length over
// the limit is refused before the body is read. A response with no stream is buffered whole first, so the limit applies
// only once the body is in memory.
async function readBounded(response: any, maxBytes: number): Promise<Uint8Array> {
  if (declaredLength(response) > maxBytes) {
    throw redirectFailure('redirect body over the limit');
  }
  const source: ChunkSource | null = chunkSource(response.body);
  if (source === null) {
    const whole: Uint8Array = new Uint8Array(await response.arrayBuffer());
    if (whole.length > maxBytes) {
      throw redirectFailure('redirect body over the limit');
    }
    return whole;
  }
  const chunks: Uint8Array[] = [];
  let total: number = 0;
  for (;;) {
    const step: any = await source.next();
    if (step.done === true) {
      break;
    }
    total += step.value.length;
    if (total > maxBytes) {
      source.cancel();
      throw redirectFailure('redirect body over the limit');
    }
    chunks.push(step.value);
  }
  const out: Uint8Array = new Uint8Array(total);
  let offset: number = 0;
  for (const chunk of chunks) {
    out.set(chunk, offset);
    offset += chunk.length;
  }
  return out;
}


// The chunks of a body: next() gives the next one as {done, value}, cancel() ends the stream early.
interface ChunkSource {
  next: () => Promise<any>;
  cancel: () => void;
}


// The chunks of a response body: a WHATWG stream is read through its reader, a Node stream (as node-fetch gives) through
// its async iterator. Null when the body is neither, so it has to be buffered.
function chunkSource(body: any): ChunkSource | null {
  if (body === null || body === undefined) {
    return null;
  }
  if (typeof body.getReader === 'function') {
    const reader: any = body.getReader();
    return {
      next: (): Promise<any> => reader.read(),
      cancel: (): void => {
        reader.cancel().catch((): void => {});
      },
    };
  }
  const asyncIterator: symbol | undefined = (Symbol as any).asyncIterator;
  if (asyncIterator !== undefined && typeof body[asyncIterator] === 'function') {
    const iterator: any = body[asyncIterator]();
    return {
      next: (): Promise<any> => iterator.next(),
      cancel: (): void => {
        if (typeof iterator.return === 'function') {
          iterator.return().catch((): void => {});
        }
      },
    };
  }
  return null;
}


// The length a response declares in its Content-Length header, 0 when it declares none.
function declaredLength(response: any): number {
  const value: string | null | undefined = response.headers?.get?.('content-length');
  if (value === null || value === undefined) {
    return 0;
  }
  const length: number = Number(value);
  return Number.isFinite(length) ? length : 0;
}


function redirectFailure(message: string): any {
  return poolError(errorCodes.redirectFailed, message);
}
