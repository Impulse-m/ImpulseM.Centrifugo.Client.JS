import { errorCodes } from './codes';
import type { RpcPoolRedirect } from './rpc_pool';
import { poolError } from './rpc_pool_error';

/** The default body limit of a redirect fetch, in bytes. */
const defaultRedirectMaxBytes: number = 16 * 1024 * 1024;

/** The most a body buffer is allocated for before a byte of it arrives: a declared length above it grows as the body comes. */
const initialBodyBytes: number = 1024 * 1024;

// A waiter of the gate: its start takes a slot, and it is removed when its signal aborts first.
interface RedirectWaiter {
  start: () => void;
}


/**
 * Bounds the redirect fetches in flight over one pool. A fetch waits for a slot in arrival order; a waiter whose signal
 * aborts leaves the line without taking one. No timer: a waiter waits for a release only. The pool bounds the waiters
 * with its own maxQueued, which counts them with the calls waiting for a connection.
 */
export class RedirectGate {
  private limit: number;
  private running: number = 0;
  // The waiters in arrival order: a Set keeps the order, and an abort removes its waiter in O(1).
  private waiters: Set<RedirectWaiter> = new Set<RedirectWaiter>();


  constructor(limit: number) {
    this.limit = limit;
  }


  /** The fetches holding a slot. */
  get inflight(): number {
    return this.running;
  }


  /** True when every slot is held, so a fetch asked now would wait for one. */
  get full(): boolean {
    return this.running >= this.limit;
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
        this.waiters.delete(waiter);
        reject(poolError(errorCodes.cancelled, 'cancelled'));
      };
      signal.addEventListener('abort', onAbort, { once: true });
      this.waiters.add(waiter);
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
    while (this.running < this.limit && this.waiters.size > 0) {
      const next: RedirectWaiter | undefined = this.waiters.values().next().value;
      if (next === undefined) {
        return;
      }
      this.waiters.delete(next);
      next.start();
    }
  }
}


/**
 * Fetches the bytes of a redirect within the signal. The URL must be https, or http from an insecure endpoint (development).
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
    // A redirect of a secure endpoint is not followed by the runtime: it fails the fetch, before any hop is sent, since a
    // browser's 'manual' gives an opaque response no hop can be checked on. The hook names the final URL instead. An
    // insecure endpoint is fetched with 'manual', and its response is checked below.
    // The request carries only the redirect headers: no cookies or ambient browser credentials (credentials 'omit').
    response = await fetchFunction(url, {
      headers: redirect.headers,
      signal: signal,
      credentials: 'omit',
      redirect: secureEndpoint(endpoint) ? 'error' : 'manual',
    });
  } catch (e: any) {
    throw redirectFailure('redirect fetch failed', e);
  }
  if (!response.ok) {
    discardBody(response);
    throw redirectFailure('redirect fetch status ' + response.status);
  }
  // A runtime which followed a redirect anyway (it ignores 'error' or 'manual') is refused before its body is read. A
  // runtime that reports that it followed one (redirected true) is refused whatever its URL holds. The URL of a response
  // has no fragment, so the fragment of the requested URL is left out of the comparison. A runtime that reports neither
  // (an empty URL and no redirected flag) is accepted: a compliant runtime follows no hop under 'error' or 'manual', and
  // the check is for the runtimes that report a followed hop (ruling R4.2).
  const requested: string = url.split('#')[0];
  if (response.redirected === true) {
    discardBody(response);
    throw redirectFailure('redirect followed by the runtime');
  }
  if (typeof response.url === 'string' && response.url !== '' && response.url !== requested) {
    discardBody(response);
    throw redirectFailure('redirect followed by the runtime');
  }
  try {
    return await readBounded(response, maxBytes);
  } catch (e: any) {
    if (e?.code === errorCodes.redirectFailed) {
      throw e;
    }
    throw redirectFailure('redirect body read failed', e);
  }
}


// The URL of a redirect, checked against the scheme of the endpoint it came from. A secure endpoint (wss: or https:)
// takes only an https: URL; an http: URL is allowed only from an endpoint which is not secure, and a ws: URL is never fetched.
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
  if (parsed.protocol === 'http:' && !secureEndpoint(endpoint)) {
    return parsed.href;
  }
  throw redirectFailure('redirect URL scheme is not allowed for the endpoint');
}


// Whether an endpoint is secure: wss: or https:. Any other endpoint is not, and an insecure one may redirect to http:.
function secureEndpoint(endpoint: string): boolean {
  return /^(wss|https):/i.test(endpoint);
}


// The body as bytes, read chunk by chunk so that a body over the limit is cut off as it arrives. A declared length over
// the limit is refused before the body is read. A body with no stream is refused, since its size cannot be bounded while
// it arrives: only a null body is buffered, and it is empty. A missing body property is refused the same way.
async function readBounded(response: any, maxBytes: number): Promise<Uint8Array> {
  const declared: number | undefined = declaredLength(response);
  if (declared !== undefined && declared > maxBytes) {
    discardBody(response);
    throw redirectFailure('redirect body over the limit');
  }
  const source: ChunkSource | null = chunkSource(response.body);
  if (source === null) {
    if (response.body !== null) {
      discardBody(response);
      throw redirectFailure('redirect body cannot be bounded');
    }
    const whole: Uint8Array = new Uint8Array(await response.arrayBuffer());
    if (whole.length > maxBytes) {
      throw redirectFailure('redirect body over the limit');
    }
    return whole;
  }
  // One buffer: allocated at the declared length up to initialBodyBytes, so that a body which declares a large length and
  // stalls holds little memory, and grown geometrically as the bytes arrive. A growth step or the final copy holds the old
  // and the new buffer at once, so a fetch peaks at 2 x maxBytes, briefly; the pool's peak is maxRedirectsInflight x 2 x maxBytes.
  const initial: number = declared !== undefined && declared >= 0 ? Math.min(declared, initialBodyBytes) : 0;
  let out: Uint8Array = new Uint8Array(initial);
  let total: number = 0;
  try {
    for (;;) {
      const step: any = await source.next();
      if (step.done === true) {
        break;
      }
      const chunk: Uint8Array = step.value;
      total += chunk.length;
      if (total > maxBytes) {
        throw redirectFailure('redirect body over the limit');
      }
      if (total > out.length) {
        out = growBuffer(out, total, maxBytes);
      }
      out.set(chunk, total - chunk.length);
    }
  } catch (error: any) {
    // A body over the limit, or a read which failed mid-body: the stream is released now, not at garbage collection.
    source.cancel();
    throw error;
  }
  return out.length === total ? out : out.slice(0, total);
}


// A buffer of at least length bytes, holding the bytes of the old one: it doubles, and is capped at maxBytes.
function growBuffer(old: Uint8Array, length: number, maxBytes: number): Uint8Array {
  const grown: Uint8Array = new Uint8Array(Math.min(maxBytes, Math.max(length, old.length * 2, 65536)));
  grown.set(old);
  return grown;
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


// The length a response declares in its Content-Length header, undefined when it declares none.
function declaredLength(response: any): number | undefined {
  const value: string | null | undefined = response.headers?.get?.('content-length');
  if (value === null || value === undefined) {
    return undefined;
  }
  const length: number = Number(value);
  return Number.isFinite(length) ? length : undefined;
}


// Cancels the body of a response the pool does not read, so that its connection is released now and not at garbage
// collection. A runtime without a cancel on the body leaves nothing to release here.
function discardBody(response: any): void {
  try {
    const pending: any = response.body?.cancel?.();
    pending?.catch?.((): void => {});
  } catch (e) {
    // The body is already gone, or it cannot be cancelled: the runtime releases it.
  }
}


// The redirect failure of a fetch. The cause, when given, is the error the runtime or the body read raised: it is kept on
// the error (as cause), so that a failure can be told apart (DNS, TLS, the stream) without a message per cause.
function redirectFailure(message: string, cause?: any): any {
  const error: any = poolError(errorCodes.redirectFailed, message);
  if (cause !== undefined) {
    error.cause = cause;
  }
  return error;
}
