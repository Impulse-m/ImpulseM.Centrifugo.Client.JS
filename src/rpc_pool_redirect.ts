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
  return readBounded(response, maxBytes);
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


// The body as bytes, read chunk by chunk so that a body over the limit is cut off as it arrives.
async function readBounded(response: any, maxBytes: number): Promise<Uint8Array> {
  const body: any = response.body;
  if (body === null || body === undefined || typeof body.getReader !== 'function') {
    const bytes: Uint8Array = new Uint8Array(await response.arrayBuffer());
    if (bytes.length > maxBytes) {
      throw redirectFailure('redirect body over the limit');
    }
    return bytes;
  }
  const reader: any = body.getReader();
  const chunks: Uint8Array[] = [];
  let total: number = 0;
  for (;;) {
    const step: any = await reader.read();
    if (step.done === true) {
      break;
    }
    total += step.value.length;
    if (total > maxBytes) {
      reader.cancel().catch((): void => {});
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


function redirectFailure(message: string): any {
  return poolError(errorCodes.redirectFailed, message);
}
