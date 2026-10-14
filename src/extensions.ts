import { ServerErrorFields } from './types';
import { toOffset } from './utils';

/**
 * Connect flag bit 32 of the protocol flag bitmask: the client understands RPC options
 * (per-call headers) and the server-side fields that come with them. Bit 32 is computed
 * with arithmetic, not a bitwise shift, since bitwise operators truncate to 32 bits.
 * @internal
 */
export const rpcOptionsFlag: number = 2 ** 32;

/**
 * Whether the bit `flag` is set in the bitmask `flags`. Uses arithmetic so bits above 31
 * are handled exactly.
 * @internal
 */
export function hasFlag(flags: number, flag: number): boolean {
  return Math.floor(flags / flag) % 2 === 1;
}

/**
 * Offset between server and local time in milliseconds, from the server time of a reply
 * and the local time of its arrival. Null when the reply carries no server time: the proto
 * zero value (0) means unset.
 * @internal
 */
export function serverTimeOffsetFrom(time: any, now: number): number | null {
  const serverTime = toOffset(time);
  if (serverTime === 0) {
    return null;
  }
  return serverTime - now;
}

/**
 * Application error fields of a server error. Fields the server did not send stay absent,
 * the proto zero values (app code 0, empty type, empty metadata) mean "not set".
 * @internal
 */
export function serverErrorFields(error: any): ServerErrorFields {
  const fields: ServerErrorFields = {};
  if (error.app_code) {
    fields.appCode = error.app_code;
  }
  if (error.app_type) {
    fields.appType = error.app_type;
  }
  if (error.metadata && Object.keys(error.metadata).length > 0) {
    fields.metadata = error.metadata;
  }
  return fields;
}

/**
 * Pattern of a client instance identifier: a random secret of at least 128 bits, in base64url
 * characters (22 characters carry 132 bits). The server drops any other value.
 * @internal
 */
export const instancePattern: RegExp = /^[A-Za-z0-9_-]{22,64}$/;

/**
 * Whether a client instance identifier has the form the server accepts.
 * @internal
 */
export function isValidInstance(instance: string): boolean {
  return instancePattern.test(instance);
}

/**
 * Whether a server error of a refresh keeps the connection, on a timer-driven refresh as on an
 * explicit one. 103 refuses a transition that would change the identity, and the connection keeps its
 * identity. 109 means the token expired: the server ends the connection with the reconnect code
 * (3005) when its expiry passes, so the client does not drop the connection itself.
 * @internal
 */
export function refreshErrorKeepsConnection(code: number): boolean {
  return code === 103 || code === 109;
}

/**
 * Settles like the promise, unless the deadline passes first (the timeout error) or the signal
 * aborts first (the cancelled error). The timer and the abort listener are removed once it settled.
 * @internal
 */
export function raceCall<T>(
  promise: Promise<T>,
  deadline: number,
  signal: AbortSignal | undefined,
  timeoutError: () => any,
  cancelledError: () => any
): Promise<T> {
  return new Promise<T>((resolve: (value: T) => void, reject: (error: any) => void): void => {
    let timer: ReturnType<typeof setTimeout> | null = null;
    const finish = (): void => {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
      signal?.removeEventListener('abort', onAbort);
    };
    const onAbort = (): void => {
      finish();
      reject(cancelledError());
    };
    signal?.addEventListener('abort', onAbort, { once: true });
    timer = setTimeout((): void => {
      finish();
      reject(timeoutError());
    }, Math.max(0, deadline - Date.now()));
    promise.then((value: T): void => {
      finish();
      resolve(value);
    }, (error: any): void => {
      finish();
      reject(error);
    });
  });
}

/**
 * The message of the first RPC option no call can work with, or null when the options are valid.
 * @internal
 */
export function rpcConfigError(instance: string, maxPendingRpc: number): string | null {
  if (instance !== '' && !isValidInstance(instance)) {
    return 'instance must be 22 to 64 characters of A-Z, a-z, 0-9, "_" or "-"';
  }
  if (!Number.isInteger(maxPendingRpc) || maxPendingRpc < 1) {
    return 'maxPendingRpc must be a positive integer';
  }
  return null;
}

/**
 * Whether an RPC options object asks for headers. An empty headers object sends nothing,
 * so it counts as no headers.
 * @internal
 */
export function hasHeaders(headers: Record<string, string> | undefined): boolean {
  return headers !== undefined && Object.keys(headers).length > 0;
}
