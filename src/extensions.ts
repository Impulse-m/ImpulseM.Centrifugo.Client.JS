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
 * Whether a server error of a timer-driven refresh is reported and retried with backoff and a fresh
 * token: a client error (code below 100), a temporary error, and 109 (the token the getter returned was
 * refused as expired). Any other error is not retried. An explicit refresh() is never retried.
 * @internal
 */
export function refreshRetries(code: number, temporary: boolean): boolean {
  return code < 100 || temporary || code === 109;
}

/**
 * Whether a server error of a timer-driven refresh keeps the connection, without a retry. 103 refuses
 * the identity change of the token the getter returned, and the same token would be refused again: the
 * connection keeps its identity, and the server ends it at its expiry.
 * @internal
 */
export function refreshKeepsConnection(code: number): boolean {
  return code === 103;
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
