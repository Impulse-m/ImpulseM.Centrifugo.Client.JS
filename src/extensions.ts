import { ServerErrorFields } from './types';

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
 * Converts an int64 field to a number. Protobuf decoding yields a Long for int64 values,
 * JSON decoding yields a number. Returns undefined for an absent value.
 * @internal
 */
export function int64ToNumber(value: any): number | undefined {
  if (value === undefined || value === null) {
    return undefined;
  }
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value.toNumber === 'function') {
    return value.toNumber();
  }
  return undefined;
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
 * Whether an RPC options object asks for headers. An empty headers object sends nothing,
 * so it counts as no headers.
 * @internal
 */
export function hasHeaders(headers: Record<string, string> | undefined): boolean {
  return headers !== undefined && Object.keys(headers).length > 0;
}
