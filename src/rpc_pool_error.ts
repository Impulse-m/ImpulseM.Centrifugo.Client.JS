/**
 * The error of the pool, of the same shape as the errors of Centrifuge.rpc(): a code and a message. It mirrors the inline
 * {code, message} literals of call_timer.ts and subscription.ts; the shape is one contract, so change them together.
 */
export function poolError(code: number, message: string): any {
  return { code: code, message: message };
}
