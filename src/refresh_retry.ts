import { randomInt } from './utils';

/** The shortest retry delay after a failed timer refresh, in milliseconds. */
const refreshRetryMinMillis: number = 10000;

/** The ceiling of the first retry delay after a failed timer refresh, in milliseconds. */
const refreshRetryFirstCeilingMillis: number = 20000;

// The failures in a row after which the ceiling no longer grows, so the power of two never overflows.
const refreshRetryMaxStep: number = 30;

/** The default of the tokenRetryCap option, in milliseconds. */
export const defaultTokenRetryCap: number = 300000;

/**
 * The tokenRetryCap in use: the option when it is a finite number of milliseconds from 0 up, else the default. Unset,
 * null, NaN, negative and non-number values all take the default, so a retry delay is never 0 or NaN.
 * @internal
 */
export function normaliseTokenRetryCap(tokenRetryCap: number | undefined): number {
  if (typeof tokenRetryCap !== 'number' || !Number.isFinite(tokenRetryCap) || tokenRetryCap < 0) {
    return defaultTokenRetryCap;
  }
  return tokenRetryCap;
}

/**
 * The ceiling of the retry delay after the given failures in a row: 20 seconds for the first, doubling with each failure
 * up to the tokenRetryCap option. A cap below 20 seconds keeps the ceiling at 20 seconds.
 * @internal
 */
export function refreshRetryCeiling(step: number, tokenRetryCap: number): number {
  const grown: number = refreshRetryFirstCeilingMillis * 2 ** Math.min(step, refreshRetryMaxStep);
  return Math.max(refreshRetryFirstCeilingMillis, Math.min(tokenRetryCap, grown));
}

/**
 * The delay in milliseconds of the retry after a failed timer refresh, the failures in a row being the step. Full jitter
 * over the growing ceiling, as the Java SDK: 10 seconds plus a uniform draw in [0, min(ceiling, 10 seconds * 2^step)],
 * never above the ceiling. The retry of the connection and the retry of a subscription share this formula.
 * @internal
 */
export function refreshRetryDelay(step: number, tokenRetryCap: number): number {
  const ceiling: number = refreshRetryCeiling(step, tokenRetryCap);
  const spread: number = Math.min(ceiling, refreshRetryMinMillis * 2 ** Math.min(step, refreshRetryMaxStep));
  return Math.min(ceiling, refreshRetryMinMillis + randomInt(0, spread));
}
