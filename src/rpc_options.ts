import { Options } from './types';

/** Default of maxPendingRpc: the calls tracked at once. */
export const defaultMaxPendingRpc: number = 32;

/**
 * Pattern of a client instance identifier: a random secret of at least 128 bits, in base64url
 * characters (22 characters carry 132 bits). The server drops any other value.
 * @internal
 */
const instancePattern: RegExp = /^[A-Za-z0-9_-]{22,64}$/;

/**
 * Whether a client instance identifier has the form the server accepts.
 * @internal
 */
function isValidInstance(instance: string): boolean {
  return instancePattern.test(instance);
}

/**
 * Settles the RPC options of a client config: an unset instance is empty, an unset maxPendingRpc takes
 * its default. Returns the message of the first option no call can work with, or null when they are valid.
 * @internal
 */
export function normaliseRpcOptions(config: Options): string | null {
  config.instance = config.instance ?? '';
  config.maxPendingRpc = config.maxPendingRpc ?? defaultMaxPendingRpc;
  if (config.instance !== '' && !isValidInstance(config.instance)) {
    return 'instance must be 22 to 64 characters of A-Z, a-z, 0-9, "_" or "-"';
  }
  if (!Number.isInteger(config.maxPendingRpc) || config.maxPendingRpc < 1) {
    return 'maxPendingRpc must be a positive integer';
  }
  return null;
}

/**
 * The config as the debug log shows it: the instance is a secret, so only whether one is set is shown.
 * @internal
 */
export function loggedConfig(config: Options): Options {
  return { ...config, instance: config.instance === '' ? '' : '<set>' };
}
