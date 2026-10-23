import { errorCodes } from './codes';
import { Options, RpcOptions } from './types';

/** Default of maxPendingRpc: the calls tracked at once. */
const defaultMaxPendingRpc: number = 32;

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
 * Settles the RPC options of a client config: an unset instance is empty, an unset maxPendingRpc takes its default,
 * so the config carries a positive integer afterwards. Throws a configuration error, before the client is used,
 * for an option no call can work with.
 * @internal
 */
export function normaliseRpcOptions(config: Options): void {
  config.instance = config.instance ?? '';
  config.maxPendingRpc = config.maxPendingRpc ?? defaultMaxPendingRpc;
  if (config.instance !== '' && !isValidInstance(config.instance)) {
    throw { code: errorCodes.badConfiguration, message: 'instance must be 22 to 64 characters of A-Z, a-z, 0-9, "_" or "-"' };
  }
  if (!Number.isInteger(config.maxPendingRpc) || config.maxPendingRpc < 1) {
    throw { code: errorCodes.badConfiguration, message: 'maxPendingRpc must be a positive integer' };
  }
}

/**
 * The deadline in milliseconds of an rpc call: its own timeout when the call sets one, else the client timeout. An own
 * timeout must be above 0 and no more than the client timeout; any other value rejects the call before it is sent.
 * @internal
 */
export function rpcCallTimeout(opts: RpcOptions | undefined, clientTimeout: number): number {
  const timeout: number | undefined = opts?.timeout;
  if (timeout === undefined) {
    return clientTimeout;
  }
  if (typeof timeout !== 'number' || !(timeout > 0) || timeout > clientTimeout) {
    throw { code: errorCodes.badConfiguration, message: 'rpc timeout must be above 0 and no more than the client timeout' };
  }
  return timeout;
}

/**
 * The config as the debug log shows it: the instance and the token are secrets, so only whether one is set is shown.
 * @internal
 */
export function loggedConfig(config: Options): Options {
  return {
    ...config,
    instance: config.instance === '' ? '' : '<set>',
    token: config.token ? '<set>' : config.token,
  };
}
