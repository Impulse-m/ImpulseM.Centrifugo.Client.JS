import type { Options } from './types';

/** The server code of a timer refresh refused as expired: the token the getter returned is refused. */
export const refreshRefusedExpired: number = 109;

/**
 * The token a retry of a refused timer refresh must not send again, or undefined when the retry may send any token.
 * Only with the option stopOnRefusedToken set, and only for a refusal as expired: by default the retry sends the
 * token the getter gives, as it always did.
 */
export function refusedAfter(options: Options, code: number, token: string): string | undefined {
  return options.stopOnRefusedToken === true && code === refreshRefusedExpired ? token : undefined;
}

/** Whether a token is the one the server refused as expired, in the attempt before. */
export function isRefused(token: string, refused: string | undefined): boolean {
  return refused !== undefined && token === refused;
}
