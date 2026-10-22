import type { Options } from './types';

/** The server code of a timer refresh refused as expired: the token the getter returned is refused. */
export const refreshRefusedExpired: number = 109;

/**
 * The token a retry of a refused timer refresh must not send again, or undefined when the retry may send any token.
 * Unless the option stopOnRefusedToken is set to false, and only for a refusal as expired: the retry does not send the
 * token the server refused again, so a token that expired is not resent forever.
 */
export function refusedAfter(options: Options, code: number, token: string): string | undefined {
  return options.stopOnRefusedToken !== false && code === refreshRefusedExpired ? token : undefined;
}

/**
 * Whether a token is the one the server refused as expired, in the attempt before. Such a token is not sent: the
 * connection is left to expire, and the next connect asks for a token.
 */
export function isRefused(token: string, refused: string | undefined): boolean {
  return refused !== undefined && token === refused;
}
