import type { Options } from './types';

/** The server code of a timer refresh refused as expired: the token the getter returned is refused. */
export const refreshRefusedExpired: number = 109;

/**
 * The token a retry of a failed timer refresh must not send again. A refusal as expired (109) makes the token it was
 * sent with the refused one, unless the option stopOnRefusedToken is set to false. Any other error keeps the marker
 * of the attempts before, so a retry with another token answered by a temporary error does not send a refused token
 * again. A successful refresh clears the marker.
 */
export function refusedAfter(options: Options, code: number, token: string, previous: string | undefined): string | undefined {
  return options.stopOnRefusedToken !== false && code === refreshRefusedExpired ? token : previous;
}

/**
 * Whether a token is the one the server refused as expired, in the attempt before. Such a token is not sent: the
 * connection is left to expire, and the next connect asks for a token.
 */
export function isRefused(token: string, refused: string | undefined): boolean {
  return refused !== undefined && token === refused;
}
