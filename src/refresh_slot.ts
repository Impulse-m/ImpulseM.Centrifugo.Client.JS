import { errorCodes } from './codes';
import { AuthContext } from './types';

/** Most refresh() calls in progress at once, the one in flight included. Beyond it refresh() rejects with errorCodes.limitExceeded. */
export const maxRefreshCalls: number = 32;

/** What the refresh slot needs from its client. */
export interface RefreshSlotHost {
  isConnected(): boolean;
  /** Resolves once the client is connected. Rejects when it is disconnected for good. */
  waitConnected(): Promise<void>;
  createError(code: number, message: string): any;
}


/**
 * Keeps the refresh commands of a client one at a time, timer-driven ones included, and the
 * refresh() calls in order: a call with another token is sent after the one in flight. A call
 * with the same token, or none, made meanwhile shares its promise.
 */
export class RefreshSlot {
  private host: RefreshSlotHost;
  // The refresh command in flight, of any kind: the next one waits for it to settle.
  private commandInFlight: Promise<void> | null = null;
  // The refresh() call in flight, and its token.
  private callInFlight: Promise<AuthContext> | null = null;
  private callToken?: string;
  // The refresh() calls in progress, the ones chained behind the call in flight included.
  private calls: number = 0;


  constructor(host: RefreshSlotHost) {
    this.host = host;
  }


  /**
   * Runs a refresh() call with the token given, or with none. The call runs at once when no other
   * is in progress, else after the one in flight. Rejects with errorCodes.limitExceeded when
   * maxRefreshCalls calls are in progress already.
   */
  call(token: string | undefined, run: (token: string | undefined) => Promise<AuthContext>): Promise<AuthContext> {
    const pending: Promise<AuthContext> | null = this.callInFlight;
    if (pending !== null && (token === undefined || token === this.callToken)) {
      return pending;
    }
    if (this.calls >= maxRefreshCalls) {
      return Promise.reject(this.host.createError(errorCodes.limitExceeded, 'too many pending refresh calls'));
    }
    this.calls++;
    const refresh: Promise<AuthContext> = pending === null
      ? run(token)
      : pending.then((): Promise<AuthContext> => run(token), (): Promise<AuthContext> => run(token));
    this.callInFlight = refresh;
    this.callToken = token;
    const settled = (): void => {
      this.calls--;
      if (this.callInFlight === refresh) {
        this.callInFlight = null;
        this.callToken = undefined;
      }
    };
    refresh.then(settled, settled);
    return refresh;
  }


  /**
   * Resolves once no refresh is in flight, the timer-driven ones included. A refresh that failed is
   * reported to its caller, not here.
   */
  async settled(): Promise<void> {
    const pending: Promise<unknown> | null = this.callInFlight ?? this.commandInFlight;
    if (pending === null) {
      return;
    }
    try {
      await pending;
    } catch {
      // Reported to the caller of the refresh.
    }
    await this.settled();
  }


  /**
   * Takes the slot for one refresh command: waits until the client is connected and no other
   * command is in flight. The returned function gives the slot back, once the command settled.
   * Rejects when the client is disconnected for good before the slot is free.
   */
  async claim(): Promise<() => void> {
    while (this.commandInFlight !== null || !this.host.isConnected()) {
      if (this.commandInFlight !== null) {
        await this.commandInFlight;
      } else {
        await this.host.waitConnected();
      }
    }
    let release: () => void = (): void => {};
    const slot: Promise<void> = new Promise<void>((resolve: () => void): void => {
      release = resolve;
    });
    this.commandInFlight = slot;
    return (): void => {
      if (this.commandInFlight === slot) {
        this.commandInFlight = null;
      }
      release();
    };
  }
}
