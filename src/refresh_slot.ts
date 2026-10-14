import { errorCodes } from './codes';
import type { CallResolveContext } from './centrifuge';
import { AuthContext } from './types';

/** Most refresh() calls in progress at once, the one in flight included. Beyond it refresh() rejects with errorCodes.limitExceeded. */
export const maxRefreshCalls: number = 32;

/** What the refresh slot needs from its client. */
export interface RefreshSlotHost {
  isConnected(): boolean;
  /** Resolves once the client is connected. Rejects when it is disconnected for good. */
  waitConnected(): Promise<void>;
  createError(code: number, message: string): any;
  /** Sends a command on the connection. Resolves with the reply context, rejects with the rejection context. */
  sendCommand(command: any): Promise<CallResolveContext>;
  hasGetToken(): boolean;
  getToken(): Promise<string>;
  token(): string;
  setToken(token: string): void;
  debug(message: string): void;
  /** Applies the reply of a refresh command to the identity of the connection. */
  applyReply(result: any): void;
  identity(): { user: string; authClass: string };
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
   * Runs a refresh() call: with the token given, or with the token of getToken. The token of the
   * connection is taken while the command is in flight, and the previous one is kept when the server
   * refuses it. Resolves with the identity the server confirms.
   */
  async run(token: string | undefined): Promise<AuthContext> {
    let newToken: string | undefined = token;
    if (newToken === undefined) {
      if (!this.host.hasGetToken()) {
        throw this.host.createError(errorCodes.badConfiguration, 'refresh needs a token: pass one to refresh() or set getToken');
      }
      newToken = await this.host.getToken();
    }
    if (!newToken) {
      throw this.host.createError(errorCodes.clientRefreshToken, 'refresh token is empty');
    }
    const previousToken: string = this.host.token();
    if (!this.host.isConnected()) {
      // The connect that comes next carries the token, and the refresh confirms it on that connection.
      this.host.setToken(newToken);
    }
    let ctx: CallResolveContext;
    try {
      ctx = await this.send(newToken);
    } catch (rejectCtx: any) {
      if (rejectCtx.next !== undefined) {
        rejectCtx.next();
      }
      // Refused or not sent: the reconnects keep the token the server accepted before.
      if (this.host.token() === newToken) {
        this.host.setToken(previousToken);
      }
      throw rejectCtx.error;
    }
    const result: any = ctx.reply.refresh;
    if (ctx.next !== undefined) {
      ctx.next();
    }
    this.host.setToken(newToken);
    this.host.debug('connection token refreshed');
    this.host.applyReply(result);
    const identity: { user: string; authClass: string } = this.host.identity();
    return { user: identity.user, authClass: identity.authClass, ttl: result.ttl ?? 0 };
  }


  /**
   * Sends a refresh command, one at a time with the other refresh commands (the explicit and the
   * timer-driven ones), once connected. Resolves with the reply context. Rejects with the
   * rejection context of the command, or with the error alone when the client is disconnected for
   * good before the command could go out.
   */
  async send(token: string): Promise<CallResolveContext> {
    let release: () => void;
    try {
      release = await this.claim();
    } catch (error) {
      throw { error: error };
    }
    try {
      return await this.host.sendCommand({ refresh: { token: token } });
    } finally {
      release();
    }
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
