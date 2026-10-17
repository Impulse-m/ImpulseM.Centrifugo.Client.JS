import { errorCodes } from './codes';
import type { CallResolveContext } from './rpc_tracker';
import type { ConnectionIdentity } from './identity';
import { AuthContext } from './types';

/** Most refresh() calls in progress at once, the one in flight included. Beyond it refresh() rejects with errorCodes.limitExceeded. */
export const maxRefreshCalls: number = 32;

/**
 * Whether a server error of a timer-driven refresh is reported and retried with backoff and a fresh
 * token: a client error (code below 100), a temporary error, and 109 (the token the getter returned was
 * refused as expired). Any other error is not retried. An explicit refresh() is never retried.
 */
export function refreshRetries(code: number, temporary: boolean): boolean {
  return code < 100 || temporary || code === 109;
}

/**
 * Whether a server error of a timer-driven refresh keeps the connection, without a retry. 103 refuses
 * the identity change of the token the getter returned, and the same token would be refused again: the
 * connection keeps its identity, and the server ends it at its expiry.
 */
export function refreshKeepsConnection(code: number): boolean {
  return code === 103;
}

/** What the refresh slot needs from its client. */
export interface RefreshSlotHost {
  isConnected(): boolean;
  /** Resolves once the client is connected. Rejects after the timeout in milliseconds, when the signal aborts, or when
   * it is disconnected for good while the wait runs. With a null timeout it waits without a limit: when the client is
   * already disconnected for good, until the next connect. */
  waitConnected(timeout: number | null, signal?: AbortSignal): Promise<void>;
  createError(code: number, message: string): any;
  /** Sends a command on the connection. Resolves with the reply context, rejects with the rejection context. */
  sendCommand(command: any): Promise<CallResolveContext>;
  hasGetToken(): boolean;
  getToken(): Promise<string>;
  /** The generation of the connection token: moves on with each refresh(token) call. */
  tokenGeneration(): number;
  /** Whether a token fetched by the token exchange, read at the generation given, was superseded by a refresh(token) call. */
  tokenSuperseded(generation: number): boolean;
  token(): string;
  setToken(token: string): void;
  debug(message: string, ...args: any[]): void;
  /** Applies the reply of a refresh command to the identity of the connection. */
  applyReply(result: any): void;
  /** Replaces the armed refresh timer with a retry timer, to retry the refresh of a connection whose token expires. */
  armRefreshRetry(): void;
  identity(): ConnectionIdentity;
  /** Reports an error raised while a refresh was taken, e.g. by an 'auth' handler. */
  dispatchFailed(err: any): void;
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
  // Moves on with each refresh() call started and each token accepted, see sendTimer.
  private generationCount: number = 0;
  // Moves on with each refresh() call that brings a token, see currentTokenGeneration.
  private tokenGenerationCount: number = 0;
  // The generation of the last token accepted.
  private acceptedGeneration: number = 0;


  constructor(host: RefreshSlotHost) {
    this.host = host;
  }


  /** The generation of the refreshes: it moves on with each refresh() call started and each token accepted. */
  currentGeneration(): number {
    return this.generationCount;
  }


  /**
   * The generation of the refresh() calls that bring a token: it moves on with those only, so a refresh() that fetches
   * its token does not supersede a token exchange in progress.
   */
  currentTokenGeneration(): number {
    return this.tokenGenerationCount;
  }


  /**
   * Runs a refresh() call with the token given, or with none. The call runs at once when no other
   * is in progress, else after the one in flight. Rejects with errorCodes.limitExceeded when
   * maxRefreshCalls calls are in progress already.
   */
  call(token: string | undefined): Promise<AuthContext> {
    const pending: Promise<AuthContext> | null = this.callInFlight;
    if (pending !== null && (token === undefined || token === this.callToken)) {
      return pending;
    }
    if (this.calls >= maxRefreshCalls) {
      return Promise.reject(this.host.createError(errorCodes.limitExceeded, 'too many pending refresh calls'));
    }
    this.calls++;
    this.generationCount++;
    if (token !== undefined) {
      this.tokenGenerationCount++;
    }
    const refresh: Promise<AuthContext> = pending === null
      ? this.run(token)
      : pending.then((): Promise<AuthContext> => this.run(token), (): Promise<AuthContext> => this.run(token));
    this.callInFlight = refresh;
    this.callToken = token;
    const settled: () => void = (): void => {
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
      const generation: number = this.host.tokenGeneration();
      newToken = await this.host.getToken();
      // A refresh(token) call made while the token was fetched by the token exchange supersedes it: the token fetched is
      // not sent, and the call's own token follows in turn. The caller gets the identity the connection has meanwhile.
      if (this.host.tokenSuperseded(generation)) {
        return this.host.identity().context(0);
      }
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
      // A refusal of the server carries next(); a connection lost meanwhile does not, and keeps the token:
      // the next connect carries it, and the server answers for it.
      const refused: boolean = rejectCtx.next !== undefined;
      if (refused) {
        rejectCtx.next();
      }
      // Refused, or never sent (the client was disconnected for good): the reconnects keep the token the server accepted before.
      if ((refused || rejectCtx.unsent === true) && this.host.token() === newToken) {
        this.host.setToken(previousToken);
      }
      throw rejectCtx.error;
    }
    const result: any = ctx.reply.refresh;
    if (ctx.next !== undefined) {
      ctx.next();
    }
    try {
      this.accepted(newToken, result);
    } catch (err) {
      // The token is accepted: the refresh succeeded, and the error of a handler is reported as any other.
      this.host.dispatchFailed(err);
    }
    return this.host.identity().context(result.ttl ?? 0);
  }


  /**
   * Takes a token the server accepted: it is the token of the connection from now on, and the identity of
   * the refresh reply applies. A refused token is never taken, so the reconnects keep the one accepted before.
   */
  accepted(token: string, result: any): void {
    this.generationCount++;
    this.acceptedGeneration = this.generationCount;
    this.host.setToken(token);
    this.host.debug('connection token refreshed');
    this.host.applyReply(result);
  }


  /**
   * Sends a refresh command, one at a time with the other refresh commands (the explicit and the
   * timer-driven ones), once connected. Resolves with the reply context. Rejects with the
   * rejection context of the command, or with the error alone when the client is disconnected for
   * good before the command could go out.
   */
  async send(token: string): Promise<CallResolveContext> {
    const release: () => void = await this.claimCommand();
    try {
      // The connection may have gone while the slot was claimed: the command is not written then.
      if (!this.host.isConnected()) {
        throw { error: this.host.createError(errorCodes.clientDisconnected, 'client disconnected'), unsent: true };
      }
      return await this.host.sendCommand({ refresh: { token: token } });
    } finally {
      release();
    }
  }


  /**
   * Sends the token of a timer refresh, read at the given generation, once the command slot is free. It is
   * not sent when a refresh() call started or a token was accepted since: then it is superseded, and
   * resolves with null, and the token it fetched is not fetched again. A refresh() call made while the token
   * was fetched is never overwritten by it. When no token was accepted since, the call may have been refused,
   * and the refresh timer is armed to retry: a token accepted since has armed the timer itself.
   */
  async sendTimer(token: string, generation: number): Promise<CallResolveContext | null> {
    const release: () => void = await this.claimCommand();
    try {
      // A connection lost while the slot was claimed is the same case: the command is not written, the retry is armed.
      if (this.generationCount !== generation || !this.host.isConnected()) {
        if (this.acceptedGeneration <= generation) {
          this.host.armRefreshRetry();
        }
        return null;
      }
      return await this.host.sendCommand({ refresh: { token: token } });
    } finally {
      release();
    }
  }


  /**
   * Takes the token of a timer refresh fetched while the client is disconnected: it is the token of the next
   * connect. Not taken when a refresh() call started or a token was accepted since the generation given.
   */
  takeTimerToken(token: string, generation: number): void {
    if (this.generationCount !== generation) {
      return;
    }
    this.host.setToken(token);
    this.host.debug('connection token refreshed');
  }


  /**
   * Resolves once the client is connected and no refresh is in flight, so an rpc call can go out. Rejects with
   * a timeout at the deadline (local time in milliseconds), or when the client is disconnected for good.
   */
  async ready(deadline: number, signal?: AbortSignal): Promise<void> {
    do {
      await this.host.waitConnected(Math.max(0, deadline - Date.now()), signal);
      await this.settled();
    } while (!this.host.isConnected());
  }


  // Takes the slot of a refresh command. A client disconnected for good first rejects with the error alone,
  // and the command was never sent.
  private async claimCommand(): Promise<() => void> {
    try {
      return await this.claim();
    } catch (error) {
      throw { error: error, unsent: true };
    }
  }


  /**
   * Resolves once no refresh is in flight, the timer-driven ones included. A refresh that failed is
   * reported to its caller, not here.
   */
  private async settled(): Promise<void> {
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
   * Rejects when the client is disconnected for good while it waits for the slot. A claim made after such a disconnect
   * waits for the next connect, without a limit, as a refresh(token) does while disconnected.
   */
  private async claim(): Promise<() => void> {
    while (this.commandInFlight !== null || !this.host.isConnected()) {
      if (this.commandInFlight !== null) {
        await this.commandInFlight;
      } else {
        await this.host.waitConnected(null);
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
