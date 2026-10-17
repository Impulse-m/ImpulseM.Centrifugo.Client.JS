import { raceCall } from './call_timer';
import { connectingCodes, errorCodes } from './codes';
import type { ClientHost } from './client_host';
import type { CallRejectContext, CallResolveContext } from './rpc_tracker';
import { refreshRetries } from './refresh_slot';
import type { Error as ServerError, Options, RpcResult, ServerErrorFields } from './types';
import { UnauthorizedError } from './unauthorized_error';
import { backoff, ttlMilliseconds } from './utils';
import { writeCommands } from './write_commands';

// The grace of a refusal run when neither the issuer nor the option sets one, in seconds.
const defaultGrace: number = 30;
// The longest grace a refusal run honours, in seconds: a misconfigured issuer cannot make a dead client poll for long.
const maxGrace: number = 300;
// The message of a refusal inside the grace horizon: it names the cause, and never a token.
const refusedMessage: string = 'refresh token refused, retrying within the grace';

/** What the `exchange` callback of tokenExchange gets. */
export interface TokenExchangeContext {
  /** The refresh token `load` returned for this exchange. */
  refreshToken: string;
  /** Sends a plain rpc call over the connection the exchange runs on: the one being set up, or the connected one.
   * It does not wait for the connected state, carries no message id and is not retransmitted. Rejects with
   * `connectionClosed` when that connection is lost, with `transportWriteError` when the write fails, and with
   * `TokenExchangeRefusedError` when the server error's `appType` is in `refusalTypes`. `data` is what `rpc` takes
   * for the codec in use. */
  rpc: (method: string, data: any) => Promise<RpcResult>;
}


/** What the `exchange` callback of tokenExchange resolves with. */
export interface TokenExchangeResult {
  /** The access token: the connection token from now on. */
  token: string;
  /** The rotated refresh token, when the issuer rotated it. Absent or equal to the one used: nothing is stored. An
   * `exchange` that persisted the rotation itself, under its own lock (see the README), returns none. */
  refreshToken?: string;
  /** How long, in seconds, the issuer keeps accepting the refresh token this exchange consumed. Kept in memory for the
   * next refusal run. Absent: the option's `grace`, else 30. Negative or not finite: ignored, the grace in use stays.
   * Above 300: 300. */
  grace?: number;
}


/** Persistence and exchange of the refresh token, see the tokenExchange option. */
export interface TokenExchange {
  /** The refresh token the application persisted, or null/undefined/'' when there is none. Called before every exchange.
   * Throw `UnauthorizedError` when the refresh token is gone for good: the session is dead, at bootstrap and at renewal. */
  load: () => string | null | undefined | Promise<string | null | undefined>;
  /** Persists a rotated refresh token. Awaited before the access token issued with it is used. A rejection is reported
   * as a `tokenStore` error, and the exchange goes on. */
  store: (refreshToken: string) => void | Promise<void>;
  /** Exchanges the refresh token for an access token. Throw `UnauthorizedError` (or rethrow the
   * `TokenExchangeRefusedError` of `ctx.rpc`) when the issuer refused the refresh token: the SDK presents the token the
   * storage holds again, with backoff, until the grace horizon has passed, and only then ends the session. Any other
   * rejection is reported and retried with backoff. */
  exchange: (ctx: TokenExchangeContext) => Promise<TokenExchangeResult>;
  /** Server error names (`appType`) that mean the issuer refused the refresh token. A `ctx.rpc` rejected with one of them
   * rejects with `TokenExchangeRefusedError`. Exact string match; codes are never consulted. Default: none. */
  refusalTypes?: string[];
  /** The grace, in seconds, used until the issuer publishes one in a result. Default 30. 0: a refusal of an unchanged
   * refresh token is terminal at once. Clamped to 300. */
  grace?: number;
}


/** The rejection of `ctx.rpc` for a server error named in `refusalTypes`. An `UnauthorizedError`. */
export class TokenExchangeRefusedError extends UnauthorizedError {
  /** The server error as `rpc()` would reject with it: code, message, temporary, appCode, appType, metadata. */
  error: ServerError & ServerErrorFields;

  constructor(error: ServerError & ServerErrorFields) {
    super('the issuer refused the refresh token');
    this.error = error;
  }
}


/**
 * A refusal of the issuer that the refusal run decides on, thrown by the exchange of a refresh token. The session is over
 * (`terminal`) when the storage holds no refresh token any more, or when the grace horizon has passed. Otherwise the
 * attempt is retried.
 */
class Refusal extends UnauthorizedError {
  terminal: boolean;

  constructor(terminal: boolean) {
    super('refresh token refused');
    this.terminal = terminal;
  }
}


/** The route the commands of an exchange are written with: the session and node of its connection. */
interface Route {
  session: string;
  node: string;
}


/**
 * One exchange of the flow: the connection it runs on, and whether that connection is still the one it was started for.
 * An exchange whose connection ended, or that ran past the client timeout, is abandoned: its access token is not used.
 * A rotated refresh token of an abandoned exchange, or of one a refresh(token) call superseded, is stored only while
 * the application still holds the token it exchanged.
 */
interface Attempt {
  route: () => Route;
  current: () => boolean;
  expire: () => void;
  expired: () => boolean;
  superseded: () => boolean;
}


/** The expiry the SDK knows of its access token: from the ttl of the reply that accepted it. */
interface ExpiryRecord {
  token: string;
  expiresAt: number;
}


/** The token fetch of a renewal in progress, and the transport it runs on. */
interface Fetch {
  transportId: number;
  token: Promise<string>;
}


/**
 * A bootstrap: the exchange that runs on a connection opened without a token, between its connect reply and its
 * connected state. The subscribe pushes held meanwhile (their payload) and the unsubscribed channels (null), by channel.
 */
interface Bootstrap {
  transportId: number;
  held: Map<string, any>;
  timer: ReturnType<typeof setTimeout> | null;
}


/**
 * One exchange on the queue of the client: the outcome its caller waits for, and the settling of the exchange itself,
 * after which the next exchange may run.
 */
interface Turn<T> {
  outcome: Promise<T>;
  done: Promise<void>;
}


/**
 * The token exchange of the tokenExchange option: the client obtains its access token over the connection from a
 * refresh token the application persists, and renews it there. Without the option the client holds no flow, and its
 * hooks check for that.
 */
export class TokenExchangeFlow {
  private host: ClientHost;
  private options: TokenExchange;
  private refusalTypes: Set<string>;
  // The grace of the option, or the default: the grace of a refusal run when the latest accepted result publishes none.
  private optionGrace: number;
  // The grace in use: the one the latest accepted result published, else optionGrace. Seconds.
  private grace: number;
  // The monotonic time (performance.now, milliseconds) of the first refusal of the open refusal run; null without one.
  private refusedSince: number | null = null;
  // The bootstrap retries of the open refusal run: the backoff of their delays.
  private refusals: number = 0;
  private expiry: ExpiryRecord | null = null;
  private bootstrap: Bootstrap | null = null;
  // The exchanges started so far, settled or not: the next one waits for them (see serialised).
  private exchanges: Promise<void> = Promise.resolve();
  // The token fetch of a renewal in progress: a refresh that asks for a token meanwhile joins it.
  private fetching: Fetch | null = null;
  // The bootstrap steps that failed since the last bootstrap completed: the backoff of their retries.
  private failures: number = 0;
  // Set while a connect reply is resumed: that reply is not intercepted again.
  private resuming: boolean = false;


  constructor(host: ClientHost, options: TokenExchange) {
    this.host = host;
    this.options = options;
    this.refusalTypes = new Set<string>(options.refusalTypes ?? []);
    this.optionGrace = options.grace === undefined ? defaultGrace : Math.min(options.grace, maxGrace);
    this.grace = this.optionGrace;
  }


  /** Called at the start of every connect attempt: an access token that is expired, or refused, is cleared. */
  beforeConnect(): void {
    const token: string = this.host.token();
    const expired: boolean = token !== '' && this.expiry !== null && this.expiry.token === token
      && Date.now() >= this.expiry.expiresAt;
    if (this.host.refreshRequired() || expired) {
      this.host.setToken('');
      this.host.clearRefreshRequired();
    }
  }


  /**
   * Called with a connect reply before the connected state. Returns true when it took over the connect and will
   * resume it with the merged reply later, false when the connect goes on at once (a reply that already carries a
   * user, a resumed reply). An anonymous reply bootstraps whether the connect sent a token or not: a token it sent
   * was refused, so none is kept.
   */
  intercept(result: any, resume: (result: any) => void): boolean {
    if (this.resuming || !!result.user) {
      return false;
    }
    this.host.setToken('');
    const bootstrap: Bootstrap = {
      transportId: this.host.transportId(),
      held: new Map<string, any>(),
      timer: null,
    };
    this.bootstrap = bootstrap;
    this.start(bootstrap, result, resume);
    return true;
  }


  /** Called for every push while connecting: true when a bootstrap held it (a subscribe or an unsubscribe). */
  holdPush(data: any): boolean {
    const bootstrap: Bootstrap | null = this.bootstrap;
    if (bootstrap === null) {
      return false;
    }
    if (data.subscribe) {
      bootstrap.held.set(data.channel, data.subscribe);
      return true;
    }
    if (data.unsubscribe) {
      bootstrap.held.set(data.channel, null);
      return true;
    }
    return false;
  }


  /**
   * The token of a renewal, and of refresh() without a token: exchanged over the connected socket, so a refresh()
   * made while disconnected waits for the connection first. Rejects with UnauthorizedError when the session is over.
   */
  async fetchToken(): Promise<string> {
    if (!this.host.isConnected()) {
      await this.host.waitConnected(null);
    }
    const transportId: number = this.host.transportId();
    const running: Fetch | null = this.fetching;
    if (running !== null && running.transportId === transportId) {
      return running.token;
    }
    const token: Promise<string> = this.serialised((): Turn<string> => {
      const attempt: Attempt = this.attempt(
        (): Route => this.host.route(),
        (): boolean => this.host.isConnected() && this.host.transportId() === transportId,
      );
      return this.bounded(this.renewalToken(attempt), attempt);
    });
    const fetch: Fetch = { transportId: transportId, token: token };
    this.fetching = fetch;
    // Cleared as soon as the fetch settles, before the continuations of its callers run: a refresh() in that gap must
    // not join a fetch that is already over.
    token.then((): void => this.clearFetch(fetch), (): void => this.clearFetch(fetch));
    return token;
  }


  /**
   * Called with the ttl of a reply that set the access token: records its expiry, and returns the delay of the refresh
   * timer in milliseconds, a random moment between 0.7 and 0.9 of the ttl.
   */
  expiryDelay(ttl: number): number {
    this.expiry = { token: this.host.token(), expiresAt: Date.now() + ttl * 1000 };
    return ttlMilliseconds(ttl * (0.7 + 0.2 * Math.random()));
  }


  /**
   * Ends a bootstrap in progress, and its retry timer: called when the connection ends. A client-initiated end (the
   * application called disconnect) also closes the refusal run; a transport loss keeps it, so a client that reconnects
   * repeatedly still ends its session at the horizon.
   */
  reset(clientInitiated: boolean): void {
    const bootstrap: Bootstrap | null = this.bootstrap;
    if (bootstrap !== null) {
      this.bootstrap = null;
      if (bootstrap.timer !== null) {
        clearTimeout(bootstrap.timer);
        bootstrap.timer = null;
      }
    }
    if (clientInitiated) {
      this.closeRun();
    }
  }


  // Forgets a settled fetch, unless a newer one replaced it.
  private clearFetch(fetch: Fetch): void {
    if (this.fetching === fetch) {
      this.fetching = null;
    }
  }


  // The token of a renewal on the connected transport it was requested for. A refresh token the application no longer
  // holds ends the session; a refusal inside the grace horizon fails the fetch, which the refresh timer retries.
  private async renewalToken(attempt: Attempt): Promise<string> {
    let token: string | null;
    let refused: boolean = false;
    try {
      token = await this.loadAndExchange(attempt);
    } catch (err: any) {
      if (!(err instanceof UnauthorizedError) || !attempt.current()) {
        throw err;
      }
      refused = err instanceof Refusal && !err.terminal;
      token = null;
    }
    if (refused) {
      throw new Error(refusedMessage);
    }
    if (token === null) {
      this.closeRun();
      throw new UnauthorizedError('');
    }
    return token;
  }


  // Runs an exchange once the exchanges started before it are done: no two of them load or rotate the refresh token at
  // once, so an exchange always reads the token the one before it stored. The caller gets the outcome of its turn.
  private serialised<T>(run: () => Turn<T>): Promise<T> {
    const turn: Promise<Turn<T>> = this.exchanges.then(run);
    this.exchanges = turn.then((next: Turn<T>): Promise<void> => next.done, (): void => {});
    return turn.then((next: Turn<T>): Promise<T> => next.outcome);
  }


  // The turn of an exchange. Its outcome rejects with a timeout error once the client timeout has passed, and the attempt
  // is expired then, so its access token is not used. The turn is done when the exchange itself settles, so the next
  // exchange loads the refresh token only after a late rotation of this one is stored. A second client timeout bounds
  // that wait, so a callback which never settles cannot hold the queue.
  private bounded<T>(work: Promise<T>, attempt: Attempt): Turn<T> {
    const timeout: number = this.host.timeout();
    const start: number = Date.now();
    const outcome: Promise<T> = raceCall(work, start + timeout, undefined, (): any => {
      attempt.expire();
      return this.host.createError(errorCodes.timeout, 'timeout');
    }, (): any => undefined);
    const done: Promise<unknown> = raceCall(work, start + 2 * timeout, undefined, (): any => undefined, (): any => undefined);
    return {
      outcome: outcome,
      done: done.then((): void => {}, (): void => {}),
    };
  }


  // Whether a continuation of a bootstrap still belongs to the connection attempt it runs for.
  private isCurrent(bootstrap: Bootstrap): boolean {
    return this.bootstrap === bootstrap
      && this.host.transportId() === bootstrap.transportId
      && this.host.isConnecting();
  }


  // Loads the refresh token and exchanges it on the connection of the attempt. Resolves with null when the application
  // holds no refresh token.
  private async loadAndExchange(attempt: Attempt): Promise<string | null> {
    const refreshToken: string | null | undefined = await this.options.load();
    if (!attempt.current()) {
      throw this.closedError();
    }
    if (!refreshToken) {
      return null;
    }
    return this.exchangeRefresh(refreshToken, attempt, false);
  }


  // An attempt that is current while alive() holds and it has not been expired by its timeout. It is superseded once a
  // refresh(token) call starts, which moves the token generation of the refresh slot on.
  private attempt(route: () => Route, alive: () => boolean): Attempt {
    let timedOut: boolean = false;
    const generation: number = this.host.tokenGeneration();
    return {
      route: route,
      current: (): boolean => !timedOut && alive(),
      expire: (): void => {
        timedOut = true;
      },
      expired: (): boolean => timedOut,
      superseded: (): boolean => this.host.tokenGeneration() !== generation,
    };
  }


  // Ends a bootstrap whose exchange stalled, or whose refresh outcome is unknown: the error is reported, then the
  // connection is closed after a backoff, so the reconnect bootstraps again. A stalled exchange is not retried in place.
  // The backoff keeps a stalled bootstrap from reconnecting at once: the connect reply resets the transport backoff.
  private abandon(bootstrap: Bootstrap, type: string, error: any): void {
    this.reported(bootstrap, type, error, (): void => {
      this.backedOff(bootstrap, (): void => {
        if (this.isCurrent(bootstrap)) {
          this.host.disconnect(connectingCodes.transportClosed, 'exchange abandoned', true);
        }
      });
    });
  }


  // Reports an error of a bootstrap that is still current, and runs next once the report is handled, if the bootstrap
  // is still current then: a handler that disconnected the client ends it instead.
  private reported(bootstrap: Bootstrap, type: string, error: any, next: () => void): void {
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    this.host.emitError({ type: type, error: error }, (): void => {
      if (this.isCurrent(bootstrap)) {
        next();
      }
    });
  }


  // Runs next after a backoff that grows with the failed bootstrap steps since the last bootstrap completed, this one
  // included, so it escalates across reconnects too. The timer is cleared by reset().
  private backedOff(bootstrap: Bootstrap, next: () => void): void {
    this.after(bootstrap, backoff(this.failures++, 5000, 60000), next);
  }


  // Runs next after the delay, on the retry timer of the bootstrap, which reset() clears.
  private after(bootstrap: Bootstrap, delay: number, next: () => void): void {
    bootstrap.timer = setTimeout((): void => {
      bootstrap.timer = null;
      next();
    }, delay);
  }


  // Runs a step of a bootstrap. An exception an application handler throws in it is dispatched as a failure of the client.
  private start(bootstrap: Bootstrap, result: any, resume: (result: any) => void): void {
    this.connectStep(bootstrap, result, resume).catch((err: any): void => {
      this.host.dispatchFailed(err);
    });
  }


  // One step of a bootstrap: loads the refresh token, exchanges it, and refreshes the anonymous connection with the
  // access token. An empty refresh token leaves the connection anonymous, unless a refusal run is open: then the
  // application removed the token meanwhile, and the session is over.
  private async connectStep(bootstrap: Bootstrap, result: any, resume: (result: any) => void): Promise<void> {
    bootstrap.timer = null;
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    const attempt: Attempt = this.attempt(
      (): Route => ({ session: result.session, node: result.node }),
      (): boolean => this.isCurrent(bootstrap),
    );
    let token: string | null;
    try {
      token = await this.serialised((): Turn<string | null> => this.bounded(this.loadAndExchange(attempt), attempt));
    } catch (err: any) {
      if (!this.isCurrent(bootstrap)) {
        return;
      }
      // The attempt ran past the client timeout: a stalled exchange, whatever error its rejection carries.
      if (attempt.expired()) {
        this.abandon(bootstrap, 'connectToken', this.host.createError(errorCodes.clientConnectToken, 'timeout'));
        return;
      }
      if (err instanceof Refusal && !err.terminal) {
        this.refused(bootstrap, result, resume);
        return;
      }
      if (err instanceof UnauthorizedError) {
        this.endSession();
        return;
      }
      this.retry(bootstrap, result, resume, 'connectToken', this.host.createError(errorCodes.clientConnectToken, err?.toString() ?? ''));
      return;
    }
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    if (token === null) {
      if (this.refusedSince !== null) {
        this.endSession();
        return;
      }
      this.finish(bootstrap, result, resume, null);
      return;
    }
    this.refreshConnection(bootstrap, result, resume, token);
  }


  // Reports a refusal inside the grace horizon, and runs the bootstrap again on the same connection after a backoff that
  // ends no later than the horizon. The horizon is checked again once the report is handled: a session that ends meanwhile
  // is ended, not retried.
  private refused(bootstrap: Bootstrap, result: any, resume: (result: any) => void): void {
    this.reported(bootstrap, 'connectToken', this.host.createError(errorCodes.clientConnectToken, refusedMessage), (): void => {
      if (this.horizonPassed()) {
        this.endSession();
        return;
      }
      this.after(bootstrap, Math.min(backoff(this.refusals++, 1000, 8000), this.horizon() - performance.now()), (): void => {
        this.start(bootstrap, result, resume);
      });
    });
  }


  // Reports a failed step of a bootstrap, and runs the step again on the same connection after a backoff.
  private retry(bootstrap: Bootstrap, result: any, resume: (result: any) => void, type: string, error: any): void {
    this.reported(bootstrap, type, error, (): void => {
      this.backedOff(bootstrap, (): void => {
        this.start(bootstrap, result, resume);
      });
    });
  }


  // Sets the access token and refreshes the anonymous connection with it, on the connection of the attempt.
  private refreshConnection(bootstrap: Bootstrap, result: any, resume: (result: any) => void, token: string): void {
    const cmd: any = { refresh: { token: token } };
    this.host.setToken(token);
    this.sendOnRoute(cmd, result.session, result.node).then((ctx: CallResolveContext): void => {
      try {
        if (!this.isCurrent(bootstrap)) {
          return;
        }
        if (ctx.reply.refresh) {
          this.finish(bootstrap, result, resume, ctx.reply.refresh);
        } else {
          this.refreshFailed(bootstrap, result, resume, token, this.host.createError(errorCodes.clientConnectToken, 'refresh reply without identity'));
        }
      } finally {
        ctx.next?.();
      }
    }, (rejectCtx: CallRejectContext): void => {
      try {
        this.refreshFailed(bootstrap, result, resume, token, rejectCtx.error);
      } finally {
        rejectCtx.next?.();
      }
    }).catch((err: any): void => {
      this.host.dispatchFailed(err);
    });
  }


  // A refresh of a bootstrap that the server refused, or that gave no identity. The access token is dropped, unless
  // the connection has moved on. A refresh that timed out or failed to write has an unknown outcome: the connection is
  // closed and the reconnect bootstraps again. A temporary refusal is retried with a new exchange; any other ends the
  // connection.
  private refreshFailed(
    bootstrap: Bootstrap,
    result: any,
    resume: (result: any) => void,
    token: string,
    err: any,
  ): void {
    if (this.host.token() === token) {
      this.host.setToken('');
    }
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    if (err.code === errorCodes.timeout || err.code === errorCodes.transportWriteError) {
      this.abandon(bootstrap, 'connect', err);
    } else if (refreshRetries(err.code, err.temporary === true)) {
      this.retry(bootstrap, result, resume, 'connect', err);
    } else {
      this.host.disconnect(err.code, err.message, false);
    }
  }


  // Ends the bootstrap and resumes the connect reply: merged with the identity of the refresh reply (when there is one)
  // and with the subscriptions the bootstrap held.
  private finish(bootstrap: Bootstrap, result: any, resume: (result: any) => void, refresh: any | null): void {
    this.bootstrap = null;
    this.failures = 0;
    let merged: any = refresh === null ? result : {
      ...result,
      expires: refresh.expires,
      ttl: refresh.ttl,
      user: refresh.user,
      auth_class: refresh.auth_class,
    };
    if (bootstrap.held.size > 0) {
      const subs: Record<string, any> = { ...(result.subs ?? {}) };
      bootstrap.held.forEach((sub: any, channel: string): void => {
        if (sub === null) {
          delete subs[channel];
        } else {
          subs[channel] = { ...sub, was_recovering: false, recovered: false };
        }
      });
      merged = { ...merged, subs: subs };
    }
    this.resuming = true;
    try {
      resume(merged);
    } catch (err: any) {
      this.host.dispatchFailed(err);
    } finally {
      this.resuming = false;
    }
  }


  // Exchanges a refresh token. The first refusal opens the refusal run, which starts the clock of the grace horizon. The
  // storage is read again at once: a different token is presented in the same attempt, once (another client of the
  // installation rotated it meanwhile), and an empty storage ends the session. Any other refusal is thrown as a Refusal,
  // which tells the caller whether the session is over: the horizon has passed, or the refusal came after the retry.
  private async exchangeRefresh(refreshToken: string, attempt: Attempt, retried: boolean): Promise<string> {
    try {
      return await this.exchangeWith(refreshToken, attempt);
    } catch (err: any) {
      if (!(err instanceof UnauthorizedError)) {
        throw err;
      }
      // A refusal counts even when its attempt has already ended: the horizon is measured from the first refusal.
      this.openRun();
      if (!attempt.current()) {
        throw err;
      }
      if (!retried) {
        const current: string | null | undefined = await this.options.load();
        if (!attempt.current()) {
          throw this.closedError();
        }
        if (!current) {
          throw new Refusal(true);
        }
        if (current !== refreshToken) {
          return this.exchangeRefresh(current, attempt, true);
        }
      }
      throw new Refusal(this.horizonPassed());
    }
  }


  // One exchange: the app's exchange callback, then the store of the rotated refresh token, awaited before the access
  // token is used. Resolves with the access token. The access token of an attempt that timed out, or was abandoned, is
  // not used, but its rotated refresh token is stored (see storeRotated): the issuer has already consumed the pair.
  // An accepted exchange closes the refusal run, whatever attempt it ran for.
  private async exchangeWith(refreshToken: string, attempt: Attempt): Promise<string> {
    const result: TokenExchangeResult = await this.options.exchange({
      refreshToken: refreshToken,
      rpc: (method: string, data: any): Promise<RpcResult> => this.rpc(attempt, method, data),
    });
    this.recordGrace(result ? result.grace : undefined);
    const rotation: string | undefined = result ? result.refreshToken : undefined;
    if (rotation && rotation !== refreshToken) {
      await this.storeRotated(refreshToken, rotation, attempt);
    }
    if (!result || !result.token) {
      throw new Error('token exchange returned no token');
    }
    this.closeRun();
    if (!attempt.current()) {
      throw this.closedError();
    }
    this.host.debug('connection token exchanged');
    return result.token;
  }


  // Keeps the grace the issuer published with an accepted exchange, for the next refusal run. An exchange that publishes
  // none leaves the option's grace in use; a value that is not a number of seconds from 0 up is ignored, and the grace in
  // use stays; a value above the maximum is clamped to it.
  private recordGrace(published: number | undefined): void {
    if (published === undefined) {
      this.grace = this.optionGrace;
      return;
    }
    if (Number.isFinite(published) && published >= 0) {
      this.grace = Math.min(published, maxGrace);
    }
  }


  // Stores a rotated refresh token the issuer issued. An abandoned attempt, or one a refresh(token) call superseded,
  // stores it only while the application still holds the token the attempt exchanged: a rotation made meanwhile by a
  // newer exchange or by the application is kept, so the late result of an old attempt never overwrites a newer token.
  // A failing store is reported, and the exchange goes on. A failing load is not a store failure: it fails the exchange.
  private async storeRotated(used: string, rotated: string, attempt: Attempt): Promise<void> {
    if ((!attempt.current() || attempt.superseded()) && (await this.options.load()) !== used) {
      return;
    }
    try {
      await this.options.store(rotated);
    } catch (err: any) {
      try {
        const ctx: any = {
          type: 'tokenStore',
          error: this.host.createError(errorCodes.clientRefreshToken, err?.toString() ?? ''),
        };
        this.host.emitError(ctx, (): void => {});
      } catch (handlerErr: any) {
        // Thrown by an error handler: reported, so the exchange still goes on with its access token.
        this.host.reportDispatchError(handlerErr);
      }
    }
  }


  // Opens the refusal run at its first refusal: the clock of the grace horizon starts, and the retries count from zero.
  private openRun(): void {
    if (this.refusedSince === null) {
      this.refusedSince = performance.now();
      this.refusals = 0;
    }
  }


  // Closes the refusal run: an accepted exchange, a verdict, or a disconnect of the application.
  private closeRun(): void {
    this.refusedSince = null;
    this.refusals = 0;
  }


  // The monotonic time at which the open refusal run reaches its grace horizon, in milliseconds (performance.now).
  private horizon(): number {
    return (this.refusedSince ?? 0) + this.grace * 1000;
  }


  // Whether the grace horizon of the open refusal run has passed: a refusal from then on ends the session.
  private horizonPassed(): boolean {
    return performance.now() >= this.horizon();
  }


  // Ends the session: the refusal run is closed, and the client is disconnected as unauthorized, without a reconnect.
  private endSession(): void {
    this.closeRun();
    this.host.failUnauthorized();
  }


  // The error of an exchange whose connection is gone.
  private closedError(): any {
    return this.host.createError(errorCodes.connectionClosed, 'connection closed');
  }


  // A plain call on the connection of a session and node: no message id, no retransmit, no wait for the connected state.
  // Settles with the reply context, or rejects with the rejection context: the caller calls next() of the context it gets.
  // Unlike the upstream plainCall of the rpc host, it does not wait for the connected state, and it writes on the route given.
  private sendOnRoute(cmd: any, session: string, node: string): Promise<CallResolveContext> {
    const call: Promise<CallResolveContext> = this.host.sendPlainCommand(cmd);
    // Without a transport nothing is written, and writeCommands settles nothing: the call is settled here. This is the
    // no-transport rule of writeCommands (write_commands.ts:82). Otherwise writeCommands settles the commands it refused
    // or could not write.
    if (!this.host.hasTransport()) {
      this.host.handleWriteError([cmd], false);
    }
    writeCommands(this.host, [cmd], session, node);
    return call;
  }


  // The rpc call of the exchange, on the connection of the attempt. Nothing is written once the connection of the attempt is gone: the call rejects with connectionClosed instead.
  // A server error named in refusalTypes rejects with TokenExchangeRefusedError; every other rejection is passed on as it is.
  private rpc(attempt: Attempt, method: string, data: any): Promise<RpcResult> {
    if (!attempt.current()) {
      return Promise.reject(this.closedError());
    }
    const target: Route = attempt.route();
    return this.sendOnRoute({ rpc: { method: method, data: data } }, target.session, target.node).then(
      (ctx: CallResolveContext): RpcResult => {
        try {
          return { data: ctx.reply.rpc?.data };
        } finally {
          ctx.next?.();
        }
      },
      (rejectCtx: CallRejectContext): never => {
        rejectCtx.next?.();
        throw this.classify(rejectCtx.error);
      });
  }


  // The rejection an rpc of the exchange settles with: a refusal by name becomes TokenExchangeRefusedError, nothing else
  // is classified (codes are never consulted).
  private classify(error: any): any {
    if (typeof error?.appType === 'string' && this.refusalTypes.has(error.appType)) {
      return new TokenExchangeRefusedError(error);
    }
    return error;
  }
}


/**
 * The flow of the tokenExchange option of a client, or null without the option. The option is exclusive with getToken:
 * setting both throws, and so does a refusal name that is not a non-empty string, a grace that is not a finite
 * number of seconds from 0 up, or a callback (load, store, exchange) that is not a function.
 */
export function createTokenExchangeFlow(host: ClientHost, options: Options): TokenExchangeFlow | null {
  const tokenExchange: TokenExchange | null | undefined = options.tokenExchange;
  if (!tokenExchange) {
    return null;
  }
  if (options.getToken) {
    throw new Error('tokenExchange and getToken are exclusive');
  }
  const refusalTypes: string[] | undefined = tokenExchange.refusalTypes;
  if (refusalTypes !== undefined && (!Array.isArray(refusalTypes)
    || !refusalTypes.every((name: string): boolean => typeof name === 'string' && name !== ''))) {
    throw new Error('tokenExchange.refusalTypes: entries must be non-empty strings');
  }
  const grace: number | undefined = tokenExchange.grace;
  if (grace !== undefined && (typeof grace !== 'number' || !Number.isFinite(grace) || grace < 0)) {
    throw new Error('tokenExchange.grace: must be a finite number of seconds, 0 or more');
  }
  if (typeof tokenExchange.load !== 'function' || typeof tokenExchange.store !== 'function'
    || typeof tokenExchange.exchange !== 'function') {
    throw new Error('tokenExchange: load, store and exchange must be functions');
  }
  return new TokenExchangeFlow(host, tokenExchange);
}
