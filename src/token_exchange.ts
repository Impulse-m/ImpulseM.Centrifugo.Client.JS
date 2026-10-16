import { raceCall } from './call_timer';
import type { Centrifuge } from './centrifuge';
import { UnauthorizedError } from './centrifuge';
import { connectingCodes, errorCodes } from './codes';
import type { ClientHost } from './client_host';
import type { CallRejectContext, CallResolveContext } from './rpc_tracker';
import { refreshRetries } from './refresh_slot';
import type { Options, RpcResult } from './types';
import { backoff, ttlMilliseconds } from './utils';
import { writeCommands } from './write_commands';

// The private members of the client this file reaches by name (a rename in centrifuge.ts must change this file too):
// _token, _refreshRequired, _transportId, _isConnecting, _isConnected, _call, _config, _session, _node, _emitError,
// _disconnect, _failUnauthorized, _dispatchFailed, _reportDispatchError, _refreshSlot.

/** What the `exchange` callback of tokenExchange gets. */
export interface TokenExchangeContext {
  /** The refresh token `load` returned for this exchange. */
  refreshToken: string;
  /** Sends a plain rpc call over the connection the exchange runs on: the one being set up, or the connected one.
   * It does not wait for the connected state, carries no message id and is not retransmitted. Rejects with
   * `connectionClosed` when that connection is lost, or with `transportWriteError` when the write fails. `data` is
   * what `rpc` takes for the codec in use. */
  rpc: (method: string, data: any) => Promise<RpcResult>;
}


/** What the `exchange` callback of tokenExchange resolves with. */
export interface TokenExchangeResult {
  /** The access token: the connection token from now on. */
  token: string;
  /** The rotated refresh token, when the issuer rotated it. Absent or equal to the one used: nothing is stored. */
  refreshToken?: string;
}


/** Persistence and exchange of the refresh token, see the tokenExchange option. */
export interface TokenExchange {
  /** The refresh token the application persisted, or null/undefined/'' when there is none. Called before every exchange.
   * Throw `UnauthorizedError` when the refresh token is gone for good: the session is dead, at bootstrap and at renewal. */
  load: () => string | null | undefined | Promise<string | null | undefined>;
  /** Persists a rotated refresh token. Awaited before the access token issued with it is used. A rejection is reported
   * as a `tokenStore` error, and the exchange goes on. */
  store: (refreshToken: string) => void | Promise<void>;
  /** Exchanges the refresh token for an access token. Throw `UnauthorizedError` when the issuer refused the refresh
   * token: the session is dead. Any other rejection is reported and retried with backoff. */
  exchange: (ctx: TokenExchangeContext) => Promise<TokenExchangeResult>;
}


/** The session and node the commands of an exchange are written with. */
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
  private client: Centrifuge;
  private host: ClientHost;
  private options: TokenExchange;
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


  constructor(client: Centrifuge, host: ClientHost, options: TokenExchange) {
    this.client = client;
    this.host = host;
    this.options = options;
  }


  /** Called at the start of every connect attempt: an access token that is expired, or refused, is cleared. */
  beforeConnect(): void {
    const token: string = this.client['_token'];
    const expired: boolean = token !== '' && this.expiry !== null && this.expiry.token === token
      && Date.now() >= this.expiry.expiresAt;
    if (this.client['_refreshRequired'] || expired) {
      this.client['_token'] = '';
      this.client['_refreshRequired'] = false;
    }
  }


  /**
   * Called with a connect reply before the connected state. Returns true when it took over the connect and will
   * resume it with the merged reply later, false when the connect goes on at once (a connect with a token, a reply
   * that already carries a user, a resumed reply).
   */
  intercept(result: any, resume: (result: any) => void): boolean {
    if (this.resuming || this.client['_token'] !== '' || !!result.user) {
      return false;
    }
    const bootstrap: Bootstrap = {
      transportId: this.client['_transportId'],
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
   * made while disconnected waits for the connection first. Rejects with UnauthorizedError when the application holds
   * no refresh token any more.
   */
  async fetchToken(): Promise<string> {
    if (!this.client['_isConnected']()) {
      await this.host.waitConnected(null);
    }
    const transportId: number = this.client['_transportId'];
    const running: Fetch | null = this.fetching;
    if (running !== null && running.transportId === transportId) {
      return running.token;
    }
    const token: Promise<string> = this.serialised((): Turn<string> => {
      const attempt: Attempt = this.attempt(
        (): Route => ({ session: this.client['_session'], node: this.client['_node'] }),
        (): boolean => this.client['_isConnected']() && this.client['_transportId'] === transportId,
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
    this.expiry = { token: this.client['_token'], expiresAt: Date.now() + ttl * 1000 };
    return ttlMilliseconds(ttl * (0.7 + 0.2 * Math.random()));
  }


  /** Ends a bootstrap in progress, and its retry timer: called when the connection ends. */
  reset(): void {
    const bootstrap: Bootstrap | null = this.bootstrap;
    if (bootstrap === null) {
      return;
    }
    this.bootstrap = null;
    if (bootstrap.timer !== null) {
      clearTimeout(bootstrap.timer);
      bootstrap.timer = null;
    }
  }


  // Forgets a settled fetch, unless a newer one replaced it.
  private clearFetch(fetch: Fetch): void {
    if (this.fetching === fetch) {
      this.fetching = null;
    }
  }


  // The token of a renewal on the connected transport it was requested for: a refresh token that is no longer held
  // is a dead session.
  private async renewalToken(attempt: Attempt): Promise<string> {
    const token: string | null = await this.loadAndExchange(attempt);
    if (token === null) {
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
    const timeout: number = this.client['_config'].timeout as number;
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
      && this.client['_transportId'] === bootstrap.transportId
      && this.client['_isConnecting']();
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
    const generation: number = this.client['_refreshSlot'].currentTokenGeneration();
    return {
      route: route,
      current: (): boolean => !timedOut && alive(),
      expire: (): void => {
        timedOut = true;
      },
      expired: (): boolean => timedOut,
      superseded: (): boolean => this.client['_refreshSlot'].currentTokenGeneration() !== generation,
    };
  }


  // Ends a bootstrap whose exchange stalled, or whose refresh outcome is unknown: the error is reported, then the
  // connection is closed after a backoff, so the reconnect bootstraps again. A stalled exchange is not retried in place.
  // The backoff keeps a stalled bootstrap from reconnecting at once: the connect reply resets the transport backoff.
  private abandon(bootstrap: Bootstrap, type: string, error: any): void {
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    const ctx: any = { type: type, error: error };
    this.client['_emitError'](ctx, (): void => {
      if (!this.isCurrent(bootstrap)) {
        return;
      }
      this.backedOff(bootstrap, (): void => {
        if (this.isCurrent(bootstrap)) {
          this.client['_disconnect'](connectingCodes.transportClosed, 'exchange abandoned', true);
        }
      });
    });
  }


  // Runs next after a backoff that grows with the failed bootstrap steps since the last bootstrap completed, this one
  // included, so it escalates across reconnects too. The timer is cleared by reset().
  private backedOff(bootstrap: Bootstrap, next: () => void): void {
    const delay: number = backoff(this.failures, 5000, 60000);
    this.failures++;
    bootstrap.timer = setTimeout((): void => {
      bootstrap.timer = null;
      next();
    }, delay);
  }


  // Runs a step of a bootstrap. An exception an application handler throws in it is dispatched as a failure of the client.
  private start(bootstrap: Bootstrap, result: any, resume: (result: any) => void): void {
    this.connectStep(bootstrap, result, resume).catch((err: any): void => {
      this.client['_dispatchFailed'](err);
    });
  }


  // One step of a bootstrap: loads the refresh token, exchanges it, and refreshes the anonymous connection with the
  // access token. An empty refresh token leaves the connection anonymous.
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
      if (err instanceof UnauthorizedError) {
        this.client['_failUnauthorized']();
        return;
      }
      this.retry(bootstrap, result, resume, 'connectToken', this.connectTokenError(err));
      return;
    }
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    if (token === null) {
      this.finish(bootstrap, result, resume, null);
      return;
    }
    this.refreshConnection(bootstrap, result, resume, token);
  }


  // The error of a bootstrap step that could not get the access token: reported as connectToken.
  private connectTokenError(err: any): any {
    return this.host.createError(errorCodes.clientConnectToken, err?.toString() ?? '');
  }


  // Reports a failed step of a bootstrap, and runs the step again on the same connection after a backoff.
  private retry(bootstrap: Bootstrap, result: any, resume: (result: any) => void, type: string, error: any): void {
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    const ctx: any = { type: type, error: error };
    this.client['_emitError'](ctx, (): void => {
      if (!this.isCurrent(bootstrap)) {
        return;
      }
      this.backedOff(bootstrap, (): void => {
        this.start(bootstrap, result, resume);
      });
    });
  }


  // Sets the access token and refreshes the anonymous connection with it, on the connection of the attempt.
  private refreshConnection(bootstrap: Bootstrap, result: any, resume: (result: any) => void, token: string): void {
    const cmd: any = { refresh: { token: token } };
    this.client['_token'] = token;
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
      this.client['_dispatchFailed'](err);
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
    if (this.client['_token'] === token) {
      this.client['_token'] = '';
    }
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    if (err.code === errorCodes.timeout || err.code === errorCodes.transportWriteError) {
      this.abandon(bootstrap, 'connect', err);
    } else if (refreshRetries(err.code, err.temporary === true)) {
      this.retry(bootstrap, result, resume, 'connect', err);
    } else {
      this.client['_disconnect'](err.code, err.message, false);
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
      this.client['_dispatchFailed'](err);
    } finally {
      this.resuming = false;
    }
  }


  // Exchanges a refresh token. The issuer refusing it is retried once with the token the application holds by then,
  // when that one is another: another client of the installation rotated it meanwhile. The same token, or none, is
  // terminal, and so is a refusal of the retry.
  private async exchangeRefresh(refreshToken: string, attempt: Attempt, retried: boolean): Promise<string> {
    try {
      return await this.exchangeWith(refreshToken, attempt);
    } catch (err: any) {
      if (!(err instanceof UnauthorizedError) || retried || !attempt.current()) {
        throw err;
      }
      const current: string | null | undefined = await this.options.load();
      if (!current || current === refreshToken) {
        throw err;
      }
      return this.exchangeRefresh(current, attempt, true);
    }
  }


  // The rotated refresh token of an exchange result, or null when the issuer did not rotate it.
  private rotated(used: string, result: TokenExchangeResult | undefined): string | null {
    return result && result.refreshToken && result.refreshToken !== used ? result.refreshToken : null;
  }


  // One exchange: the app's exchange callback, then the store of the rotated refresh token, awaited before the access
  // token is used. Resolves with the access token. The access token of an attempt that timed out, or was abandoned, is
  // not used, but its rotated refresh token is stored (see storeRotated): the issuer has already consumed the pair.
  private async exchangeWith(refreshToken: string, attempt: Attempt): Promise<string> {
    const result: TokenExchangeResult = await this.options.exchange({
      refreshToken: refreshToken,
      rpc: (method: string, data: any): Promise<RpcResult> => this.rpc(attempt, method, data),
    });
    const rotation: string | null = this.rotated(refreshToken, result);
    if (rotation !== null) {
      await this.storeRotated(refreshToken, rotation, attempt);
    }
    if (!attempt.current()) {
      throw this.closedError();
    }
    if (!result || !result.token) {
      throw new Error('token exchange returned no token');
    }
    this.host.debug('connection token exchanged');
    return result.token;
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
        this.client['_emitError'](ctx, (): void => {});
      } catch (handlerErr: any) {
        // Thrown by an error handler: reported, so the exchange still goes on with its access token.
        this.client['_reportDispatchError'](handlerErr);
      }
    }
  }


  // The error of an exchange whose connection is gone.
  private closedError(): any {
    return this.host.createError(errorCodes.connectionClosed, 'connection closed');
  }


  // A plain call on the connection of a session and node: no message id, no retransmit, no wait for the connected state.
  // Settles with the reply context, or rejects with the rejection context: the caller calls next() of the context it gets.
  // Unlike the upstream plainCall of the rpc host, it does not wait for the connected state, and it writes on the route given.
  private sendOnRoute(cmd: any, session: string, node: string): Promise<CallResolveContext> {
    const call: Promise<CallResolveContext> = this.client['_call'](cmd, true);
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
        throw rejectCtx.error;
      });
  }
}


/**
 * The flow of the tokenExchange option of a client, or null without the option. The option is exclusive with getToken:
 * setting both throws.
 */
export function createTokenExchangeFlow(client: Centrifuge, host: ClientHost, options: Options): TokenExchangeFlow | null {
  if (!options.tokenExchange) {
    return null;
  }
  if (options.getToken) {
    throw new Error('tokenExchange and getToken are exclusive');
  }
  return new TokenExchangeFlow(client, host, options.tokenExchange);
}
