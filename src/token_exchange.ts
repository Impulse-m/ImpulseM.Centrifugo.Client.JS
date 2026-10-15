import type { Centrifuge } from './centrifuge';
import { UnauthorizedError } from './centrifuge';
import { errorCodes } from './codes';
import type { ClientHost } from './client_host';
import type { CallRejectContext, CallResolveContext } from './rpc_tracker';
import { refreshRetries } from './refresh_slot';
import type { RpcResult } from './types';
import { backoff, ttlMilliseconds } from './utils';
import { writeCommands } from './write_commands';

// The private members of the client this file reaches by name (a rename in centrifuge.ts must change this file too):
// _token, _refreshRequired, _transportId, _isConnecting, _isConnected, _call, _session, _node, _emitError, _disconnect,
// _failUnauthorized, _dispatchFailed, _reportDispatchError.

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
  /** The refresh token the application persisted, or null/undefined/'' when there is none. Called before every exchange. */
  load: () => string | null | undefined | Promise<string | null | undefined>;
  /** Persists a rotated refresh token. Awaited before the access token issued with it is used. */
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
 * An exchange whose connection ended is abandoned: its access token is not used.
 */
interface Attempt {
  route: () => Route;
  current: () => boolean;
}


/** The expiry the SDK knows of its access token: from the ttl of the reply that accepted it. */
interface ExpiryRecord {
  token: string;
  expiresAt: number;
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
   * resume it with the merged reply later, false when the connect goes on at once (a connect with a token, a resumed
   * reply).
   */
  intercept(result: any, resume: (result: any) => void): boolean {
    if (this.resuming || this.client['_token'] !== '') {
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
    const refreshToken: string | null | undefined = await this.options.load();
    if (!refreshToken) {
      throw new UnauthorizedError('');
    }
    const transportId: number = this.client['_transportId'];
    const attempt: Attempt = {
      route: (): Route => ({ session: this.client['_session'], node: this.client['_node'] }),
      current: (): boolean => this.client['_isConnected']() && this.client['_transportId'] === transportId,
    };
    return this.exchangeRefresh(refreshToken, attempt, false);
  }


  /** The delay of the refresh timer for a ttl in seconds: a random moment between 0.7 and 0.9 of the ttl. */
  renewalDelay(ttl: number): number {
    return ttlMilliseconds(ttl * (0.7 + 0.2 * Math.random()));
  }


  /** Records the expiry of the access token from the ttl of the reply that accepted it. */
  observe(token: string, ttl: number): void {
    this.expiry = { token: token, expiresAt: Date.now() + ttl * 1000 };
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


  // Whether a continuation of a bootstrap still belongs to the connection attempt it runs for.
  private isCurrent(bootstrap: Bootstrap): boolean {
    return this.bootstrap === bootstrap
      && this.client['_transportId'] === bootstrap.transportId
      && this.client['_isConnecting']();
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
    let refreshToken: string | null | undefined;
    try {
      refreshToken = await this.options.load();
    } catch (err: any) {
      this.retry(bootstrap, result, resume, 'connectToken', this.connectTokenError(err));
      return;
    }
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    if (!refreshToken) {
      this.finish(bootstrap, result, resume, null);
      return;
    }
    const attempt: Attempt = {
      route: (): Route => ({ session: result.session, node: result.node }),
      current: (): boolean => this.isCurrent(bootstrap),
    };
    let token: string;
    try {
      token = await this.exchangeRefresh(refreshToken, attempt, false);
    } catch (err: any) {
      if (!this.isCurrent(bootstrap)) {
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
    this.refreshConnection(bootstrap, result, resume, token);
  }


  // The error of a bootstrap step that could not get the access token: reported as connectToken.
  private connectTokenError(err: any): any {
    return this.host.createError(errorCodes.clientConnectToken, err?.toString() ?? '');
  }


  // Reports a failed step of a bootstrap, and runs the step again on the same connection after a backoff. The backoff
  // counts the failed steps since the last bootstrap completed, so it escalates across reconnects too.
  private retry(bootstrap: Bootstrap, result: any, resume: (result: any) => void, type: string, error: any): void {
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    this.client['_emitError']({ type: type, error: error }, (): void => {
      if (!this.isCurrent(bootstrap)) {
        return;
      }
      const delay: number = backoff(this.failures, 5000, 60000);
      this.failures++;
      bootstrap.timer = setTimeout((): void => {
        this.start(bootstrap, result, resume);
      }, delay);
    });
  }


  // Sets the access token and refreshes the anonymous connection with it, on the connection of the attempt.
  private refreshConnection(bootstrap: Bootstrap, result: any, resume: (result: any) => void, token: string): void {
    const cmd: any = { refresh: { token: token } };
    this.client['_token'] = token;
    this.plainCall(cmd, result.session, result.node).then((ctx: CallResolveContext): void => {
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
  // the connection has moved on. A temporary refusal is retried with a new exchange; any other ends the connection.
  private refreshFailed(bootstrap: Bootstrap, result: any, resume: (result: any) => void, token: string, err: any): void {
    if (this.client['_token'] === token) {
      this.client['_token'] = '';
    }
    if (!this.isCurrent(bootstrap)) {
      return;
    }
    if (refreshRetries(err.code, err.temporary === true)) {
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


  // One exchange: the app's exchange callback, then the store of the rotated refresh token, awaited before the access
  // token is used. Resolves with the access token. The access token of an abandoned attempt is not used, but its
  // rotated refresh token is stored (see storeRotated): the issuer has already consumed the pair.
  private async exchangeWith(refreshToken: string, attempt: Attempt): Promise<string> {
    const result: TokenExchangeResult = await this.options.exchange({
      refreshToken: refreshToken,
      rpc: (method: string, data: any): Promise<RpcResult> => this.rpc(attempt.route, method, data),
    });
    if (result && result.refreshToken && result.refreshToken !== refreshToken) {
      await this.storeRotated(refreshToken, result.refreshToken, attempt);
    }
    if (!attempt.current()) {
      throw this.host.createError(errorCodes.connectionClosed, 'connection closed during the token exchange');
    }
    this.host.debug('connection token exchanged');
    if (!result || !result.token) {
      throw new Error('token exchange returned no token');
    }
    return result.token;
  }


  // Stores a rotated refresh token the issuer issued. An abandoned attempt stores it only while the application still
  // holds the token the attempt exchanged: a rotation made meanwhile by a newer exchange is kept, so the late result of
  // an old attempt never overwrites a newer token. A failing store is reported, and the exchange goes on.
  private async storeRotated(used: string, rotated: string, attempt: Attempt): Promise<void> {
    try {
      if (!attempt.current() && (await this.options.load()) !== used) {
        return;
      }
      await this.options.store(rotated);
    } catch (err: any) {
      try {
        this.client['_emitError']({ type: 'tokenStore', error: this.host.createError(errorCodes.clientRefreshToken, err?.toString() ?? '') }, (): void => {});
      } catch (handlerErr: any) {
        // Thrown by an error handler: reported, so the exchange still goes on with its access token.
        this.client['_reportDispatchError'](handlerErr);
      }
    }
  }


  // A plain call on the connection of a session and node: no message id, no retransmit, no wait for the connected state.
  // Settles with the reply context, or rejects with the rejection context: the caller calls next() of the context it gets.
  private plainCall(cmd: any, session: string, node: string): Promise<CallResolveContext> {
    const call: Promise<CallResolveContext> = this.client['_call'](cmd, true);
    if (!writeCommands(this.host, [cmd], session, node)) {
      this.host.handleWriteError([cmd], false);
    }
    return call;
  }


  // The rpc call of the exchange, on the connection the route names.
  private rpc(route: () => Route, method: string, data: any): Promise<RpcResult> {
    const target: Route = route();
    return this.plainCall({ rpc: { method: method, data: data } }, target.session, target.node).then(
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
