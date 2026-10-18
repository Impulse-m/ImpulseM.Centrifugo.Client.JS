import { Centrifuge } from './centrifuge';
import { CentrifugeProtobuf } from './protobuf_client';
import { disconnectedCodes, errorCodes } from './codes';
import { callTimer } from './call_timer';
import { laterResendDelay, maxLaterResends, retrySignal } from './rpc_tracker';
import { RpcPoolEndpoint } from './rpc_pool_endpoint';
import type { PendingCall, PoolConnection } from './rpc_pool_endpoint';
import { fetchRedirect, RedirectGate } from './rpc_pool_redirect';
import { poolError } from './rpc_pool_error';
import { tokenSourceOf } from './token_source';
import type { SourcedToken, TokenSource } from './token_source';
import { UnauthorizedError } from './unauthorized_error';
import type { Options, RpcOptions, RpcResult } from './types';

// The connection options a pool passes on to its clients. The type of RpcPoolOptions.connection and the check of
// connectionOptionsOf both come from this list, so they cannot disagree.
const connectionOptionNames = [
  'token', 'getToken', 'headers', 'name', 'version', 'timeout', 'websocket',
  'minReconnectDelay', 'maxReconnectDelay', 'maxServerPingDelay', 'debug',
] as const;
type ConnectionOptionName = typeof connectionOptionNames[number];

/** The options of a pool call's client a pool accepts: nothing else of the client options is taken. */
export interface RpcPoolOptions {
  /**
   * The client which owns the access token of the pool's connections. The pool lends the client's current token to each
   * connection, and renews it through that client when the token is refused or expires: the pool runs no exchange and
   * calls no getter of its own. Exclusive with connection.token and connection.getToken; connection.tokenExchange is
   * refused. It works with every token mode of the client, and needs no change of the client.
   */
  tokenFrom?: Centrifuge;
  /**
   * Endpoints of an endpoint key. '' is the key of a call without one. The key is whatever the server answered the
   * application (a cluster, a region); the pool does not interpret it. Resolved once per key, until close(); a resolution
   * which fails is resolved again by the next call. Each key keeps its connections and its state for the life of the
   * pool, so keys should be a bounded set (clusters, regions), not one per object.
   */
  endpoints: (endpointKey: string) => string | string[] | Promise<string | string[]>;
  /**
   * Options of each connection: token, getToken, headers, name, version, timeout, websocket, minReconnectDelay,
   * maxReconnectDelay, maxServerPingDelay and debug. Any other option throws a badConfiguration error, instance in
   * particular: pool calls are plain calls, without an instance.
   */
  connection?: Partial<Pick<Options, ConnectionOptionName>>;
  /**
   * Name of the per-call header which carries RpcPoolCallOptions.affinity. Required to pass affinity; it must match
   * the allow-list of the method and the affinity header of the target on the server.
   */
  affinityHeader?: string;
  /** Connections opened per endpoint at most. Default 2. */
  maxConnectionsPerEndpoint?: number;
  /**
   * Calls in flight per connection at most. Default 1, which works against a server at its default client.concurrency
   * (0, one command at a time). A larger window needs client.concurrency >= window + 1 on the server.
   */
  maxInflightPerConnection?: number;
  /**
   * Calls waiting for a slot at most, over all endpoints: the calls waiting for their endpoint key to resolve, and the
   * ones waiting in a line for a connection. Beyond it rpc() rejects with limitExceeded. Default 64. A change of a
   * connection places only the calls waiting for its endpoint key, so the bound does not multiply the work of an event.
   * A resend of a call already admitted (lost connection, later, move) is queued beyond this bound, and is bounded by
   * retries instead. The redirect fetches waiting for a slot are bounded by it too.
   */
  maxQueued?: number;
  /**
   * Closes a connection without calls for this long, in milliseconds, at most 2147483647 (the longest timer). 0 keeps it
   * open. Default 60000.
   */
  idleTimeout?: number;
  /**
   * Resends of a call lost with its connection, refused with the later signal, or moved by reroute, counted together.
   * Default 3, the main client's count for the later signal. A later resend waits a delay drawn uniformly from
   * [0, 250 ms * 2^n], n being the resends before it, as the main client does.
   */
  retries?: number;
  /**
   * A server error which moves the call to another endpoint key: returns the key, or undefined when the error is not a
   * move. Called on server errors only, while the call has resends left. The error is the rejection value of rpc().
   */
  reroute?: (error: any) => string | undefined;
  /**
   * A reply or an error which moves the bytes to another URL (a presigned URL, a CDN): returns the target, or undefined.
   * Called once per call with the outcome the server or the connection gave; not for a timeout or a cancel of the pool.
   */
  redirect?: (reply: RpcResult | null, error: any | null) => RpcPoolRedirect | undefined;
  /** Redirect fetches in flight at most, over the pool. Default 4. */
  maxRedirectsInflight?: number;
  /** Fetch function for redirects. Default the global fetch. */
  fetch?: typeof fetch;
}

/** The options of one pool call. */
export interface RpcPoolCallOptions {
  /** Endpoint key, given to endpoints(). Default ''. */
  endpointKey?: string;
  /** Server-issued opaque affinity hint, sent in the affinityHeader header. Default none. */
  affinity?: string;
  /** Per-call headers, as RpcOptions.headers. The affinityHeader among them is refused with badConfiguration. */
  headers?: Record<string, string>;
  /** Rejects the promise with cancelled. The call itself drains on its connection: no cancel goes to the server. */
  signal?: AbortSignal;
  /**
   * Deadline of the call across its resends and its redirect fetch, in milliseconds. Default connection.timeout, which
   * is also the most it can be: a reply is bounded by the connection timeout, so a longer call needs a longer one.
   */
  timeout?: number;
}

/** The URL a redirect sends the bytes of a call to. */
export interface RpcPoolRedirect {
  url: string;
  /** Request headers of the fetch. Default none. */
  headers?: Record<string, string>;
  /** Body bytes accepted at most. Default 16 MiB. */
  maxBytes?: number;
}

/** The state of a pool, for the application's own pacing. */
export interface RpcPoolStats {
  /** Connections open or connecting. */
  connections: number;
  /** Calls holding a slot of a connection. */
  inflight: number;
  /** Calls waiting for a slot. */
  queued: number;
  /** Redirect fetches holding a slot. */
  redirects: number;
}

// The reply timeout of a client when none is set. Keep it equal to the default timeout of centrifuge.ts (config).
const defaultTimeout: number = 5000;
const defaultMaxConnectionsPerEndpoint: number = 2;
const defaultMaxInflightPerConnection: number = 1;
const defaultMaxQueued: number = 64;
const defaultIdleTimeout: number = 60000;
const defaultMaxRedirectsInflight: number = 4;
// The longest delay a timer runs for, as ttlMilliseconds in utils.ts: the idleTimeout may not be longer.
const maxTimerDelay: number = 2147483647;
// Codes from this one up are the server's errors; the ones below are the client's own, see errorCodes.
const serverErrorFloor: number = 100;
const noop: () => void = (): void => {};
const noData: Uint8Array = new Uint8Array(0);


/**
 * A pool of connections for calls which carry large payloads: several connections per endpoint, each with a window of
 * calls in flight, a bounded wait queue, and the server's moves (reroute, redirect) honoured. The calls are plain
 * requests: no message id and no instance, so they are retried only where the contract allows it, and their methods
 * must be idempotent. The pool speaks protobuf only, and opens connections on demand.
 */
export class RpcPool {
  private readonly resolver: (endpointKey: string) => string | string[] | Promise<string | string[]>;
  private readonly connectionOptions: Partial<Options>;
  private readonly fetchOption: typeof fetch | undefined;
  private readonly rerouteHook: ((error: any) => string | undefined) | undefined;
  private readonly redirectHook: ((reply: RpcResult | null, error: any | null) => RpcPoolRedirect | undefined) | undefined;
  private readonly affinityHeader: string | undefined;
  private readonly timeout: number;
  private readonly maxConnectionsPerEndpoint: number;
  private readonly maxInflightPerConnection: number;
  private readonly maxQueued: number;
  private readonly idleTimeout: number;
  private readonly retries: number;
  private readonly redirects: RedirectGate;
  // The token source of tokenFrom, null without it: a pool which takes no token from a client holds none.
  private readonly source: TokenSource | null;
  private readonly resolved: Map<string, Promise<string[]>> = new Map();
  private readonly groups: Map<string, RpcPoolEndpoint> = new Map();
  // The connections retired while they carry calls: they close when their last call settles, see retire.
  private readonly retiring: Set<PoolConnection> = new Set();
  // The calls not settled yet, so that close() settles them all.
  private readonly calls: Set<PendingCall> = new Set();
  // The groups whose waiting calls may be placed now: a pass over a group is started by the event that frees its room.
  private readonly dirty: Set<RpcPoolEndpoint> = new Set();
  // The calls in the lines of the groups, counted for maxQueued; a settled call leaves the count when it settles.
  private queued: number = 0;
  private readonly errorListeners: Array<(ctx: { endpoint: string; error: any }) => void> = [];
  private closed: boolean = false;
  private dispatching: boolean = false;
  // Told by the token source when its client connects or ends: the waiting calls of every group are placed or failed then.
  private readonly onSource: () => void = (): void => {
    this.dispatchAll();
  };


  constructor(options: RpcPoolOptions) {
    if (typeof options.endpoints !== 'function') {
      throw poolError(errorCodes.badConfiguration, 'endpoints must be a function');
    }
    this.resolver = options.endpoints;
    this.connectionOptions = connectionOptionsOf(options.connection);
    this.timeout = this.connectionOptions.timeout ?? defaultTimeout;
    if (options.affinityHeader !== undefined && (typeof options.affinityHeader !== 'string' || options.affinityHeader === '')) {
      throw poolError(errorCodes.badConfiguration, 'affinityHeader must be a header name');
    }
    this.affinityHeader = options.affinityHeader;
    this.maxConnectionsPerEndpoint = integerOption('maxConnectionsPerEndpoint', options.maxConnectionsPerEndpoint, defaultMaxConnectionsPerEndpoint, 1);
    this.maxInflightPerConnection = integerOption('maxInflightPerConnection', options.maxInflightPerConnection, defaultMaxInflightPerConnection, 1);
    this.maxQueued = integerOption('maxQueued', options.maxQueued, defaultMaxQueued, 0);
    this.idleTimeout = integerOption('idleTimeout', options.idleTimeout, defaultIdleTimeout, 0);
    if (this.idleTimeout > maxTimerDelay) {
      throw poolError(errorCodes.badConfiguration, `idleTimeout must be at most ${maxTimerDelay} milliseconds`);
    }
    // Every call deadline is a timer of this span, so a longer one would fire at once.
    if (this.timeout > maxTimerDelay) {
      throw poolError(errorCodes.badConfiguration, `connection.timeout must be at most ${maxTimerDelay} milliseconds`);
    }
    this.retries = integerOption('retries', options.retries, maxLaterResends, 0);
    this.redirects = new RedirectGate(integerOption('maxRedirectsInflight', options.maxRedirectsInflight, defaultMaxRedirectsInflight, 1));
    this.rerouteHook = options.reroute;
    this.redirectHook = options.redirect;
    this.fetchOption = options.fetch;
    // Last: a pool which fails a check above leaves no listener on the client it names.
    this.source = tokenSourceFrom(options.tokenFrom, this.connectionOptions);
    this.source?.listen(this.onSource);
  }


  /**
   * Runs a call on a connection of its endpoint key. It takes a slot of a connection with room, opens one when the key
   * has none and may open one, or waits in the queue. Rejects with badConfiguration for a timeout out of range, with
   * limitExceeded when the queue is full, with timeout at the deadline, with cancelled when the signal aborts, and with
   * the error of the server otherwise.
   */
  rpc(method: string, data: Uint8Array, opts?: RpcPoolCallOptions): Promise<RpcResult> {
    return new Promise<RpcResult>((resolve: (result: RpcResult) => void, reject: (error: any) => void): void => {
      if (this.closed) {
        reject(poolError(errorCodes.clientClosed, 'rpc pool is closed'));
        return;
      }
      // The session of the client ended as unauthorized, or no token can be lent and the client is not connecting: no
      // connection is opened, and the call is not sent. Removed listeners are put back by the token requests, and by a
      // call while a request waits for an event of the client, since only that event can answer the waiting request.
      this.source?.reattachWaiting();
      if (this.source?.refuses() === true) {
        reject(poolError(errorCodes.clientDisconnected, 'unauthorized'));
        return;
      }
      const signal: AbortSignal | undefined = opts?.signal;
      if (signal?.aborted === true) {
        reject(poolError(errorCodes.cancelled, 'cancelled'));
        return;
      }
      let headers: Record<string, string> | undefined;
      let timeout: number;
      try {
        headers = this.headersOf(opts);
        timeout = this.timeoutOf(opts);
      } catch (e) {
        reject(e);
        return;
      }
      const call: PendingCall = {
        method: method,
        data: data,
        headers: headers,
        key: opts?.endpointKey ?? '',
        deadline: Date.now() + timeout,
        resends: 0,
        laterResends: 0,
        settled: false,
        sent: false,
        queued: false,
        line: null,
        previous: null,
        next: null,
        timer: null,
        resend: null,
        detach: noop,
        controller: null,
        resolve: resolve,
        reject: reject,
      };
      if (signal !== undefined) {
        const onAbort: () => void = (): void => this.abort(call);
        signal.addEventListener('abort', onAbort, { once: true });
        call.detach = (): void => signal.removeEventListener('abort', onAbort);
      }
      this.calls.add(call);
      call.timer = callTimer(call.deadline, (): void => this.expire(call));
      this.attempt(call, true);
    });
  }


  /** The state of the pool: see RpcPoolStats. */
  stats(): RpcPoolStats {
    let connections: number = 0;
    let inflight: number = 0;
    for (const group of this.groups.values()) {
      for (const conn of group.all()) {
        connections++;
        inflight += conn.inflight;
      }
    }
    for (const conn of this.retiring) {
      connections++;
      inflight += conn.inflight;
    }
    return { connections: connections, inflight: inflight, queued: this.queued, redirects: this.redirects.inflight };
  }


  /** Closes every connection. Waiting calls reject with cancelled, calls in flight with connectionClosed. */
  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.source?.unlisten(this.onSource);
    for (const group of this.groups.values()) {
      for (const conn of group.all()) {
        conn.idle?.clear();
        conn.idle = null;
        conn.client.disconnect();
      }
    }
    for (const conn of Array.from(this.retiring)) {
      conn.idle?.clear();
      conn.idle = null;
      conn.client.disconnect();
    }
    this.retiring.clear();
    for (const call of Array.from(this.calls)) {
      // A call whose reply carried a redirect was served already: it is in flight until its fetch ends.
      if (!call.sent && call.controller === null) {
        this.fail(call, poolError(errorCodes.cancelled, 'cancelled'));
      } else {
        this.fail(call, poolError(errorCodes.connectionClosed, 'connection closed'));
      }
    }
  }


  /**
   * Reports a connection that ended for good with a code other than the one of disconnect(): a message size close,
   * an unauthorized token, and the like. The listener gets the endpoint and the disconnect.
   */
  on(event: 'error', listener: (ctx: { endpoint: string; error: any }) => void): this {
    if (event === 'error') {
      this.errorListeners.push(listener);
    }
    return this;
  }


  // The headers of a call, with the affinity hint in its header. The affinity header is the pool's own: a call may not
  // set it by its headers, and an affinity needs it.
  private headersOf(opts?: RpcPoolCallOptions): Record<string, string> | undefined {
    const headers: Record<string, string> | undefined = opts?.headers;
    const affinity: string | undefined = opts?.affinity;
    const name: string | undefined = this.affinityHeader;
    if (name === undefined) {
      if (affinity !== undefined) {
        throw poolError(errorCodes.badConfiguration, 'affinity needs the affinityHeader option');
      }
      return headers;
    }
    if (headers !== undefined && hasHeader(headers, name)) {
      throw poolError(errorCodes.badConfiguration, 'the affinity header is set by the affinity option, not by headers');
    }
    if (affinity === undefined) {
      return headers;
    }
    return { ...headers, [name]: affinity };
  }


  // The deadline span of a call in milliseconds: the connection timeout when unset, else a positive finite number at
  // most the connection timeout, since the reply of a connection is bounded by it.
  private timeoutOf(opts?: RpcPoolCallOptions): number {
    const timeout: number = opts?.timeout ?? this.timeout;
    if (!Number.isFinite(timeout) || timeout <= 0) {
      throw poolError(errorCodes.badConfiguration, 'timeout must be a positive number of milliseconds');
    }
    if (timeout > this.timeout) {
      throw poolError(errorCodes.badConfiguration, 'timeout is above connection.timeout, which bounds the reply of a connection');
    }
    return timeout;
  }


  // The fetch of redirects: the option, else the global fetch as it is when a redirect is fetched, not when the pool is made.
  private fetchOf(): typeof fetch | undefined {
    return this.fetchOption ?? (typeof fetch === 'function' ? fetch : undefined);
  }


  // The endpoints of a key: resolved once, and resolved again after a failure.
  private endpointsOf(key: string): Promise<string[]> {
    let urls: Promise<string[]> | undefined = this.resolved.get(key);
    if (urls === undefined) {
      // A resolver that throws or rejects rejects the lookup: the calls of the key reject with its error.
      urls = Promise.resolve()
        .then((): string | string[] | Promise<string | string[]> => this.resolver(key))
        .then((value: string | string[]): string[] => endpointList(value));
      this.resolved.set(key, urls);
      urls.catch((): void => {
        this.resolved.delete(key);
      });
    }
    return urls;
  }


  // The endpoint group of a key, made with its endpoints the first time the key is used.
  private groupOf(key: string, urls: string[]): RpcPoolEndpoint {
    let group: RpcPoolEndpoint | undefined = this.groups.get(key);
    if (group === undefined) {
      group = new RpcPoolEndpoint(urls);
      this.groups.set(key, group);
    }
    return group;
  }


  // Places a call on a connection of its key, or queues it. A fresh call beyond maxQueued is refused; a call sent again
  // is queued whatever the bound, since it was admitted already. A call for a key not resolved yet is counted in
  // queued while it resolves, so the bound covers it too: a resolver which is slow cannot admit calls without a limit.
  // The first such call is admitted at any bound, since its connection opens once the key resolves, as a call on a
  // resolved key with a free slot is.
  private attempt(call: PendingCall, fresh: boolean): void {
    const resolving: boolean = !this.groups.has(call.key);
    if (resolving) {
      if (fresh && this.queued > 0 && this.queued >= this.maxQueued) {
        this.fail(call, poolError(errorCodes.limitExceeded, 'too many rpc calls waiting for a connection'));
        return;
      }
      this.count(call);
    }
    this.endpointsOf(call.key).then((urls: string[]): void => {
      if (call.settled) {
        return;
      }
      try {
        const group: RpcPoolEndpoint = this.groupOf(call.key, urls);
        if (resolving) {
          this.uncount(call);
        }
        if (this.assign(call, group)) {
          return;
        }
        if (fresh && this.queued >= this.maxQueued) {
          this.fail(call, poolError(errorCodes.limitExceeded, 'too many rpc calls waiting for a connection'));
          return;
        }
        this.count(call);
        group.enqueue(call);
      } catch (e) {
        this.fail(call, e);
      }
    }, (error: any): void => {
      this.fail(call, error);
    });
  }


  // Counts a call in queued, the bound of the waiting calls.
  private count(call: PendingCall): void {
    call.queued = true;
    this.queued++;
  }


  // Stops counting a call in queued: it holds a slot, it failed, or it settled.
  private uncount(call: PendingCall): void {
    call.line?.dequeue(call);
    if (call.queued) {
      call.queued = false;
      this.queued--;
    }
  }


  // Sends a call on a connection of its group, if there is one with room or one may be opened.
  private assign(call: PendingCall, group: RpcPoolEndpoint): boolean {
    // A call placed after the session ended, or with no token to lend, would open or use a connection under an identity
    // which is gone: it fails here, as rpc() fails it. Calls queued before the end fail through this check too.
    if (this.source?.refuses() === true) {
      throw poolError(errorCodes.clientDisconnected, 'unauthorized');
    }
    // The client is connecting: no connection's identity is confirmed, so the call waits in its line until the client
    // connects, or ends (the token source tells the pool then).
    if (this.source?.connecting() === true) {
      return false;
    }
    this.retireStale(group);
    const roomy: PoolConnection | null = group.roomy(this.maxInflightPerConnection);
    if (roomy !== null) {
      this.send(call, roomy);
      return true;
    }
    const url: string | null = group.openable(this.maxConnectionsPerEndpoint);
    if (url === null) {
      return false;
    }
    this.send(call, this.open(group, url));
    return true;
  }


  // Sends the call on a connection: the slot is taken until the reply or the error arrives.
  private send(call: PendingCall, conn: PoolConnection): void {
    conn.inflight++;
    call.sent = true;
    const options: RpcOptions | undefined = call.headers === undefined ? undefined : { headers: call.headers };
    conn.client.rpc(call.method, call.data, options).then(
      (reply: RpcResult): void => this.replied(call, conn, reply),
      (error: any): void => this.errored(call, conn, error)
    );
  }


  // Opens a connection to an endpoint. Its connect is in flight until its first connected event.
  private open(group: RpcPoolEndpoint, url: string): PoolConnection {
    const source: TokenSource | null = this.source;
    // The token getter of the client is bound to the connection below. The getter runs only from connect(), which is
    // called after the connection exists.
    const options: Partial<Options> = source === null
      ? this.connectionOptions
      : { ...this.connectionOptions, getToken: (): Promise<string> => this.tokenOf(conn, source) };
    const client: CentrifugeProtobuf = new CentrifugeProtobuf(url, options);
    const conn: PoolConnection = {
      url: url,
      group: group,
      client: client,
      inflight: 0,
      connecting: true,
      closeCode: null,
      lastUse: Date.now(),
      idle: null,
      token: '',
      generation: null,
      retired: false,
    };
    client.on('connected', (): void => {
      conn.connecting = false;
      this.dispatch(group);
    });
    client.on('disconnected', (ctx: any): void => {
      this.lost(conn, ctx);
    });
    // The connection joins its group after connect() returned: a connect which throws leaves no connection behind. A
    // connection which ended during connect() (lost has run already) joins no group.
    client.connect();
    if (conn.closeCode === null) {
      group.add(conn);
    }
    return conn;
  }


  // The token of a connection from the source. A token request after the first means the connection's last token is
  // no longer usable. A connection which is given the token of another identity is retired, and fails as unauthorized:
  // its calls are never sent under the identity of another connection.
  private tokenOf(conn: PoolConnection, source: TokenSource): Promise<string> {
    if (conn.generation !== null && conn.generation !== source.generation()) {
      this.retire(conn);
      return Promise.reject(new UnauthorizedError('unauthorized'));
    }
    return source.token(conn, conn.token).then((sourced: SourcedToken): string => {
      if (conn.generation === null) {
        conn.generation = sourced.generation;
      } else if (conn.generation !== sourced.generation) {
        this.retire(conn);
        throw new UnauthorizedError('unauthorized');
      }
      conn.token = sourced.token;
      return sourced.token;
    });
  }


  // Takes a connection out of its group: it takes no new call, and it does not count against the endpoint's connections.
  // It closes at once when it carries no call; else it waits in retiring, and closes when its last call settles, so
  // its calls complete under the identity they were sent with.
  private retire(conn: PoolConnection): void {
    if (conn.retired) {
      return;
    }
    conn.retired = true;
    conn.group.remove(conn);
    if (conn.inflight > 0) {
      this.retiring.add(conn);
      return;
    }
    this.closeRetired(conn);
  }


  // Closes a retired connection. The close reports no error: its code is disconnectCalled.
  private closeRetired(conn: PoolConnection): void {
    this.retiring.delete(conn);
    conn.idle?.clear();
    conn.idle = null;
    conn.client.disconnect();
  }


  // Retires the connections of a group whose token belongs to another identity than the current one. A pass over the
  // group, not over the pool: the calls of other groups are not looked at.
  private retireStale(group: RpcPoolEndpoint): void {
    if (this.source === null) {
      return;
    }
    const generation: number = this.source.generation();
    let stale: PoolConnection | null = group.staleOf(generation);
    while (stale !== null) {
      this.retire(stale);
      stale = group.staleOf(generation);
    }
  }


  // A connection ended for good. It leaves its group; the calls waiting take the other connections, and the
  // application hears of the end unless it asked for it, or unless the end is the retirement of an identity change, which
  // the application made itself (as Java does not report it either).
  private lost(conn: PoolConnection, ctx: any): void {
    conn.connecting = false;
    conn.closeCode = ctx.code;
    conn.idle?.clear();
    conn.idle = null;
    conn.group.remove(conn);
    this.retiring.delete(conn);
    this.source?.forget(conn);
    this.dispatch(conn.group);
    if (ctx.code !== disconnectedCodes.disconnectCalled && !conn.retired) {
      for (const listener of this.errorListeners) {
        listener({ endpoint: conn.url, error: ctx });
      }
    }
  }


  // A reply of the server: the slot is free, and the outcome settles the call unless the call settled before.
  private replied(call: PendingCall, conn: PoolConnection, reply: RpcResult): void {
    conn.inflight--;
    call.sent = false;
    if (!call.settled) {
      this.finish(call, conn.url, null, reply);
    }
    this.released(conn);
  }


  // An error of the connection or of the server: the slot is free, then the error is retried or surfaced.
  private errored(call: PendingCall, conn: PoolConnection, error: any): void {
    conn.inflight--;
    call.sent = false;
    if (!call.settled) {
      this.failed(call, conn, error);
    }
    this.released(conn);
  }


  // A slot became free: a retired connection left without calls closes now, a stale one is retired, and a connection
  // left without calls is marked as used now. The line of its key takes the slot.
  private released(conn: PoolConnection): void {
    if (conn.retired) {
      if (conn.inflight === 0) {
        this.closeRetired(conn);
      }
    } else if (this.stale(conn)) {
      this.retire(conn);
    } else if (conn.inflight === 0) {
      conn.lastUse = Date.now();
      this.armIdle(conn);
    }
    this.dispatch(conn.group);
  }


  // Arms the idle check of a connection when none is pending. Nothing runs without the option, nor on a connection
  // which ended: its calls may settle after it left the group, and close() would not reach a timer armed then. The
  // check is not cancelled when a call starts: it reads lastUse when it is due, so a busy connection arms no timer per
  // call, and a connection has at most one check pending. The idleTimeout is at most the longest timer (see the
  // constructor), so one check per period is one timer.
  private armIdle(conn: PoolConnection): void {
    if (this.idleTimeout === 0 || this.closed || conn.closeCode !== null || conn.idle !== null) {
      return;
    }
    conn.idle = callTimer(conn.lastUse + this.idleTimeout, (): void => this.idleDue(conn));
  }


  // The idle check: a connection with calls waits for the release of its last call, and a connection used since the
  // check was armed waits for the rest of its period.
  private idleDue(conn: PoolConnection): void {
    conn.idle = null;
    if (this.closed || conn.closeCode !== null || conn.inflight > 0) {
      return;
    }
    if (Date.now() < conn.lastUse + this.idleTimeout) {
      this.armIdle(conn);
      return;
    }
    conn.group.remove(conn);
    conn.client.disconnect();
  }


  // Whether a connection holds the identity of an earlier generation: its token belongs to a session the client left.
  private stale(conn: PoolConnection): boolean {
    return this.source !== null && conn.generation !== null && conn.generation !== this.source.generation();
  }


  // Whether the client the tokens come from is connecting: its identity is not confirmed, and a resend would wait for a
  // connect which may bring another identity. A call lost then is not resent. A disconnected client refuses the resend
  // at once, as before, so it is not in this case.
  private unconfirmed(): boolean {
    return this.source !== null && this.source.connecting();
  }


  // The error of a call on a connection. A connection the server closed for message size settles its calls without
  // a resend: the failure is the same on every attempt. So does a connection closed as unauthorized, or retired, since
  // its identity is gone: the call would be sent under the same identity. The same holds for a connection whose identity
  // moved on while the call was in flight, and for a loss while the client is not connected, since the identity of a
  // resend is then unknown. A lost connection or a later refusal is retried; a server error may move the call by
  // reroute, and the later signal is retried as the main client does.
  private failed(call: PendingCall, conn: PoolConnection, error: any): void {
    const code: number | undefined = error?.code;
    if (code === errorCodes.connectionClosed || code === errorCodes.clientDisconnected) {
      if (conn.closeCode === disconnectedCodes.messageSizeLimit
        || conn.closeCode === disconnectedCodes.unauthorized
        || conn.retired
        || this.stale(conn)
        || this.unconfirmed()) {
        this.finish(call, conn.url, error, null);
        return;
      }
      this.retry(call, conn.url, error, 0);
      return;
    }
    if (typeof code === 'number' && code >= serverErrorFloor) {
      if (call.resends < this.retries && this.rerouteHook !== undefined) {
        let key: string | undefined;
        try {
          key = this.rerouteHook(error);
        } catch (e) {
          this.fail(call, e);
          return;
        }
        if (key !== undefined && key !== null) {
          call.key = key;
          this.retry(call, conn.url, error, 0);
          return;
        }
      }
      if (retrySignal(error) === 'later') {
        const delay: number = laterResendDelay(call.laterResends);
        call.laterResends++;
        this.retry(call, conn.url, error, delay);
        return;
      }
    }
    this.finish(call, conn.url, error, null);
  }


  // Sends the call again, after delay milliseconds, while its resends and its deadline allow; else surfaces the error.
  private retry(call: PendingCall, endpoint: string, error: any, delay: number): void {
    if (call.resends >= this.retries || call.deadline <= Date.now()) {
      this.finish(call, endpoint, error, null);
      return;
    }
    call.resends++;
    if (delay <= 0) {
      this.attempt(call, false);
      return;
    }
    call.resend = callTimer(Math.min(Date.now() + delay, call.deadline), (): void => {
      call.resend = null;
      // The deadline timer of the call may fire in the same tick: a call past its deadline is not sent again.
      if (call.deadline <= Date.now()) {
        this.expire(call);
        return;
      }
      this.attempt(call, false);
    });
  }


  // The outcome of the attempts: the redirect hook may move its bytes to a URL, which is fetched within the deadline.
  private finish(call: PendingCall, endpoint: string, error: any | null, reply: RpcResult | null): void {
    let redirect: RpcPoolRedirect | undefined;
    if (this.redirectHook !== undefined) {
      try {
        redirect = this.redirectHook(reply, error);
      } catch (e) {
        this.fail(call, e);
        return;
      }
    }
    if (redirect !== undefined && redirect !== null) {
      this.fetchInto(call, endpoint, redirect);
      return;
    }
    if (error === null && reply !== null) {
      this.succeed(call, reply);
      return;
    }
    this.fail(call, error);
  }


  // Fetches a redirect for a call, once a redirect slot is free. The call settles with the bytes or with the failure.
  private fetchInto(call: PendingCall, endpoint: string, redirect: RpcPoolRedirect): void {
    // A fetch which gets a free slot waits for nothing: it is not counted, and a zero maxQueued does not refuse it. A fetch
    // which would wait is counted in the pool-wide queued, like a call waiting for a connection, and refused beyond it.
    const waits: boolean = this.redirects.full;
    if (waits && this.queued >= this.maxQueued) {
      this.fail(call, poolError(errorCodes.limitExceeded, 'too many redirects waiting for a fetch slot'));
      return;
    }
    const controller: AbortController = new AbortController();
    call.controller = controller;
    if (waits) {
      this.count(call);
    }
    this.redirects.acquire(controller.signal).then((release: () => void): void => {
      this.uncount(call);
      fetchRedirect(this.fetchOf(), endpoint, redirect, controller.signal).then(
        (data: Uint8Array): void => {
          release();
          this.succeed(call, { data: data, cached: false });
        },
        (error: any): void => {
          release();
          this.fail(call, error);
        }
      );
    }, (error: any): void => {
      // An abort of the call settled it already; a full gate of waiting fetches fails it here.
      this.fail(call, error);
    });
  }


  // The deadline of a call passed: the call settles, and a redirect fetch in progress is cancelled. Its slot on the
  // connection is freed when the reply or the error of the connection arrives.
  private expire(call: PendingCall): void {
    this.fail(call, poolError(errorCodes.timeout, 'timeout'));
  }


  // The signal of a call aborted: the call settles, and the server is not told.
  private abort(call: PendingCall): void {
    this.fail(call, poolError(errorCodes.cancelled, 'cancelled'));
  }


  private succeed(call: PendingCall, reply: RpcResult): void {
    if (this.settle(call)) {
      call.resolve(reply);
    }
  }


  private fail(call: PendingCall, error: any): void {
    if (this.settle(call)) {
      call.reject(error);
    }
  }


  // Settles a call: no timer, no signal listener, not waiting, and a redirect fetch is cancelled. False when it settled.
  // A call waiting in a line leaves it here, in O(1), so a settled call is retained by no line.
  private settle(call: PendingCall): boolean {
    if (call.settled) {
      return false;
    }
    call.settled = true;
    this.calls.delete(call);
    call.timer?.clear();
    call.timer = null;
    call.resend?.clear();
    call.resend = null;
    call.detach();
    call.controller?.abort();
    this.uncount(call);
    call.data = noData;
    return true;
  }


  // The token source changed: each group takes one pass, which places its calls or fails them. The cost is per group.
  private dispatchAll(): void {
    for (const group of this.groups.values()) {
      this.dispatch(group);
    }
  }


  // Places the waiting calls of a group on its connections, first come first served. A pass is started for the group
  // whose room changed, and only that group is walked: a call's placement depends on its group alone, so the pass stops
  // at the first call which cannot be placed. An event during a pass (it may open or end a connection) marks its group
  // for one more pass, which the running pass takes after its own.
  private dispatch(group: RpcPoolEndpoint): void {
    // A release with no waiting call has nothing to place: it returns before any allocation.
    if (this.closed || group.head() === null) {
      return;
    }
    this.dirty.add(group);
    if (this.dispatching) {
      return;
    }
    this.dispatching = true;
    try {
      while (this.dirty.size > 0) {
        const next: RpcPoolEndpoint | undefined = this.dirty.values().next().value;
        if (next === undefined) {
          break;
        }
        this.dirty.delete(next);
        this.drain(next);
      }
    } finally {
      this.dispatching = false;
    }
  }


  // Places the calls at the head of a group's line while the group has room for them. A call the placement fails is
  // failed with its error, as a call which cannot open its connection. A settled call is no longer in the line.
  private drain(group: RpcPoolEndpoint): void {
    let head: PendingCall | null = group.head();
    while (head !== null) {
      try {
        if (!this.assign(head, group)) {
          return;
        }
      } catch (e) {
        this.uncount(head);
        this.fail(head, e);
        head = group.head();
        continue;
      }
      this.uncount(head);
      head = group.head();
    }
  }
}


// The token source of tokenFrom, or null without it. The client must be a Centrifuge, and the connection of the pool
// holds no token of its own: the token is the client's.
function tokenSourceFrom(client: Centrifuge | undefined, connection: Partial<Options>): TokenSource | null {
  if (client === undefined) {
    return null;
  }
  if (!(client instanceof Centrifuge)) {
    throw poolError(errorCodes.badConfiguration, 'tokenFrom must be a Centrifuge client');
  }
  if (connection.token !== undefined || connection.getToken !== undefined) {
    throw poolError(errorCodes.badConfiguration, 'tokenFrom is exclusive with connection.token and connection.getToken');
  }
  return tokenSourceOf(client);
}


// The options of a client a pool passes on: only the names of connectionOptionNames are taken.
function connectionOptionsOf(connection: Partial<Options> | undefined): Partial<Options> {
  const taken: Record<string, any> = {};
  if (connection === undefined) {
    return taken;
  }
  for (const name of Object.keys(connection)) {
    if (name === 'tokenExchange') {
      throw poolError(errorCodes.badConfiguration, 'tokenExchange is not accepted by the rpc pool: pass the client which owns the token as tokenFrom');
    }
    if ((connectionOptionNames as readonly string[]).indexOf(name) < 0) {
      throw poolError(errorCodes.badConfiguration, 'connection option ' + name + ' is not accepted by the rpc pool');
    }
    taken[name] = (connection as Record<string, any>)[name];
  }
  return taken;
}


// An integer option of the pool, with its default when unset; a value out of range is a configuration error.
function integerOption(name: string, value: number | undefined, fallback: number, min: number): number {
  const actual: number = value ?? fallback;
  if (!Number.isInteger(actual) || actual < min) {
    throw poolError(errorCodes.badConfiguration, name + ' must be an integer of at least ' + min);
  }
  return actual;
}


// The endpoints a resolver answered, as a list: an empty answer is a configuration error of the application.
function endpointList(value: string | string[] | undefined | null): string[] {
  const urls: string[] = typeof value === 'string' ? [value] : (Array.isArray(value) ? value : []);
  const usable: string[] = urls.filter((url: string): boolean => typeof url === 'string' && url !== '');
  if (usable.length === 0) {
    throw poolError(errorCodes.badConfiguration, 'the endpoint resolver gave no endpoint for the key');
  }
  return usable;
}


// Whether a header set has the name, names being case-insensitive.
function hasHeader(headers: Record<string, string>, name: string): boolean {
  const lower: string = name.toLowerCase();
  return Object.keys(headers).some((key: string): boolean => key.toLowerCase() === lower);
}
