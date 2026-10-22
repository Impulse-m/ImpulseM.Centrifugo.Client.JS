import { Centrifuge } from './centrifuge';
import { CentrifugeProtobuf } from './protobuf_client';
import { disconnectedCodes, errorCodes } from './codes';
import { callTimer, maxTimerDelay } from './call_timer';
import { presentedTokenOf } from './client_host';
import { laterResendDelay, maxLaterResends, retrySignal } from './rpc_tracker';
import { RpcPoolEndpoint, RpcPoolLine } from './rpc_pool_endpoint';
import type { PendingCall, PoolConnection } from './rpc_pool_endpoint';
import { fetchRedirect, RedirectGate } from './rpc_pool_redirect';
import { poolError } from './rpc_pool_error';
import { tokenSourceOf } from './token_source';
import type { MainListener, MainState, SourcedToken, TokenSource } from './token_source';
import { UnauthorizedError } from './unauthorized_error';
import { backoff } from './utils';
import type { Options, RpcOptions, RpcResult } from './types';

// The connection options a pool passes on to its clients. The type of RpcPoolOptions.connection and the check of
// connectionOptionsOf both come from this list, so they cannot disagree.
const connectionOptionNames = [
  'token', 'getToken', 'headers', 'name', 'version', 'timeout', 'websocket',
  'minReconnectDelay', 'maxReconnectDelay', 'maxServerPingDelay', 'debug',
] as const;
type ConnectionOptionName = typeof connectionOptionNames[number];

/** The options of the pool's clients a pool accepts: nothing else of the client options is taken. */
export interface RpcPoolOptions {
  /**
   * The client which owns the access token of the pool's connections. The pool lends the client's confirmed token to each
   * connection, and renews it through that client when the token is refused or expires: the pool runs no exchange and
   * calls no getter of its own. Exclusive with connection.token and connection.getToken; connection.tokenExchange is
   * refused. It works with every token mode of the client, and needs no change of the client. The token is handed over
   * unchanged, so a fleet which checks it must accept it as it is.
   */
  tokenFrom?: Centrifuge;
  /**
   * Endpoints of an endpoint key. '' is the key of a call without one. The key is whatever the server answered the
   * application (a cluster, a region); the pool does not interpret it. Resolved once per key, until close(); a resolution
   * which fails is resolved again by the next call. Each key keeps its line and its state for the life of the pool, so
   * keys should be a bounded set (clusters, regions), not one per object. Connections belong to the endpoint URL, so keys
   * which resolve to one URL share its connections.
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
   * ones waiting in a line for a connection. Beyond it rpc() rejects with limitExceeded. Default 64. 0 is valid: then a
   * call waits for no slot, and a connection is opened only for the call which needs it. A resend of a call already
   * admitted (lost connection, later, move) is queued beyond this bound, and is bounded by retries instead. The redirect
   * fetches waiting for a slot are bounded by it too.
   */
  maxQueued?: number;
  /**
   * Closes a connection without calls for this long, in milliseconds, at most 2147483647 (the longest timer). 0 keeps it
   * open, and then close() is the only way to release it. With closeEndedSession, a connection of a session the main
   * client ended is closed at once, whatever the idle timeout. Default 60000.
   */
  idleTimeout?: number;
  /**
   * Closes the idle connections of a session the main client ended at once, when the client ends it, instead of at the
   * idle timeout or the next call. Default true. Set false to keep them until the idle timeout or the next call.
   */
  closeEndedSession?: boolean;
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
  /** Redirect fetches in flight at most, over the pool, from 1 to 16 (a value above 16 throws badConfiguration). Default 4. */
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
// The most redirect fetches in flight over a pool: each may hold a body of maxBytes, see fetchRedirect.
const maxRedirectsInflightCeiling: number = 16;
// The reconnect delays of a client when none is set: the defaults of centrifuge.ts, which time the hold of opens.
const defaultMinReconnectDelay: number = 500;
const defaultMaxReconnectDelay: number = 20000;
// Codes from this one up are the server's errors; the ones below are the client's own, see errorCodes.
const serverErrorFloor: number = 100;
const noop: () => void = (): void => {};
const noData: Uint8Array = new Uint8Array(0);

// What placing a call did: sent on a connection, opened a connection and left the call in its line, left it waiting, or
// settled it with an error.
type Placement = 'placed' | 'opened' | 'wait' | 'failed';


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
  private readonly closeEndedSession: boolean;
  private readonly retries: number;
  private readonly minReconnectDelay: number;
  private readonly maxReconnectDelay: number;
  private readonly redirects: RedirectGate;
  // The token source of tokenFrom, null without it: a pool which takes no token from a client holds none.
  private readonly source: TokenSource | null;
  // The pool as the source's listener: the events of the main client drain the keys with waiting calls.
  private readonly mainListener: MainListener = {
    onMainEvent: (): void => {
      this.mainEvent();
    },
  };
  // The endpoint lookups of the keys: pending while a resolution runs, and kept after it resolved.
  private readonly resolved: Map<string, Promise<RpcPoolEndpoint[]>> = new Map<string, Promise<RpcPoolEndpoint[]>>();
  // The endpoints of the resolved keys.
  private readonly keyEndpoints: Map<string, RpcPoolEndpoint[]> = new Map<string, RpcPoolEndpoint[]>();
  // The endpoints by URL, which own the connections.
  private readonly endpoints: Map<string, RpcPoolEndpoint> = new Map<string, RpcPoolEndpoint>();
  // The line of each key, made on the first call which waits for one.
  private readonly lines: Map<string, RpcPoolLine> = new Map<string, RpcPoolLine>();
  // The keys with calls in their line. While there is one, the source listens to the pool.
  private readonly waitingKeys: Set<string> = new Set<string>();
  // The connections retired while they carry calls: they close when their last call settles, see released.
  private readonly retiring: Set<PoolConnection> = new Set<PoolConnection>();
  // The calls not settled yet, so that close() settles them all.
  private readonly calls: Set<PendingCall> = new Set<PendingCall>();
  // The keys whose line may place a call now. A pass takes them one by one, see pass.
  private readonly dirtyKeys: Set<string> = new Set<string>();
  // The calls counted for maxQueued: the ones waiting for a key to resolve, and the ones in a line.
  private queued: number = 0;
  private readonly errorListeners: Array<(ctx: { endpoint: string; error: any }) => void> = [];
  private closed: boolean = false;
  // True while a pass runs: an event inside it marks its key, and the pass takes the key after its own work.
  private busy: boolean = false;
  // The key lines a pass looked at, for the tests which bound the work of an event.
  private drainedKeys: number = 0;


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
    this.closeEndedSession = options.closeEndedSession !== false;
    if (this.idleTimeout > maxTimerDelay) {
      throw poolError(errorCodes.badConfiguration, `idleTimeout must be at most ${maxTimerDelay} milliseconds`);
    }
    // Every call deadline is a timer of this span, so a longer one would fire at once.
    if (this.timeout > maxTimerDelay) {
      throw poolError(errorCodes.badConfiguration, `connection.timeout must be at most ${maxTimerDelay} milliseconds`);
    }
    this.minReconnectDelay = this.connectionOptions.minReconnectDelay ?? defaultMinReconnectDelay;
    this.maxReconnectDelay = this.connectionOptions.maxReconnectDelay ?? defaultMaxReconnectDelay;
    // The hold of opens after a close is a timer of these spans.
    if (this.minReconnectDelay > maxTimerDelay || this.maxReconnectDelay > maxTimerDelay) {
      throw poolError(errorCodes.badConfiguration, `connection.minReconnectDelay and connection.maxReconnectDelay must be at most ${maxTimerDelay} milliseconds`);
    }
    this.retries = integerOption('retries', options.retries, maxLaterResends, 0);
    this.redirects = new RedirectGate(integerOption('maxRedirectsInflight', options.maxRedirectsInflight, defaultMaxRedirectsInflight, 1, maxRedirectsInflightCeiling));
    this.rerouteHook = options.reroute;
    this.redirectHook = options.redirect;
    this.fetchOption = options.fetch;
    // Last: a pool which fails a check above leaves no listener on the client it names. The source listens to the pool only
    // while a call waits in a key line.
    this.source = tokenSourceFrom(options.tokenFrom, this.connectionOptions);
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
      const source: TokenSource | null = this.source;
      let state: MainState = 'connected';
      if (source !== null) {
        // A listener the application removed is put back while a request waits for an event of the client, since only
        // that event answers the request.
        source.reattachWaiting();
        state = source.mainState();
        if (state === 'ended') {
          reject(poolError(errorCodes.clientDisconnected, 'unauthorized'));
          return;
        }
        if (state === 'disconnected') {
          reject(poolError(errorCodes.clientDisconnected, 'client disconnected'));
          return;
        }
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
        // A call made while the main client is connected is bound to its identity now; one made in the connecting window
        // is bound at its first placement after the connect.
        generation: source !== null && state === 'connected' ? source.generation() : null,
        rerouted: false,
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
      this.pass((): void => {
        this.route(call, true);
      });
    });
  }


  /** The state of the pool: see RpcPoolStats. */
  stats(): RpcPoolStats {
    let connections: number = 0;
    let inflight: number = 0;
    for (const endpoint of this.endpoints.values()) {
      for (const conn of endpoint.all()) {
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
    this.source?.unlisten(this.mainListener);
    this.source?.release(this.mainListener);
    for (const endpoint of this.endpoints.values()) {
      endpoint.hold?.clear();
      endpoint.hold = null;
      for (const conn of endpoint.copy()) {
        this.source?.forget(conn);
        this.shut(conn);
      }
    }
    for (const conn of Array.from(this.retiring)) {
      this.source?.forget(conn);
      this.shut(conn);
    }
    this.retiring.clear();
    this.dirtyKeys.clear();
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


  // Starts the resolution of a key, which keeps its endpoints once resolved and forgets a failed one.
  private startResolution(key: string): Promise<RpcPoolEndpoint[]> {
    // A resolver that throws or rejects rejects the lookup: the calls of the key reject with its error.
    const endpoints: Promise<RpcPoolEndpoint[]> = Promise.resolve()
      .then((): string | string[] | Promise<string | string[]> => this.resolver(key))
      .then((value: string | string[]): RpcPoolEndpoint[] => endpointList(value).map((url: string): RpcPoolEndpoint => this.endpointOf(url)));
    this.resolved.set(key, endpoints);
    endpoints.then((list: RpcPoolEndpoint[]): void => {
      this.keyEndpoints.set(key, list);
    }, (): void => {
      this.resolved.delete(key);
    });
    return endpoints;
  }


  // The endpoint of a URL, made the first time the URL is used.
  private endpointOf(url: string): RpcPoolEndpoint {
    let endpoint: RpcPoolEndpoint | undefined = this.endpoints.get(url);
    if (endpoint === undefined) {
      endpoint = new RpcPoolEndpoint(url);
      this.endpoints.set(url, endpoint);
    }
    return endpoint;
  }


  // The line of a key, made with its endpoints the first time the key has a waiting call.
  private lineOf(key: string, endpoints: RpcPoolEndpoint[]): RpcPoolLine {
    let line: RpcPoolLine | undefined = this.lines.get(key);
    if (line === undefined) {
      line = new RpcPoolLine(key, endpoints);
      this.lines.set(key, line);
    }
    return line;
  }


  // Routes a call to its key. A key which is resolved admits or places the call at once. A key
  // being resolved lets a fresh call wait behind the bound, and the call is judged again when the key resolves. The first
  // call of an unresolved key starts its resolution and is admitted at any bound.
  private route(call: PendingCall, fresh: boolean): void {
    const endpoints: RpcPoolEndpoint[] | undefined = this.keyEndpoints.get(call.key);
    if (endpoints !== undefined) {
      this.admit(call, fresh, endpoints);
      return;
    }
    const pending: Promise<RpcPoolEndpoint[]> | undefined = this.resolved.get(call.key);
    if (pending !== undefined) {
      if (fresh && this.queued >= this.maxQueued) {
        this.fail(call, poolError(errorCodes.limitExceeded, 'too many rpc calls waiting for a connection'));
        return;
      }
      this.count(call);
      this.judgeWhenResolved(call, fresh, pending);
      return;
    }
    this.count(call);
    this.judgeWhenResolved(call, fresh, this.startResolution(call.key));
  }


  // Judges a call once its key resolved: it is uncounted, and admitted again as a call of the key.
  private judgeWhenResolved(call: PendingCall, fresh: boolean, pending: Promise<RpcPoolEndpoint[]>): void {
    pending.then((): void => {
      this.pass((): void => {
        this.judge(call, fresh);
      });
    }, (error: any): void => {
      this.fail(call, error);
    });
  }


  // The judgement of a call of a key which resolved: it counts again, as a fresh call of the key.
  private judge(call: PendingCall, fresh: boolean): void {
    if (call.settled) {
      return;
    }
    this.uncount(call);
    const endpoints: RpcPoolEndpoint[] | undefined = this.keyEndpoints.get(call.key);
    if (endpoints === undefined) {
      this.fail(call, poolError(errorCodes.badConfiguration, 'the endpoint resolver gave no endpoint for the key'));
      return;
    }
    this.admit(call, fresh, endpoints);
  }


  // Admits a call of a resolved key. A call with an empty line is placed at once; one that waits joins the line under
  // the bound. A resend is checked before it joins a line, so one that can never be placed does not wait behind others.
  private admit(call: PendingCall, fresh: boolean, endpoints: RpcPoolEndpoint[]): void {
    const front: boolean = call.rerouted;
    call.rerouted = false;
    if (!fresh && !this.entryOk(call, endpoints)) {
      return;
    }
    const line: RpcPoolLine = this.lineOf(call.key, endpoints);
    if (line.isEmpty()) {
      const placement: Placement = this.place(call, endpoints);
      if (placement === 'placed' || placement === 'failed') {
        return;
      }
      if (placement === 'opened') {
        // The call is the opener of an endpoint: admitted at any bound, it waits for the connect in its line.
        this.join(call, line, front);
        return;
      }
    }
    if (fresh && this.queued >= this.maxQueued && !this.mayOpen(endpoints)) {
      this.fail(call, poolError(errorCodes.limitExceeded, 'too many rpc calls waiting for a connection'));
      return;
    }
    this.join(call, line, front);
  }


  // Puts a call in its line, counted for maxQueued. A rerouted call goes to the front; a key which gains its first call
  // is told to the source and to its endpoints.
  private join(call: PendingCall, line: RpcPoolLine, front: boolean): void {
    this.count(call);
    const wasEmpty: boolean = line.isEmpty();
    if (front) {
      line.addFirst(call);
    } else {
      line.enqueue(call);
    }
    if (wasEmpty) {
      this.keyWaits(line);
    }
    this.dirtyKeys.add(line.key);
  }


  // A key gains its first waiting call: the pool and its endpoints keep it, and the source listens to the pool once.
  private keyWaits(line: RpcPoolLine): void {
    this.waitingKeys.add(line.key);
    for (const endpoint of line.endpoints) {
      endpoint.waitingKeys.add(line.key);
    }
    if (this.waitingKeys.size === 1) {
      this.source?.listen(this.mainListener);
    }
  }


  // A key loses its last waiting call: the pool and its endpoints forget it, and the source stops listening to a pool with none.
  private keyIdle(line: RpcPoolLine): void {
    this.waitingKeys.delete(line.key);
    for (const endpoint of line.endpoints) {
      endpoint.waitingKeys.delete(line.key);
    }
    if (this.waitingKeys.size === 0) {
      this.source?.unlisten(this.mainListener);
    }
  }


  // Counts a call in queued, the bound of the waiting calls.
  private count(call: PendingCall): void {
    call.queued = true;
    this.queued++;
  }


  // Stops counting a call in queued: it left its line, it holds a slot, it failed or it settled.
  private uncount(call: PendingCall): void {
    this.leave(call);
    if (call.queued) {
      call.queued = false;
      this.queued--;
    }
  }


  // Takes a call out of its line, in O(1). A key which empties is forgotten.
  private leave(call: PendingCall): void {
    const line: RpcPoolLine | null = call.line;
    if (line === null) {
      return;
    }
    line.dequeue(call);
    if (line.isEmpty()) {
      this.keyIdle(line);
    }
  }


  // The entry check of a call against the main client: when the main client has ended or is disconnected, the call is
  // bound to another identity, or the token source is spent for its connection, a call that can never be placed settles
  // now, with that case's error. False when it settled.
  private entryOk(call: PendingCall, endpoints: RpcPoolEndpoint[]): boolean {
    const source: TokenSource | null = this.source;
    if (source === null) {
      return true;
    }
    const state: MainState = source.mainState();
    if (state === 'ended') {
      this.fail(call, poolError(errorCodes.clientDisconnected, 'unauthorized'));
      return false;
    }
    if (state === 'disconnected') {
      this.fail(call, poolError(errorCodes.clientDisconnected, 'client disconnected'));
      return false;
    }
    if (call.generation !== null && call.generation !== source.generation()) {
      this.fail(call, poolError(errorCodes.clientDisconnected, 'identity changed'));
      return false;
    }
    if (call.generation !== null && source.spent() && !this.liveOf(endpoints, call.generation)) {
      this.fail(call, poolError(errorCodes.clientDisconnected, 'token refused'));
      return false;
    }
    return true;
  }


  // Whether a connection of the endpoints is live with the generation given: the calls bound to it may be placed there.
  private liveOf(endpoints: RpcPoolEndpoint[], generation: number): boolean {
    for (const endpoint of endpoints) {
      for (const conn of endpoint.all()) {
        if (conn.live && conn.generation === generation) {
          return true;
        }
      }
    }
    return false;
  }


  // Places a call of a key whose line is empty or whose line is being drained. A placed call takes a slot of a
  // live connection with room; an open connection is started for the call when its endpoint may open, and the call waits
  // for it. The call is bound to the identity of the main client at its first placement that finds it connected.
  private place(call: PendingCall, endpoints: RpcPoolEndpoint[]): Placement {
    const source: TokenSource | null = this.source;
    if (source !== null) {
      if (!this.entryOk(call, endpoints)) {
        return 'failed';
      }
      const state: MainState = source.mainState();
      if (call.generation === null) {
        if (state !== 'connected') {
          return 'wait';
        }
        call.generation = source.generation();
      }
      if (state === 'connected') {
        this.retireStale(endpoints, source.generation());
      }
    }
    const generation: number | null = source === null ? null : call.generation;
    let best: PoolConnection | null = null;
    for (const endpoint of endpoints) {
      for (const conn of endpoint.all()) {
        if (conn.live
          && conn.inflight < this.maxInflightPerConnection
          && (generation === null || conn.generation === generation)
          && (best === null || conn.inflight < best.inflight)) {
          best = conn;
        }
      }
    }
    if (best !== null) {
      this.send(call, best);
      return 'placed';
    }
    if (source !== null && (source.mainState() !== 'connected' || !source.canLend())) {
      // A spent source places nothing new: the call is refused unless a live connection of its generation takes it later.
      if (call.generation !== null && source.spent() && !this.liveOf(endpoints, call.generation)) {
        this.fail(call, poolError(errorCodes.clientDisconnected, 'token refused'));
        return 'failed';
      }
      return 'wait';
    }
    for (const endpoint of endpoints) {
      if (this.openable(endpoint)) {
        this.open(endpoint);
        return 'opened';
      }
    }
    return 'wait';
  }


  // Whether a call of the key may open a connection now: an endpoint may open, and the main client lends a token to it.
  private mayOpen(endpoints: RpcPoolEndpoint[]): boolean {
    if (this.source !== null && (this.source.mainState() !== 'connected' || !this.source.canLend())) {
      return false;
    }
    for (const endpoint of endpoints) {
      if (this.openable(endpoint)) {
        return true;
      }
    }
    return false;
  }


  // Whether an endpoint may open a connection: fewer than the maximum, no connect in flight, and not held after a close.
  private openable(endpoint: RpcPoolEndpoint): boolean {
    return endpoint.count() < this.maxConnectionsPerEndpoint && !endpoint.connecting() && endpoint.hold === null;
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


  // Opens a connection to an endpoint. The connection joins its endpoint before connect() is called, so
  // a terminal disconnect during connect() is handled as any other. Its connect is in flight until it connects.
  private open(endpoint: RpcPoolEndpoint): void {
    const source: TokenSource | null = this.source;
    // The token getter of the client is bound to the connection below. The getter runs only from connect(), which is
    // called after the connection exists.
    const options: Partial<Options> = source === null
      ? this.connectionOptions
      : { ...this.connectionOptions, getToken: (): Promise<string> => this.tokenOf(conn, source) };
    const client: CentrifugeProtobuf = new CentrifugeProtobuf(endpoint.url, options);
    const conn: PoolConnection = {
      endpoint: endpoint,
      client: client,
      inflight: 0,
      connecting: true,
      live: false,
      closeCode: null,
      lastUse: Date.now(),
      idle: null,
      generation: null,
      presented: '',
      retired: false,
    };
    client.on('connecting', (): void => {
      this.pass((): void => {
        this.transportLost(conn);
      });
    });
    client.on('connected', (ctx: any): void => {
      this.pass((): void => {
        this.connected(conn, ctx);
      });
    });
    client.on('disconnected', (ctx: any): void => {
      this.pass((): void => {
        this.lost(conn, ctx);
      });
    });
    this.attach(conn);
    this.armIdle(conn);
    try {
      client.connect();
    } catch (error: any) {
      // The connection carries no call, so nothing is lost: the calls waiting stay in their line, and the endpoint holds
      // its opens after the error.
      this.detach(conn);
      conn.retired = true;
      conn.closeCode = disconnectedCodes.disconnectCalled;
      conn.idle?.clear();
      conn.idle = null;
      this.source?.forget(conn);
      this.emitError(endpoint.url, error);
      this.holdEndpoint(endpoint);
    }
  }


  // The token the connection presented at its last connect attempt that the client did not abandon. The client clears its
  // token on a state-invalidated disconnect, before the reconnect that asks for a new one, so an empty token of the client
  // keeps the one presented before. A connect that carries a token reads it here, at the request or at its verified connect.
  private presentedOf(conn: PoolConnection): string {
    const current: string = presentedTokenOf(conn.client);
    if (current !== '') {
      conn.presented = current;
    }
    return conn.presented;
  }


  // The token of a connection from the source. A request of a connection stamped with an earlier generation retires it,
  // and is answered unauthorized without asking the source. A token delivered of another generation than the stamp
  // retires the connection too. The request names the token the connection presented last: a token answered for an
  // attempt the client dropped is not one it presented.
  private tokenOf(conn: PoolConnection, source: TokenSource): Promise<string> {
    if (conn.generation !== null && conn.generation !== source.generation()) {
      this.retire(conn);
      return Promise.reject(new UnauthorizedError('unauthorized'));
    }
    return source.token(conn, this.presentedOf(conn)).then((sourced: SourcedToken): string => {
      if (conn.generation === null) {
        conn.generation = sourced.generation;
      } else if (conn.generation !== sourced.generation) {
        this.retire(conn);
        throw new UnauthorizedError('unauthorized');
      }
      return sourced.token;
    });
  }


  // The connection's transport was lost and its client reconnects: the connect is in flight again, and the connection takes
  // no call until it connects. A stale connection is retired now, before its client reuses the token it stored.
  private transportLost(conn: PoolConnection): void {
    conn.connecting = true;
    conn.live = false;
    if (this.stale(conn)) {
      this.retire(conn);
    }
  }


  // The connection connected. With tokenFrom its identity is verified against the generation and the
  // pair the source recorded: a connection which is not of the current generation is retired silently, and one whose
  // server confirmed another identity is retired and reported, with its token marked unusable.
  private connected(conn: PoolConnection, ctx: any): void {
    conn.connecting = false;
    if (conn.retired) {
      return;
    }
    const source: TokenSource | null = this.source;
    if (source === null) {
      this.goLive(conn);
      return;
    }
    if (!source.recordedPair() || conn.generation !== source.generation()) {
      this.retire(conn);
      return;
    }
    // The connect is verified: the token it carried is the one it presented, and it is the one found unusable below.
    const presented: string = this.presentedOf(conn);
    if (!source.identityMatches(ctx.user, ctx.authClass)) {
      this.retire(conn);
      source.unusable(presented);
      this.emitError(conn.endpoint.url, { code: disconnectedCodes.unauthorized, reason: 'identity mismatch' });
      return;
    }
    this.goLive(conn);
  }


  // A verified connection takes calls: its endpoint's connect is over, and its keys with waiting calls are placed.
  private goLive(conn: PoolConnection): void {
    conn.live = true;
    conn.endpoint.closes = 0;
    this.scheduleEndpoint(conn.endpoint);
  }


  // Takes a connection out of its endpoint: it takes no new call, and it does not count against the endpoint's connections.
  // It closes at once when it carries no call; else it waits in retiring, and closes when its last call settles, so its
  // calls complete under the identity they were sent with.
  private retire(conn: PoolConnection): void {
    if (conn.retired) {
      return;
    }
    conn.retired = true;
    conn.live = false;
    this.source?.forget(conn);
    this.detach(conn);
    if (conn.inflight > 0) {
      this.retiring.add(conn);
      return;
    }
    this.closeRetired(conn);
  }


  // Closes a retired connection. The close reports no error and holds no open: its code is disconnectCalled.
  private closeRetired(conn: PoolConnection): void {
    this.retiring.delete(conn);
    this.shut(conn);
  }


  // Retires the connections of the endpoints whose token belongs to another generation than the current one. Only the
  // endpoints of the key are looked at.
  private retireStale(endpoints: RpcPoolEndpoint[], generation: number): void {
    for (const endpoint of endpoints) {
      let stale: PoolConnection | null = endpoint.staleOf(generation);
      while (stale !== null) {
        this.retire(stale);
        stale = endpoint.staleOf(generation);
      }
    }
  }


  // Ends a connection's timers and its client. The teardown of a connection, shared by close, retirement and idle close.
  private shut(conn: PoolConnection): void {
    conn.idle?.clear();
    conn.idle = null;
    conn.client.disconnect();
  }


  // A connection ended for good. Retired and disconnectCalled ends report nothing; a message size close reports and holds
  // nothing; any other end reports and holds the endpoint's opens. Its calls go by the outcome rules of a lost call.
  private lost(conn: PoolConnection, ctx: any): void {
    conn.connecting = false;
    conn.live = false;
    conn.closeCode = ctx.code;
    conn.idle?.clear();
    conn.idle = null;
    const endpoint: RpcPoolEndpoint = conn.endpoint;
    this.detach(conn);
    this.retiring.delete(conn);
    this.source?.forget(conn);
    // A stale connection is retired at this, its next event: its end is no error, and holds no open.
    if (this.stale(conn)) {
      conn.retired = true;
    }
    if (!conn.retired && ctx.code !== disconnectedCodes.disconnectCalled) {
      this.emitError(endpoint.url, ctx);
      if (ctx.code !== disconnectedCodes.messageSizeLimit) {
        this.holdEndpoint(endpoint);
      }
    }
    // The end of a retired connection drains nothing: its endpoint took it out already.
    if (!conn.retired) {
      this.scheduleEndpoint(endpoint);
    }
  }


  // Reports an error of a connection to the listeners of the pool.
  private emitError(url: string, error: any): void {
    for (const listener of this.errorListeners) {
      listener({ endpoint: url, error: error });
    }
  }


  // Holds the opens of an endpoint after a terminal close: the hold lasts a backoff of the closes so far, and one timer
  // runs for it. The count resets at the next connected event.
  private holdEndpoint(endpoint: RpcPoolEndpoint): void {
    if (this.closed || endpoint.hold !== null) {
      return;
    }
    const delay: number = backoff(endpoint.closes, this.minReconnectDelay, this.maxReconnectDelay);
    endpoint.closes++;
    endpoint.hold = callTimer(Date.now() + delay, (): void => {
      endpoint.hold = null;
      this.pass((): void => {
        this.scheduleEndpoint(endpoint);
      });
    });
  }


  // The outcome of an attempt on a connection: the slot is free, then the reply settles the call unless it settled before.
  private replied(call: PendingCall, conn: PoolConnection, reply: RpcResult): void {
    this.pass((): void => {
      conn.inflight--;
      call.sent = false;
      if (!call.settled) {
        this.finish(call, conn.endpoint.url, null, reply);
      }
      this.released(conn);
    });
  }


  // An error of the connection or of the server: the slot is free, then the error is retried or surfaced.
  private errored(call: PendingCall, conn: PoolConnection, error: any): void {
    this.pass((): void => {
      conn.inflight--;
      call.sent = false;
      if (!call.settled) {
        this.failed(call, conn, error);
      }
      this.released(conn);
    });
  }


  // A slot became free: a retired connection left without calls closes now, a stale one is retired, and a
  // connection left without calls is marked as used now. The keys of its endpoint which wait take the slot.
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
    this.scheduleEndpoint(conn.endpoint);
  }


  // Arms the idle check of a connection when none is pending. Nothing runs without the option, nor on a connection that ended
  // or is retired. The check is not cancelled when a call starts: it reads lastUse when it is due, so a busy connection arms
  // no timer per call, and a connection has at most one check pending.
  private armIdle(conn: PoolConnection): void {
    if (this.idleTimeout === 0 || this.closed || conn.closeCode !== null || conn.retired || conn.idle !== null) {
      return;
    }
    conn.idle = callTimer(conn.lastUse + this.idleTimeout, (): void => {
      this.pass((): void => {
        this.idleDue(conn);
      });
    });
  }


  // The idle check: a connection with calls waits for the release of its last call, and a connection used since the check
  // was armed waits for the rest of its period, which the check re-arms for.
  private idleDue(conn: PoolConnection): void {
    conn.idle = null;
    if (this.closed || conn.closeCode !== null || conn.retired || conn.inflight > 0) {
      return;
    }
    if (Date.now() < conn.lastUse + this.idleTimeout) {
      this.armIdle(conn);
      return;
    }
    this.detach(conn);
    conn.live = false;
    this.shut(conn);
  }


  // Whether a connection holds the identity of an earlier generation: its token belongs to a session the main client left.
  private stale(conn: PoolConnection): boolean {
    return this.source !== null && conn.generation !== null && conn.generation !== this.source.generation();
  }


  // The error of a call on a connection. A connection the server closed for message size, one closed as
  // unauthorized, one retired or stale: the call settles without a resend. A lost connection is retried; a server error
  // may move the call by reroute, and the later signal is retried as the main client does.
  private failed(call: PendingCall, conn: PoolConnection, error: any): void {
    const url: string = conn.endpoint.url;
    const code: number | undefined = error?.code;
    if (code === errorCodes.connectionClosed || code === errorCodes.clientDisconnected) {
      if (conn.closeCode === disconnectedCodes.messageSizeLimit
        || conn.closeCode === disconnectedCodes.unauthorized
        || conn.retired
        || this.stale(conn)) {
        this.finish(call, url, error, null);
        return;
      }
      this.retry(call, url, error, 0);
      return;
    }
    if (typeof code === 'number' && code >= serverErrorFloor) {
      // The move is checked before the later signal: a reroute that maps the error wins over it, so the call goes to the
      // new key at once.
      if (call.resends < this.retries && call.deadline > Date.now() && this.rerouteHook !== undefined) {
        let key: string | undefined;
        try {
          key = this.rerouteHook(error);
        } catch (e) {
          this.fail(call, e);
          return;
        }
        if (key !== undefined && key !== null) {
          call.key = key;
          call.rerouted = true;
          this.retry(call, url, error, 0);
          return;
        }
      }
      if (retrySignal(error) === 'later') {
        const delay: number = laterResendDelay(call.laterResends);
        call.laterResends++;
        this.retry(call, url, error, delay);
        return;
      }
    }
    this.finish(call, url, error, null);
  }


  // Sends the call again, after delay milliseconds, while its resends and its deadline allow; else surfaces the error. A
  // resend is not bounded by maxQueued: it was admitted already.
  private retry(call: PendingCall, endpoint: string, error: any, delay: number): void {
    if (call.resends >= this.retries || call.deadline <= Date.now()) {
      this.finish(call, endpoint, error, null);
      return;
    }
    call.resends++;
    if (delay <= 0) {
      this.route(call, false);
      return;
    }
    call.resend = callTimer(Math.min(Date.now() + delay, call.deadline), (): void => {
      this.pass((): void => {
        call.resend = null;
        // The deadline timer of the call may fire in the same tick: a call past its deadline is not sent again.
        if (call.deadline <= Date.now()) {
          this.expire(call);
          return;
        }
        this.route(call, false);
      });
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


  // An event of the main client: the keys with waiting calls are placed or failed, and, with closeEndedSession, the
  // connections of an earlier generation are retired, so an idle one of an ended session is closed. No other key is looked at.
  private mainEvent(): void {
    this.pass((): void => {
      for (const key of this.waitingKeys) {
        this.dirtyKeys.add(key);
      }
      if (this.closeEndedSession && this.source !== null) {
        this.retireStale(Array.from(this.endpoints.values()), this.source.generation());
      }
    });
  }


  // A connection joins its endpoint, and the source holds the pool while one is held: the one place a connection joins.
  private attach(conn: PoolConnection): void {
    conn.endpoint.add(conn);
    this.syncHolding();
  }


  // A connection leaves its endpoint, and the source's hold follows: the one place a connection leaves.
  private detach(conn: PoolConnection): void {
    conn.endpoint.remove(conn);
    this.syncHolding();
  }


  // The source tells the pool of the events that end a session while the pool holds a connection: see TokenSource.hold.
  // Without closeEndedSession the pool is never told, so it is not kept by the source at all.
  private syncHolding(): void {
    if (this.source === null || !this.closeEndedSession) {
      return;
    }
    if (this.closed || !this.holdsConnection()) {
      this.source.release(this.mainListener);
    } else {
      this.source.hold(this.mainListener);
    }
  }


  // Whether a connection of any endpoint is held, idle or busy.
  private holdsConnection(): boolean {
    for (const endpoint of this.endpoints.values()) {
      if (endpoint.count() > 0) {
        return true;
      }
    }
    return false;
  }


  // An event of an endpoint: the keys with waiting calls on it are placed or failed.
  private scheduleEndpoint(endpoint: RpcPoolEndpoint): void {
    for (const key of endpoint.waitingKeys) {
      this.dirtyKeys.add(key);
    }
  }


  // Runs the work of an event, then the keys it marked. An event inside a pass marks its keys only: the pass takes them
  // after its own work, so a key line is never drained inside another drain of it.
  private pass(work: () => void): void {
    if (this.busy) {
      work();
      return;
    }
    this.busy = true;
    try {
      work();
      let key: string | undefined = this.nextDirty();
      while (key !== undefined) {
        this.dirtyKeys.delete(key);
        this.drainKey(key);
        key = this.nextDirty();
      }
    } finally {
      this.busy = false;
    }
  }


  // The first key marked for a drain, undefined when none is.
  private nextDirty(): string | undefined {
    const next: IteratorResult<string> = this.dirtyKeys.values().next();
    return next.done === true ? undefined : next.value;
  }


  // Places the calls at the head of a key's line while they can be placed. The line stops at the first call that waits or
  // opens a connection; a call which settles, or is placed, leaves the line.
  private drainKey(key: string): void {
    if (this.closed) {
      return;
    }
    this.drainedKeys++;
    const line: RpcPoolLine | undefined = this.lines.get(key);
    if (line === undefined) {
      return;
    }
    let head: PendingCall | null = line.head();
    while (head !== null) {
      const placement: Placement = this.place(head, line.endpoints);
      if (placement === 'wait' || placement === 'opened') {
        return;
      }
      this.uncount(head);
      head = line.head();
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


// An integer option of the pool, with its default when unset, and its bounds: a value out of them is a configuration error.
function integerOption(name: string, value: number | undefined, fallback: number, min: number, max?: number): number {
  const actual: number = value ?? fallback;
  if (!Number.isInteger(actual) || actual < min || (max !== undefined && actual > max)) {
    const bound: string = max === undefined ? ' of at least ' + min : ' from ' + min + ' to ' + max;
    throw poolError(errorCodes.badConfiguration, name + ' must be an integer' + bound);
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

