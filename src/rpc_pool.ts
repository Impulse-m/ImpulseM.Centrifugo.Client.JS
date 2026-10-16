import { ProtobufCentrifuge } from './protobuf';
import { disconnectedCodes, errorCodes } from './codes';
import { callTimer } from './call_timer';
import type { CallTimer } from './call_timer';
import { laterResendDelay, maxLaterResends, retrySignal } from './rpc_tracker';
import { poolError, RpcPoolEndpoint } from './rpc_pool_endpoint';
import type { PoolConnection } from './rpc_pool_endpoint';
import { fetchRedirect, RedirectGate } from './rpc_pool_redirect';
import type { Options, RpcOptions, RpcResult } from './types';

/** The options of a pool call's client a pool accepts: nothing else of the client options is taken. */
export interface RpcPoolOptions {
  /**
   * Endpoints of an endpoint key. '' is the key of a call without one. The key is whatever the server answered the
   * application (a cluster, a region); the pool does not interpret it. Resolved once per key, until close().
   */
  endpoints: (endpointKey: string) => string | string[] | Promise<string | string[]>;
  /**
   * Options of each connection: token, getToken, headers, name, version, timeout, websocket, minReconnectDelay,
   * maxReconnectDelay, maxServerPingDelay and debug. Any other option throws a badConfiguration error, instance in
   * particular: pool calls are plain calls, without an instance.
   */
  connection?: Partial<Pick<Options, 'token' | 'getToken' | 'headers' | 'name' | 'version' | 'timeout' | 'websocket' | 'minReconnectDelay' | 'maxReconnectDelay' | 'maxServerPingDelay' | 'debug'>>;
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
  /** Calls waiting for a slot at most, over all endpoints. Beyond it rpc() rejects with limitExceeded. Default 64. */
  maxQueued?: number;
  /** Closes a connection without calls for this long, in milliseconds. 0 keeps it open. Default 60000. */
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
  /** Deadline of the call across its resends and its redirect fetch, in milliseconds. Default connection.timeout. */
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

// A call of the pool from rpc() until it settles.
interface PendingCall {
  method: string;
  data: Uint8Array;
  headers: Record<string, string> | undefined;
  key: string;
  deadline: number;
  // Resends made so far, across the three causes: see failed and retry.
  resends: number;
  settled: boolean;
  // True while the call holds a slot of a connection: from its send until its reply or its error.
  sent: boolean;
  timer: CallTimer | null;
  resend: CallTimer | null;
  // Removes the abort listener of the signal, if the call has one.
  detach: () => void;
  // Aborted when the call settles, so a redirect fetch in progress is cancelled.
  controller: AbortController | null;
  resolve: (result: RpcResult) => void;
  reject: (error: any) => void;
}

// The connection options a pool passes on to its clients.
const connectionOptionNames: string[] = [
  'token', 'getToken', 'headers', 'name', 'version', 'timeout', 'websocket',
  'minReconnectDelay', 'maxReconnectDelay', 'maxServerPingDelay', 'debug',
];
// The reply timeout of a client when none is set. Keep it equal to the default timeout of centrifuge.ts (config).
const defaultTimeout: number = 5000;
const defaultMaxConnectionsPerEndpoint: number = 2;
const defaultMaxInflightPerConnection: number = 1;
const defaultMaxQueued: number = 64;
const defaultIdleTimeout: number = 60000;
const defaultMaxRedirectsInflight: number = 4;
// Codes from this one up are the server's errors; the ones below are the client's own, see errorCodes.
const serverErrorFloor: number = 100;
const noop: () => void = (): void => {};


/**
 * A pool of connections for calls which carry large payloads: several connections per endpoint, each with a window of
 * calls in flight, a bounded wait queue, and the server's moves (reroute, redirect) honoured. The calls are plain
 * requests: no message id and no instance, so they are retried only where the contract allows it, and their methods
 * must be idempotent. The pool speaks protobuf only, and opens connections on demand.
 */
export class RpcPool {
  private readonly resolver: (endpointKey: string) => string | string[] | Promise<string | string[]>;
  private readonly connectionOptions: Partial<Options>;
  private readonly fetchFunction: typeof fetch | undefined;
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
  private readonly resolved: Map<string, Promise<string[]>> = new Map();
  private readonly groups: Map<string, RpcPoolEndpoint> = new Map();
  // The calls not settled yet, so that close() settles them all.
  private readonly calls: Set<PendingCall> = new Set();
  // The calls waiting for a slot, first come first served over all endpoints.
  private readonly queue: PendingCall[] = [];
  private readonly errorListeners: Array<(ctx: { endpoint: string; error: any }) => void> = [];
  private closed: boolean = false;
  private dispatching: boolean = false;
  private dispatchAgain: boolean = false;


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
    this.retries = integerOption('retries', options.retries, maxLaterResends, 0);
    this.redirects = new RedirectGate(integerOption('maxRedirectsInflight', options.maxRedirectsInflight, defaultMaxRedirectsInflight, 1));
    this.rerouteHook = options.reroute;
    this.redirectHook = options.redirect;
    this.fetchFunction = options.fetch ?? (typeof fetch === 'function' ? fetch : undefined);
  }


  /**
   * Runs a call on a connection of its endpoint key. It takes a slot of a connection with room, opens one when the key
   * has none and may open one, or waits in the queue. Rejects with limitExceeded when the queue is full, with
   * timeout at the deadline, with cancelled when the signal aborts, and with the error of the server otherwise.
   */
  rpc(method: string, data: Uint8Array, opts?: RpcPoolCallOptions): Promise<RpcResult> {
    return new Promise<RpcResult>((resolve: (result: RpcResult) => void, reject: (error: any) => void): void => {
      if (this.closed) {
        reject(poolError(errorCodes.clientClosed, 'rpc pool is closed'));
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
        settled: false,
        sent: false,
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
    return { connections: connections, inflight: inflight, queued: this.queue.length, redirects: this.redirects.inflight };
  }


  /** Closes every connection. Waiting calls reject with cancelled, calls in flight with connectionClosed. */
  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    for (const group of this.groups.values()) {
      for (const conn of group.all()) {
        conn.idle?.clear();
        conn.idle = null;
        conn.client.disconnect();
      }
    }
    for (const call of Array.from(this.calls)) {
      if (!call.sent) {
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


  // The deadline span of a call in milliseconds: a positive finite number, the connection timeout when unset.
  private timeoutOf(opts?: RpcPoolCallOptions): number {
    const timeout: number = opts?.timeout ?? this.timeout;
    if (!Number.isFinite(timeout) || timeout <= 0) {
      throw poolError(errorCodes.badConfiguration, 'timeout must be a positive number of milliseconds');
    }
    return timeout;
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
  // is queued whatever the bound, since it was admitted already.
  private attempt(call: PendingCall, fresh: boolean): void {
    this.endpointsOf(call.key).then((urls: string[]): void => {
      if (call.settled) {
        return;
      }
      try {
        const group: RpcPoolEndpoint = this.groupOf(call.key, urls);
        if (this.assign(call, group)) {
          return;
        }
        if (fresh && this.queue.length >= this.maxQueued) {
          this.fail(call, poolError(errorCodes.limitExceeded, 'too many rpc calls waiting for a connection'));
          return;
        }
        this.queue.push(call);
      } catch (e) {
        this.fail(call, e);
      }
    }, (error: any): void => {
      this.fail(call, error);
    });
  }


  // Sends a call on a connection of its group, if there is one with room or one may be opened.
  private assign(call: PendingCall, group: RpcPoolEndpoint): boolean {
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
    conn.idle?.clear();
    conn.idle = null;
    const options: RpcOptions | undefined = call.headers === undefined ? undefined : { headers: call.headers };
    conn.client.rpc(call.method, call.data, options).then(
      (reply: RpcResult): void => this.replied(call, conn, reply),
      (error: any): void => this.errored(call, conn, error)
    );
  }


  // Opens a connection to an endpoint. Its connect is in flight until its first connected event.
  private open(group: RpcPoolEndpoint, url: string): PoolConnection {
    const client: ProtobufCentrifuge = new ProtobufCentrifuge(url, this.connectionOptions);
    const conn: PoolConnection = {
      url: url,
      group: group,
      client: client,
      inflight: 0,
      connecting: true,
      closeCode: null,
      idle: null,
    };
    group.add(conn);
    client.on('connected', (): void => {
      conn.connecting = false;
      this.dispatch();
    });
    client.on('disconnected', (ctx: any): void => {
      this.lost(conn, ctx);
    });
    client.connect();
    return conn;
  }


  // A connection ended for good. It leaves its group; the calls waiting take the other connections, and the
  // application hears of the end unless it asked for it.
  private lost(conn: PoolConnection, ctx: any): void {
    conn.connecting = false;
    conn.closeCode = ctx.code;
    conn.idle?.clear();
    conn.idle = null;
    conn.group.remove(conn);
    this.dispatch();
    if (ctx.code !== disconnectedCodes.disconnectCalled) {
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


  // A slot became free: an idle connection is closed after idleTimeout, and the queue takes the free slot.
  private released(conn: PoolConnection): void {
    if (conn.inflight === 0) {
      this.armIdle(conn);
    }
    this.dispatch();
  }


  // Closes a connection when it stays without calls for idleTimeout. Nothing runs without the option.
  private armIdle(conn: PoolConnection): void {
    if (this.idleTimeout === 0 || this.closed) {
      return;
    }
    conn.idle?.clear();
    conn.idle = callTimer(Date.now() + this.idleTimeout, (): void => {
      conn.idle = null;
      if (conn.inflight === 0 && !this.closed) {
        conn.group.remove(conn);
        conn.client.disconnect();
      }
    });
  }


  // The error of a call on a connection. A connection the server closed for message size settles its calls without
  // a resend: the failure is the same on every attempt. A lost connection or a later refusal is retried; a server error
  // may move the call by reroute, and the later signal is retried as the main client does.
  private failed(call: PendingCall, conn: PoolConnection, error: any): void {
    const code: number | undefined = error?.code;
    if (code === errorCodes.connectionClosed || code === errorCodes.clientDisconnected) {
      if (conn.closeCode === disconnectedCodes.messageSizeLimit) {
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
        if (key !== undefined) {
          call.key = key;
          this.retry(call, conn.url, error, 0);
          return;
        }
      }
      if (retrySignal(error) === 'later') {
        this.retry(call, conn.url, error, laterResendDelay(call.resends));
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
    if (redirect !== undefined) {
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
    const controller: AbortController = new AbortController();
    call.controller = controller;
    this.redirects.acquire(controller.signal).then((release: () => void): void => {
      fetchRedirect(this.fetchFunction, endpoint, redirect, controller.signal).then(
        (data: Uint8Array): void => {
          release();
          this.succeed(call, { data: data, cached: false });
        },
        (error: any): void => {
          release();
          this.fail(call, error);
        }
      );
    }, (): void => {});
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


  // Settles a call: no timer, no signal listener, not queued, and a redirect fetch is cancelled. False when it settled.
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
    const index: number = this.queue.indexOf(call);
    if (index >= 0) {
      this.queue.splice(index, 1);
    }
    return true;
  }


  // Places the queued calls on the connections with room, first come first served. A call that cannot be placed keeps
  // its place. A connection event during a pass (it may open or end a connection) asks for one more pass instead.
  private dispatch(): void {
    if (this.closed) {
      return;
    }
    if (this.dispatching) {
      this.dispatchAgain = true;
      return;
    }
    this.dispatching = true;
    try {
      do {
        this.dispatchAgain = false;
        for (const call of this.queue.slice(0)) {
          const group: RpcPoolEndpoint | undefined = this.groups.get(call.key);
          if (!call.settled && group !== undefined) {
            this.place(call, group);
          }
        }
      } while (this.dispatchAgain);
    } finally {
      this.dispatching = false;
    }
  }


  // Takes a queued call off the queue when a connection takes it. A failure to open a connection fails the call.
  private place(call: PendingCall, group: RpcPoolEndpoint): void {
    try {
      if (!this.assign(call, group)) {
        return;
      }
    } catch (e) {
      this.fail(call, e);
    }
    const index: number = this.queue.indexOf(call);
    if (index >= 0) {
      this.queue.splice(index, 1);
    }
  }
}


// The options of a client a pool passes on: only the names of connectionOptionNames are taken.
function connectionOptionsOf(connection: Partial<Options> | undefined): Partial<Options> {
  const taken: Record<string, any> = {};
  if (connection === undefined) {
    return taken;
  }
  for (const name of Object.keys(connection)) {
    if (connectionOptionNames.indexOf(name) < 0) {
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
