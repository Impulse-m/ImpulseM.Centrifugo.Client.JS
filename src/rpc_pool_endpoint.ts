import type { Centrifuge } from './centrifuge';
import type { CallTimer } from './call_timer';
import type { RpcResult } from './types';

// A call of the pool from rpc() until it settles.
export interface PendingCall {
  method: string;
  data: Uint8Array;
  headers: Record<string, string> | undefined;
  key: string;
  deadline: number;
  // Resends made so far, across the three causes: see failed and retry.
  resends: number;
  // Later refusals retried so far: the exponent of their delay, as the main client counts them.
  laterResends: number;
  settled: boolean;
  // True while the call holds a slot of a connection: from its send until its reply or its error.
  sent: boolean;
  // True while the call is counted in RpcPool.queued: from its admission until it holds a slot or settles. It is counted
  // while its key is still resolving, and while it waits in the line of its key (line).
  queued: boolean;
  // The line of the key the call waits in, null while it is not in one.
  line: RpcPoolLine | null;
  // The neighbours of the call in its line, null at either end.
  previous: PendingCall | null;
  next: PendingCall | null;
  // The identity generation the call is bound to, null while it is not bound (with tokenFrom only, see TokenSource).
  generation: number | null;
  // True when a reroute moved the call to its new key: it joins the front of that key's line.
  rerouted: boolean;
  timer: CallTimer | null;
  resend: CallTimer | null;
  // Removes the abort listener of the signal, if the call has one.
  detach: () => void;
  // Aborted when the call settles, so a redirect fetch in progress is cancelled.
  controller: AbortController | null;
  resolve: (result: RpcResult) => void;
  reject: (error: any) => void;
}

/** A connection of an rpc pool: its client, the calls it carries and whether its connect is still in flight. */
export interface PoolConnection {
  /** The endpoint the client connects to, which holds the connection. */
  endpoint: RpcPoolEndpoint;
  client: Centrifuge;
  /** Calls in flight on the connection, each holding a slot of the window. */
  inflight: number;
  /** True from the open, and again while the client reconnects, until its next connected event: the endpoint's connect. */
  connecting: boolean;
  /** True while the connection is connected and its identity verified: it takes calls (see RpcPool, live). */
  live: boolean;
  /** The code of the disconnect that ended the connection for good, null while it is not ended. */
  closeCode: number | null;
  /** The time the connection was last left without calls, read by its idle check, see RpcPool. */
  lastUse: number;
  /** The pending idle check, at most one per connection, see RpcPool. */
  idle: CallTimer | null;
  /** The identity generation of the connection's token, null before its first token; only with tokenFrom. */
  generation: number | null;
  /**
   * The token the connection presented at its last connect attempt that the client did not abandon, '' before the first.
   * The client clears its own token on a state-invalidated disconnect, so the pool keeps it here, see RpcPool, presentedOf.
   */
  presented: string;
  /** True once the connection is retired: it takes no new call, and it closes when its calls settle. */
  retired: boolean;
}


/**
 * A line of the calls of one endpoint key, first come first served: a doubly linked line, so a call leaves it in O(1)
 * when it settles. The endpoints of the key are kept beside it, for the keys waiting per endpoint (see RpcPoolEndpoint).
 */
export class RpcPoolLine {
  readonly key: string;
  readonly endpoints: RpcPoolEndpoint[];
  private first: PendingCall | null = null;
  private last: PendingCall | null = null;


  constructor(key: string, endpoints: RpcPoolEndpoint[]) {
    this.key = key;
    this.endpoints = endpoints;
  }


  /** The call at the head of the line, null when no call waits. */
  head(): PendingCall | null {
    return this.first;
  }


  /** Whether no call waits in the line. */
  isEmpty(): boolean {
    return this.first === null;
  }


  /** Appends a call to the end of the line. */
  enqueue(call: PendingCall): void {
    call.line = this;
    call.previous = this.last;
    call.next = null;
    if (this.last === null) {
      this.first = call;
    } else {
      this.last.next = call;
    }
    this.last = call;
  }


  /** Puts a call at the head of the line: a rerouted call goes first. */
  addFirst(call: PendingCall): void {
    call.line = this;
    call.previous = null;
    call.next = this.first;
    if (this.first === null) {
      this.last = call;
    } else {
      this.first.previous = call;
    }
    this.first = call;
  }


  /** Takes a call out of the line in O(1). A call which is not in this line is left alone. */
  dequeue(call: PendingCall): void {
    if (call.line !== this) {
      return;
    }
    if (call.previous === null) {
      this.first = call.next;
    } else {
      call.previous.next = call.next;
    }
    if (call.next === null) {
      this.last = call.previous;
    } else {
      call.next.previous = call.previous;
    }
    call.line = null;
    call.previous = null;
    call.next = null;
  }
}


/**
 * One endpoint: a URL, and the connections the pool opened to it. Connections belong to the URL, so every key which
 * resolves to it shares them. It keeps the hold of opens after a terminal close, and the keys with waiting calls.
 */
export class RpcPoolEndpoint {
  readonly url: string;
  // Terminal closes since the endpoint last connected: the hold of its next open grows with them.
  closes: number = 0;
  // The hold of the opens after a terminal close: one timer per held endpoint, see RpcPool.
  hold: CallTimer | null = null;
  // The keys with calls in their line, among the keys that resolve to this endpoint.
  readonly waitingKeys: Set<string> = new Set<string>();
  // The connections the endpoint holds: opened, and neither retired nor removed.
  private connections: PoolConnection[] = [];


  constructor(url: string) {
    this.url = url;
  }


  /** The connections the endpoint holds. The caller must not keep the array across a call that may remove one. */
  all(): PoolConnection[] {
    return this.connections;
  }


  /** A copy of the connections: the caller may close them while it walks the list. */
  copy(): PoolConnection[] {
    return this.connections.slice(0);
  }


  /** How many connections the endpoint holds. */
  count(): number {
    return this.connections.length;
  }


  /** Whether a connection of the endpoint is connecting: its connect is in flight, and no other opens. */
  connecting(): boolean {
    for (const conn of this.connections) {
      if (conn.connecting) {
        return true;
      }
    }
    return false;
  }


  add(conn: PoolConnection): void {
    this.connections.push(conn);
  }


  remove(conn: PoolConnection): void {
    const index: number = this.connections.indexOf(conn);
    if (index >= 0) {
      this.connections.splice(index, 1);
    }
  }


  /** The first connection whose token belongs to a generation other than the one given, null when there is none. */
  staleOf(generation: number): PoolConnection | null {
    for (const conn of this.connections) {
      if (conn.generation !== null && conn.generation !== generation) {
        return conn;
      }
    }
    return null;
  }
}
