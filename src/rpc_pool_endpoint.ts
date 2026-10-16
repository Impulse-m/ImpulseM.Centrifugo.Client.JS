import type { Centrifuge } from './centrifuge';
import type { CallTimer } from './call_timer';

/**
 * The error of the pool, of the same shape as the errors of Centrifuge.rpc(): a code and a message. It mirrors the inline
 * {code, message} literals of call_timer.ts and subscription.ts; the shape is one contract, so change them together.
 */
export function poolError(code: number, message: string): any {
  return { code: code, message: message };
}

/** A connection of an rpc pool: its client, the calls it carries and whether its connect is still in flight. */
export interface PoolConnection {
  /** The endpoint the client connects to. */
  url: string;
  /** The endpoint key's group the connection belongs to. */
  group: RpcPoolEndpoint;
  client: Centrifuge;
  /** Calls in flight on the connection, each holding a slot of the window. */
  inflight: number;
  /** True until the first connected event: one connect in flight per endpoint. */
  connecting: boolean;
  /** The code of the disconnect that ended the connection for good, null while it is not ended. */
  closeCode: number | null;
  /** Closes the connection when it stays without calls, see RpcPool. */
  idle: CallTimer | null;
}


/** The connections of one endpoint key, over the endpoints its resolver gave. */
export class RpcPoolEndpoint {
  readonly urls: string[];
  private connections: PoolConnection[] = [];


  constructor(urls: string[]) {
    this.urls = urls;
  }


  /** The connection of the key with a free slot of the window and the fewest calls in flight, null when none has one. */
  roomy(window: number): PoolConnection | null {
    let best: PoolConnection | null = null;
    for (const conn of this.connections) {
      if (conn.inflight < window && (best === null || conn.inflight < best.inflight)) {
        best = conn;
      }
    }
    return best;
  }


  /**
   * The first endpoint with fewer connections than perEndpoint and no connect in flight, null when there is none:
   * a connect is serialised per endpoint, so a burst of calls opens one socket per endpoint at a time.
   */
  openable(perEndpoint: number): string | null {
    for (const url of this.urls) {
      let count: number = 0;
      let connecting: boolean = false;
      for (const conn of this.connections) {
        if (conn.url === url) {
          count++;
          connecting = connecting || conn.connecting;
        }
      }
      if (!connecting && count < perEndpoint) {
        return url;
      }
    }
    return null;
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


  /** A copy of the connections: the caller may close them while it walks the list. */
  all(): PoolConnection[] {
    return this.connections.slice(0);
  }
}
