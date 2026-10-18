import type { Centrifuge } from './centrifuge';
import { disconnectedCodes } from './codes';
import { createTokenSourceHost } from './client_host';
import type { TokenSourceHost } from './client_host';
import { State } from './types';
import { UnauthorizedError } from './unauthorized_error';

/** A token the source delivers, with the generation of the identity it belongs to. */
export interface SourcedToken {
  token: string;
  generation: number;
}

// A request of a pool connection waiting for the token: the token it was refused, and how to answer it.
interface Waiter {
  refused: string;
  resolve: (sourced: SourcedToken) => void;
  reject: (error: any) => void;
}

// The source of each client, made by tokenSourceOf on the first pool that names the client.
const sources: WeakMap<Centrifuge, TokenSource> = new WeakMap<Centrifuge, TokenSource>();


/**
 * The token of the connections of an RPC pool, lent from the client which owns it (the pool's tokenFrom). It runs no
 * timer and no poll: it answers the requests of the pool's connections from the client's own token, and it starts one
 * renewal of the client through the client's refresh, when the token the client holds was refused or has expired. A
 * request is answered at once, or it waits for an event of the client; a request never fails with a retryable error.
 *
 * The identity generation counts the changes of the user and auth class the server confirmed: a pool connection
 * carries the generation of its first token, and is retired when the generation moves on.
 */
export class TokenSource {
  private readonly client: Centrifuge;
  private readonly host: TokenSourceHost;
  // The listeners the source has on its client, by event name: attached again when the application removes them.
  private readonly handlers: Map<string, (ctx: any) => void> = new Map<string, (ctx: any) => void>();
  // The requests waiting for a token, one per requester: a new request of a requester replaces its waiting one.
  private readonly waiting: Map<object, Waiter> = new Map<object, Waiter>();
  // The token a renewal was started for since the client last connected; null before the first connect.
  private renewedFor: string | null = null;
  // The identity recorded, and whether one was recorded at all.
  private recorded: boolean = false;
  private user: string = '';
  private authClass: string = '';
  private generationCount: number = 0;
  // Set when the client's session ended as unauthorized, until its next connect attempt.
  private sessionEnded: boolean = false;


  constructor(client: Centrifuge) {
    this.client = client;
    this.host = createTokenSourceHost(client);
    this.handlers.set('connecting', (): void => {
      this.sessionEnded = false;
    });
    this.handlers.set('connected', (): void => {
      this.renewedFor = null;
      this.observe();
      this.settle();
    });
    this.handlers.set('disconnected', (ctx: any): void => {
      if (ctx?.code === disconnectedCodes.unauthorized) {
        this.sessionEnded = true;
        this.recorded = false;
        this.generationCount++;
      }
      this.settle();
    });
    this.handlers.set('auth', (): void => {
      this.observe();
    });
    this.attach();
    this.host.setRefreshAccepted((): void => {
      this.refusedAgain();
      this.settle();
    });
  }


  /**
   * The token for a connection of the pool, with the generation it belongs to. refused is the token the connection was
   * given last, '' on its first request. Resolves at once when the connected client holds a usable token, with '' when it
   * is anonymous; it waits while the client connects, and rejects with UnauthorizedError when the client is not
   * connected or connecting, or the session has ended.
   */
  token(requester: object, refused: string): Promise<SourcedToken> {
    this.reattach();
    return new Promise<SourcedToken>((resolve: (sourced: SourcedToken) => void, reject: (error: any) => void): void => {
      const waiter: Waiter = { refused: refused, resolve: resolve, reject: reject };
      // The request it replaces settles, so that no promise of the requester is left pending.
      this.waiting.get(requester)?.reject(new UnauthorizedError('unauthorized'));
      this.waiting.set(requester, waiter);
      this.step(requester, waiter);
    });
  }


  /** The generation of the identity of the client: it moves on when the identity changes, and when the session ends. */
  generation(): number {
    this.observe();
    return this.generationCount;
  }


  /**
   * Whether a call of the pool must not be sent now: the session ended as unauthorized, or the client is neither
   * connecting nor connected, so no token can be lent. Then no connection is opened, and the call fails at once.
   */
  refuses(): boolean {
    if (this.sessionEnded) {
      return true;
    }
    const state: State = this.host.state();
    return state !== State.Connected && state !== State.Connecting;
  }


  /** Whether the client is connecting: its identity is not confirmed yet, and no token is lent in that window. */
  connecting(): boolean {
    return this.host.state() === State.Connecting;
  }


  /** Drops the waiting request of a requester which left the pool. */
  forget(requester: object): void {
    this.waiting.delete(requester);
  }


  /**
   * Re-attaches the listeners an application removed (removeAllListeners). An event missed that way, such as the connect
   * of the client, is answered here: the waiting requests are answered again, once, when a listener was missing. The
   * check is three lookups, and it settles nothing when all listeners are in place.
   */
  reattach(): void {
    if (this.attach()) {
      this.settle();
    }
  }


  // Keeps the listeners of the handlers on the client. True when one was missing and is attached now.
  private attach(): boolean {
    let attached: boolean = false;
    for (const [name, handler] of this.handlers) {
      if (this.client.listeners(name as 'connected').indexOf(handler) < 0) {
        this.client.on(name as 'connected', handler);
        attached = true;
      }
    }
    return attached;
  }


  // Records the identity of a connected client: the first one without a count, a changed one with one.
  private observe(): void {
    if (this.host.state() !== State.Connected) {
      return;
    }
    const identity: { user: string; authClass: string } = this.host.identity();
    if (!this.recorded) {
      this.record(identity.user, identity.authClass);
      return;
    }
    if (identity.user !== this.user || identity.authClass !== this.authClass) {
      this.record(identity.user, identity.authClass);
      this.generationCount++;
    }
  }


  private record(user: string, authClass: string): void {
    this.recorded = true;
    this.user = user;
    this.authClass = authClass;
  }


  // Answers a waiting request when the client allows it, and starts the renewal it needs. True when it settled.
  private step(requester: object, waiter: Waiter): boolean {
    if (this.sessionEnded) {
      return this.fail(requester, waiter);
    }
    // A token is lent only while the client is connected, so that its identity is confirmed: in the connecting window
    // the request waits for the connect, an accepted token or a terminal disconnect.
    const state: State = this.host.state();
    if (state === State.Connecting) {
      return false;
    }
    if (state !== State.Connected) {
      return this.fail(requester, waiter);
    }
    const token: string = this.host.token();
    if (token !== '' && token !== waiter.refused && !this.host.tokenExpired(token)) {
      return this.deliver(requester, waiter, token);
    }
    if (token === '') {
      return this.deliver(requester, waiter, '');
    }
    if (!this.host.canRenew()) {
      return this.fail(requester, waiter);
    }
    // The renewal is recorded only when it started: a refresh in flight starts none, and the next event asks again.
    if (this.renewedFor !== token && this.host.renewNow()) {
      this.renewedFor = token;
    }
    return false;
  }


  // Answers every waiting request again, after an event of the client. A request only leaves the map when it settles,
  // and a step adds none, so the map is walked in place.
  private settle(): void {
    for (const [requester, waiter] of this.waiting) {
      this.step(requester, waiter);
    }
  }


  // A renewal accepted with the token a request was refused: the renewal gave back the refused token, which no request
  // can use, so those requests fail as unauthorized now. Waiting for the next event of the client would wait up to a
  // whole token ttl, and the same refusal would come back. The choice is recorded in the README.
  private refusedAgain(): void {
    const token: string = this.host.token();
    if (token === '') {
      return;
    }
    // The token a renewal was started for has come back, so a request refused it again starts a renewal of its own: each
    // such renewal is driven by a refusal, not by an event, so it cannot loop.
    if (this.renewedFor === token) {
      this.renewedFor = null;
    }
    for (const [requester, waiter] of this.waiting) {
      if (waiter.refused === token) {
        this.fail(requester, waiter);
      }
    }
  }


  private deliver(requester: object, waiter: Waiter, token: string): boolean {
    this.waiting.delete(requester);
    waiter.resolve({ token: token, generation: this.generation() });
    return true;
  }


  private fail(requester: object, waiter: Waiter): boolean {
    this.waiting.delete(requester);
    waiter.reject(new UnauthorizedError('unauthorized'));
    return true;
  }
}


/** The source of a client, made on the first call for it. The client holds no source until a pool names it. */
export function tokenSourceOf(client: Centrifuge): TokenSource {
  let source: TokenSource | undefined = sources.get(client);
  if (source === undefined) {
    source = new TokenSource(client);
    sources.set(client, source);
  }
  return source;
}
