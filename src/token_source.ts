import type { Centrifuge } from './centrifuge';
import { disconnectedCodes } from './codes';
import { createTokenSourceHost } from './client_host';
import type { TokenSourceHost } from './client_host';
import { refreshKeepsConnection } from './refresh_slot';
import { State } from './types';
import { UnauthorizedError } from './unauthorized_error';

/** A token the source delivers, with the generation of the identity it belongs to. */
export interface SourcedToken {
  token: string;
  generation: number;
}

/**
 * The state of the main client as the pools read it: connected with its identity confirmed, connecting, or
 * disconnected. A session that ended as unauthorized reads as ended, until the next connect attempt.
 */
export type MainState = 'connected' | 'connecting' | 'disconnected' | 'ended';

/** A pool with a call in a key line: told of the events of the main client that may place or fail its calls. */
export interface MainListener {
  onMainEvent(): void;
}

// A request of a pool connection waiting for the token: the token it was refused, and how to answer it.
interface Waiter {
  refused: string;
  resolve: (sourced: SourcedToken) => void;
  reject: (error: any) => void;
}

// The source of each client, made by tokenSourceOf on the first pool that names the client. Made on first use.
let sources: WeakMap<Centrifuge, TokenSource> | null = null;


/**
 * The token of the connections of an RPC pool, lent from the client which owns it (the pool's tokenFrom). It runs no timer
 * and no poll: it answers the requests of the pool's connections from the client's confirmed token, and starts one renewal
 * of the client through the client's refresh when the token it holds was refused or has expired. A request is answered at
 * once, or it waits for an event of the client; it never waits for a timer of its own.
 *
 * The identity generation moves on when the identity the server confirmed changes, and when the session ends. A pool
 * connection is stamped with the generation of its first token, and is retired when the generation moves on.
 */
export class TokenSource {
  private readonly client: Centrifuge;
  private readonly host: TokenSourceHost;
  // The one listener the source has on its client: the terminal disconnect. Attached again when the application removed it.
  private readonly onDisconnected: (ctx: any) => void = (ctx: any): void => {
    this.terminal(ctx);
  };
  // The pools with a call in a key line, told of the events that may place or fail their calls (see MainListener).
  private readonly pools: Set<MainListener> = new Set<MainListener>();
  // The requests waiting for a token, one per requester: a new request of a requester replaces its waiting one.
  private readonly waiting: Map<object, Waiter> = new Map<object, Waiter>();
  // The token the main client confirmed last, null before its first confirmation.
  private confirmed: string | null = null;
  // The token found unusable: a pool connection connected with it, and its server confirmed another identity.
  private unusableToken: string | null = null;
  // The confirmed token the fleet refused and no renewal can replace before the next confirmation: the source is spent.
  private spentToken: string | null = null;
  // The confirmed token a renewal was started for since the main client last connected, null after each connect.
  private renewedFor: string | null = null;
  // The identity recorded, and whether one was recorded at all.
  private recorded: boolean = false;
  private user: string = '';
  private authClass: string = '';
  private generationCount: number = 0;
  // Set by a terminal disconnect as unauthorized; the connect confirmation clears it.
  private ended: boolean = false;
  // Set by the connect confirmation, cleared by a terminal disconnect: the main client is connected and its identity known.
  private confirmedConnect: boolean = false;


  constructor(client: Centrifuge) {
    this.client = client;
    this.host = createTokenSourceHost(client);
    this.attach();
    this.host.setConnectConfirmed((): void => {
      this.onConnected();
    });
    this.host.setRefreshAccepted((): void => {
      this.onAccepted();
    });
    this.host.setRefreshFailed((error: any, explicit: boolean): void => {
      this.onRefreshFailed(error, explicit);
    });
    // The source is made by the first pool which names the client, which may come after the client connected: the connect
    // confirmation it missed is taken now, from the identity the client holds.
    if (this.host.state() === State.Connected) {
      this.onConnected();
    }
  }


  /**
   * The token for a connection of the pool, with the generation it belongs to. refused is the token the connection
   * presented last, '' on its first request. The request is answered at once when the main client is connected and its
   * confirmed token is usable; it waits while the client connects, and while a renewal runs. It rejects with
   * UnauthorizedError when the session has ended or the client is disconnected.
   */
  token(requester: object, refused: string): Promise<SourcedToken> {
    this.reattach();
    return new Promise<SourcedToken>((resolve: (sourced: SourcedToken) => void, reject: (error: any) => void): void => {
      const waiter: Waiter = { refused: refused, resolve: resolve, reject: reject };
      // The request it replaces is answered unauthorized, so that no promise of the requester is left pending.
      this.waiting.get(requester)?.reject(new UnauthorizedError('unauthorized'));
      this.waiting.set(requester, waiter);
      this.step(requester, waiter);
    });
  }


  /** The identity generation: a plain read, it moves on at the confirmations of the main client and at a session end. */
  generation(): number {
    return this.generationCount;
  }


  /** Whether the main client recorded an identity, which a session end clears until the next connect confirmation. */
  recordedPair(): boolean {
    return this.recorded;
  }


  /** Whether a user and auth class are the identity the main client recorded. */
  identityMatches(user: string, authClass: string): boolean {
    return this.recorded && user === this.user && authClass === this.authClass;
  }


  /** The state of the main client, see MainState. */
  mainState(): MainState {
    const state: State = this.host.state();
    if (state === State.Connected) {
      return this.confirmedConnect ? 'connected' : 'connecting';
    }
    if (state === State.Connecting) {
      return 'connecting';
    }
    return this.ended ? 'ended' : 'disconnected';
  }


  /** Whether the pool may open a connection now: the main client is connected, and its confirmed token is neither unusable nor spent. */
  canLend(): boolean {
    return this.mainState() === 'connected'
      && this.confirmed !== null
      && this.confirmed !== this.unusableToken
      && this.confirmed !== this.spentToken;
  }


  /** Whether the main client is connected and its confirmed token is spent: the fleet refused it and no renewal replaces it. */
  spent(): boolean {
    return this.mainState() === 'connected' && this.confirmed !== null && this.confirmed === this.spentToken;
  }


  /** Marks the confirmed token as unusable: a connection which carried it was confirmed another identity. */
  unusable(token: string): void {
    this.unusableToken = token;
  }


  /** Drops the waiting request of a requester which left the pool, and answers it unauthorized. */
  forget(requester: object): void {
    const waiter: Waiter | undefined = this.waiting.get(requester);
    if (waiter !== undefined) {
      this.waiting.delete(requester);
      waiter.reject(new UnauthorizedError('unauthorized'));
    }
  }


  /** Tells the pool the events of the main client that may place or fail its calls. */
  listen(pool: MainListener): void {
    this.pools.add(pool);
  }


  /** Stops telling the pool: a pool with no call in a key line, or a closed one, is not kept by the source. */
  unlisten(pool: MainListener): void {
    this.pools.delete(pool);
  }


  /** Re-attaches the listener the application may have removed, when a request waits for an event of the client. */
  reattachWaiting(): void {
    if (this.waiting.size > 0) {
      this.reattach();
    }
  }


  // Puts the terminal listener back when it is missing, and answers the waiting requests then: an event missed that way
  // is answered by the re-evaluation.
  private reattach(): void {
    if (this.attach()) {
      this.settle();
    }
  }


  // Keeps the listener on the client. True when it was missing and is attached now.
  private attach(): boolean {
    if (this.client.listeners('disconnected').indexOf(this.onDisconnected) >= 0) {
      return false;
    }
    this.client.on('disconnected', this.onDisconnected);
    return true;
  }


  // Records the identity of the main client: the first one without a count, a changed one with one.
  private observe(): void {
    if (this.mainState() !== 'connected') {
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


  // Answers one waiting request, and starts the renewal it needs. A request left waiting is
  // answered by a later event of the main client.
  private step(requester: object, waiter: Waiter): void {
    const state: MainState = this.mainState();
    if (state === 'ended' || state === 'disconnected') {
      this.fail(requester, waiter);
      return;
    }
    if (state === 'connecting' || this.confirmed === null) {
      return;
    }
    const confirmed: string = this.confirmed;
    if (confirmed !== ''
      && confirmed !== waiter.refused
      && confirmed !== this.unusableToken
      && confirmed !== this.spentToken
      && !this.host.tokenExpired(confirmed)) {
      this.deliver(requester, waiter, confirmed);
      return;
    }
    // Found unusable and not spent: the next confirmation decides, a renewal would only repeat the mismatch.
    if (confirmed === this.unusableToken && confirmed !== this.spentToken) {
      return;
    }
    if (confirmed === '') {
      this.deliver(requester, waiter, '');
      return;
    }
    // No renewal can replace the token, or it is spent already: the request is answered now, and the token is spent.
    if (!this.host.canRenew() || confirmed === this.spentToken) {
      this.fail(requester, waiter);
      this.spend(confirmed);
      return;
    }
    // One renewal per confirmed token and main connection: a renewal started for this token is not started again.
    if (this.renewedFor !== confirmed && this.host.renewNow()) {
      this.renewedFor = confirmed;
    }
  }


  // The confirmed token is spent: the fleet refused it and no renewal can replace it before the next confirmation. The
  // pools hear of it, after the requests of the event are answered.
  private spend(token: string): void {
    if (this.spentToken === token) {
      return;
    }
    this.spentToken = token;
    this.notify();
  }


  // Answers every waiting request again, after an event of the main client. The requests are walked over a copy, and a
  // request which settles during the walk is not evaluated again.
  private settle(): void {
    for (const [requester, waiter] of Array.from(this.waiting)) {
      if (this.waiting.get(requester) === waiter) {
        this.step(requester, waiter);
      }
    }
  }


  // Tells the pools of the main client an event which may place or fail their waiting calls. Each pool takes its own pass.
  private notify(): void {
    for (const pool of Array.from(this.pools)) {
      pool.onMainEvent();
    }
  }


  // The connect of the main client is confirmed: its token is the confirmed one, a renewal, an unusable mark and a spent
  // mark are cleared.
  private onConnected(): void {
    this.confirmedConnect = true;
    this.ended = false;
    this.renewedFor = null;
    this.unusableToken = null;
    this.spentToken = null;
    this.confirmed = this.host.token();
    this.observe();
    this.settle();
    this.notify();
  }


  // A refresh of the main client is accepted while it is connected: the accepted token is the confirmed one. The requests
  // refused that token fail now, since a renewal gave it back; a renewal that gave back the token it was started for, or
  // that refused a request, spends the source. The others are evaluated again. The pools hear of it only when the identity
  // changed, an unusable mark was cleared, or the spent mark changed.
  private onAccepted(): void {
    if (this.mainState() !== 'connected') {
      return;
    }
    const generation: number = this.generationCount;
    const cleared: boolean = this.unusableToken !== null;
    const spentBefore: string | null = this.spentToken;
    this.confirmed = this.host.token();
    this.unusableToken = null;
    this.spentToken = null;
    this.observe();
    const refused: boolean = this.refusedAgain();
    if (refused || this.confirmed === this.renewedFor) {
      this.spentToken = this.confirmed;
    }
    this.settle();
    if (this.generationCount !== generation || cleared || this.spentToken !== spentBefore) {
      this.notify();
    }
  }


  // A refresh failed while the main client is connected. A 103 for the renewal of the confirmed token is final: the main
  // client keeps its connection and will not retry, so the token is spent, and the requests wait no longer and fail. Else
  // an explicit refresh re-evaluates the requests (it may leave a renewal to start), and a timer refresh refused with 103
  // does too; another timer refusal is the main client's own business: it retries or ends the connection.
  private onRefreshFailed(error: any, explicit: boolean): void {
    if (this.mainState() !== 'connected') {
      return;
    }
    const code: number = error?.code;
    const confirmed: string | null = this.confirmed;
    if (refreshKeepsConnection(code) && this.renewedFor !== null && this.renewedFor === confirmed && confirmed !== null) {
      for (const [requester, waiter] of Array.from(this.waiting)) {
        this.fail(requester, waiter);
      }
      this.spend(confirmed);
      return;
    }
    if (explicit || refreshKeepsConnection(code)) {
      this.settle();
    }
  }


  // The terminal disconnect of the main client: the session ends as unauthorized, or the client is disconnected for good.
  // Every waiting request is answered unauthorized, and the pools hear of it.
  private terminal(ctx: any): void {
    this.confirmedConnect = false;
    this.ended = ctx?.code === disconnectedCodes.unauthorized;
    if (this.ended) {
      this.recorded = false;
      this.generationCount++;
    }
    for (const [requester, waiter] of Array.from(this.waiting)) {
      this.fail(requester, waiter);
    }
    this.notify();
  }


  // A renewal accepted with the token a request was refused: that request cannot use it, so it fails as unauthorized now.
  // True when such a request was there.
  private refusedAgain(): boolean {
    const confirmed: string | null = this.confirmed;
    if (confirmed === null || confirmed === '') {
      return false;
    }
    let refused: boolean = false;
    for (const [requester, waiter] of Array.from(this.waiting)) {
      if (waiter.refused === confirmed && this.waiting.get(requester) === waiter) {
        refused = true;
        this.fail(requester, waiter);
      }
    }
    return refused;
  }


  private deliver(requester: object, waiter: Waiter, token: string): void {
    this.observe();
    this.waiting.delete(requester);
    waiter.resolve({ token: token, generation: this.generationCount });
  }


  private fail(requester: object, waiter: Waiter): void {
    this.waiting.delete(requester);
    waiter.reject(new UnauthorizedError('unauthorized'));
  }
}


/** The source of a client, made on the first call for it. The client holds no source until a pool names it. */
export function tokenSourceOf(client: Centrifuge): TokenSource {
  if (sources === null) {
    sources = new WeakMap<Centrifuge, TokenSource>();
  }
  let source: TokenSource | undefined = sources.get(client);
  if (source === undefined) {
    source = new TokenSource(client);
    sources.set(client, source);
  }
  return source;
}
