import { errorCodes } from './codes';
import { CallTimer, callTimer, raceCall } from './call_timer';
import { hasHeaders } from './extensions';
import { MsgIdGenerator, RpcQueue, TrackedRpc } from './rpc_queue';
import type { CallRejectContext, CallResolveContext } from './centrifuge';
import { RpcOptions, RpcResult } from './types';

// The metadata key the server sets on its own rpc errors, and its two values, see retrySignal. Error
// metadata of the server cannot carry this key itself.
const rpcRetryMetadataKey: string = 'x-centrifugo-retry';
const rpcRetryNewMsgId: string = 'new_msg_id';
const rpcRetryLater: string = 'later';
// Most sends of a call refused with a new message id, and most later attempts, after its first send.
const maxNewIdResends: number = 2;
const maxLaterResends: number = 3;
// The delay of the first later attempt; each further one waits twice as long.
const laterResendBase: number = 250;

/** The resend signal of a server error, see retrySignal. */
export type RetrySignal = 'new_msg_id' | 'later' | null;

/**
 * The resend signal of a server error: 'new_msg_id' when the server did not run the call and refused its
 * message id, 'later' when it did not run the call and the message id is still good, null otherwise.
 * @internal
 */
export function retrySignal(error: any): RetrySignal {
  const value: any = error?.metadata?.[rpcRetryMetadataKey];
  if (value === rpcRetryNewMsgId || value === rpcRetryLater) {
    return value;
  }
  return null;
}


/** The callbacks of a command waiting for its reply, as the client keeps them by command id. */
export interface RpcCallbacks {
  callback: (ctx: CallResolveContext) => void;
  errback: (ctx: CallRejectContext) => void;
  // Called instead of errback when the connection is lost before the reply.
  lost?: () => void;
  timeout: CallTimer | null;
}


/** What the tracker needs from its client. */
export interface RpcHost {
  nextCommandId(): number;
  addCommand(command: any): void;
  setCallbacks(id: number, callbacks: RpcCallbacks): void;
  deleteCallbacks(id: number): void;
  createError(code: number, message: string): any;
  serverTime(): number;
  rpcOptionsSupported(): boolean;
  isConnected(): boolean;
  hasInstance(): boolean;
  /** Resolves once a call can go out: connected, and no refresh in flight. Rejects when the client is disconnected for good. */
  ready(deadline: number): Promise<void>;
  /** Sends a plain rpc request on the connection. Resolves with the result of its reply. */
  plainCall(command: any): Promise<any>;
}


/**
 * Tracks the rpc calls of a client. A call with a message id is kept until it settles: by a reply,
 * an error, its deadline or its signal. A call whose connection is lost before its reply waits for
 * the next connection, and is sent again there with the same message id. The ids of the replies
 * received are acknowledged with the next request.
 */
export class RpcTracker {
  private host: RpcHost;
  private maxPendingRpc: number;
  private callTimeout: number;
  private queue: RpcQueue = new RpcQueue();
  private msgIds: MsgIdGenerator = new MsgIdGenerator();
  // Message ids of cancels not delivered: calls aborted while their connection was lost, or whose
  // cancel was lost with it. Sent with the next connection, in case the server received the call.
  private cancels: number[] = [];
  // Calls of this client holding a slot of maxPendingRpc: from the call until it settled.
  private pending: number = 0;
  // Timers that drop the callbacks of settled calls, see forgetCommand.
  private forgets: Set<CallTimer> = new Set();


  constructor(host: RpcHost, maxPendingRpc: number, callTimeout: number) {
    this.host = host;
    this.maxPendingRpc = maxPendingRpc;
    this.callTimeout = callTimeout;
  }


  /**
   * Runs an rpc call. It waits for the connection and for a refresh in flight, then goes out: tracked
   * when the connection supports RPC options and the client has an instance, else as a plain request.
   * The deadline counts from the rpc() call, across the wait and the attempts. With an instance, the call
   * takes a slot of maxPendingRpc at once, or rejects with errorCodes.limitExceeded; the slot is given back
   * when the call settles.
   */
  async call(method: string, data: any, opts: RpcOptions | undefined, deadline: number): Promise<RpcResult> {
    if (!this.host.hasInstance()) {
      return this.attempt(method, data, opts, deadline);
    }
    if (this.pending >= this.maxPendingRpc) {
      throw this.host.createError(errorCodes.limitExceeded, 'too many pending rpc calls');
    }
    this.pending++;
    try {
      return await this.attempt(method, data, opts, deadline);
    } finally {
      this.pending--;
    }
  }


  /**
   * Tracks a call: it is sent on the current connection, or on the next one if it is lost. The call
   * rejects with a timeout at its deadline, counted from the moment of the rpc() call.
   */
  track(
    method: string,
    data: any,
    headers: Record<string, string> | undefined,
    signal: AbortSignal | undefined,
    deadline: number
  ): Promise<RpcResult> {
    return new Promise<RpcResult>((resolve, reject) => {
      // An abort before the call is tracked fired no listener: it is seen here.
      if (signal?.aborted) {
        reject(this.host.createError(errorCodes.cancelled, 'cancelled'));
        return;
      }
      const call: TrackedRpc = {
        msgId: this.msgIds.next(this.host.serverTime()),
        method: method,
        data: data,
        headers: headers,
        deadline: deadline,
        commandId: 0,
        sent: false,
        timer: null,
        settled: false,
        newIdResends: 0,
        laterResends: 0,
        resend: null,
        detach: (): void => {},
        resolve: resolve,
        reject: reject,
      };
      if (signal !== undefined) {
        const onAbort = (): void => this.abort(call);
        signal.addEventListener('abort', onAbort, { once: true });
        call.detach = (): void => signal.removeEventListener('abort', onAbort);
      }
      this.queue.add(call);
      call.timer = callTimer(call.deadline, (): void => this.expire(call));
      this.send(call, false);
    });
  }


  /**
   * Acts on a connection that confirmed the capability: sends the cancels and the calls waiting
   * for a connection, in message id order, each with its original deadline. Without the capability
   * the waiting calls are rejected with connectionClosed: they were written on a lost connection
   * and may have run, so they are not sent again.
   */
  sendWaiting(): void {
    if (!this.host.rpcOptionsSupported()) {
      // The cancels wait for a connection that carries them.
      for (const call of this.queue.waiting()) {
        this.settle(call);
        call.reject(this.host.createError(errorCodes.connectionClosed, 'connection closed'));
      }
      return;
    }
    const cancels: number[] = this.cancels;
    this.cancels = [];
    for (const msgId of cancels) {
      this.cancel(msgId);
    }
    for (const call of this.queue.waiting()) {
      if (call.deadline <= Date.now()) {
        this.expire(call);
      } else {
        // A call that was never written is not a retransmit: the server has not seen its message id.
        this.send(call, call.sent);
      }
    }
  }


  /** Rejects all tracked calls, e.g. on a disconnect that will not be followed by a reconnect. */
  rejectAll(code: number, message: string): void {
    this.cancels = [];
    for (const timer of this.forgets) {
      timer.clear();
    }
    this.forgets.clear();
    for (const call of this.queue.clear()) {
      this.settle(call);
      call.reject(this.host.createError(code, message));
    }
  }


  // The attempt of a call: see call.
  private async attempt(method: string, data: any, opts: RpcOptions | undefined, deadline: number): Promise<RpcResult> {
    const headers: Record<string, string> | undefined = opts?.headers;
    const signal: AbortSignal | undefined = opts?.signal;
    if (signal?.aborted) {
      // A connection that cannot carry a call with a signal rejects it with unsupported, as below.
      if (this.host.isConnected() && (!this.host.rpcOptionsSupported() || !this.host.hasInstance())) {
        throw this.signalUnsupported();
      }
      throw this.host.createError(errorCodes.cancelled, 'cancelled');
    }
    await raceCall(
      this.host.ready(deadline),
      deadline,
      signal,
      (): any => this.host.createError(errorCodes.timeout, 'timeout'),
      (): any => this.host.createError(errorCodes.cancelled, 'cancelled'),
    );
    // Checked once connected: the capability belongs to the connection the call goes out on.
    // Only a call with an instance is tracked: the server scopes the retransmit and the cancel by it.
    if (this.host.rpcOptionsSupported() && this.host.hasInstance()) {
      return this.track(method, data, headers, signal, deadline);
    }
    if (!this.host.rpcOptionsSupported() && hasHeaders(headers)) {
      throw this.host.createError(errorCodes.unsupported, 'rpc headers are not supported by the server');
    }
    if (signal !== undefined) {
      throw this.signalUnsupported();
    }
    const cmd: any = {
      rpc: {
        method,
        data
      }
    };
    if (this.host.rpcOptionsSupported() && hasHeaders(headers)) {
      cmd.rpc.headers = headers;
    }
    const result: any = await this.host.plainCall(cmd);
    return {
      data: result.data,
      cached: result.cached === true
    };
  }


  // The error of a call with a signal on a connection that cannot carry it.
  private signalUnsupported(): any {
    return this.host.createError(
      errorCodes.unsupported,
      this.host.rpcOptionsSupported() ? 'rpc cancel needs the instance option' : 'rpc cancel is not supported by the server'
    );
  }


  // Sends the call on the current connection. The command id is new for every attempt, the
  // message id is the call's.
  private send(call: TrackedRpc, retransmit: boolean): void {
    // A send supersedes a delayed attempt of the call.
    if (call.resend !== null) {
      call.resend.clear();
      call.resend = null;
    }
    if (!this.host.isConnected()) {
      // Not written: the call waits for the connection, and sendWaiting sends it there.
      call.commandId = 0;
      return;
    }
    const rpc: any = {
      method: call.method,
      data: call.data,
      msg_id: call.msgId,
    };
    if (hasHeaders(call.headers)) {
      rpc.headers = call.headers;
    }
    if (retransmit) {
      rpc.retransmit = true;
    }
    this.dispatch(rpc, (id: number, acks: number[]): RpcCallbacks => {
      call.commandId = id;
      call.sent = true;
      return {
        callback: (ctx: CallResolveContext): void => this.replied(call, ctx),
        errback: (ctx: CallRejectContext): void => this.failed(call, ctx),
        lost: (): void => this.lost(call, acks),
        timeout: null,
      };
    });
  }


  // Adds a command to the connection under a new command id, with the acknowledgements received so far.
  // The callbacks are made by the caller from the id and the acknowledgements; the id is returned.
  private dispatch(rpc: any, callbacks: (id: number, acks: number[]) => RpcCallbacks): number {
    const id: number = this.host.nextCommandId();
    const acks: number[] = this.takeAcks(rpc);
    this.host.setCallbacks(id, callbacks(id, acks));
    this.host.addCommand({ id: id, rpc: rpc });
    return id;
  }


  // Sets the message ids received so far as the acknowledgement of a command, and returns them.
  private takeAcks(rpc: any): number[] {
    const acks: number[] = this.queue.takeAcks();
    if (acks.length > 0) {
      rpc.ack = acks;
    }
    return acks;
  }


  // Puts the acknowledgements of a command that was not delivered back, so the next command carries them.
  private restoreAcks(acks: number[]): void {
    for (const msgId of acks) {
      this.queue.addAck(msgId);
    }
  }


  // Sends a cancel of a call. The reply to a cancel carries nothing the client needs. A cancel that
  // is lost before the server got it is kept, and sent with the next connection. Its callbacks stay
  // for one call timeout, so a slow reply does not lose the cancel; a cancel unanswered by then is not
  // sent again.
  private cancel(msgId: number): void {
    const rpc: any = { cancel: msgId };
    this.dispatch(rpc, (id: number, acks: number[]): RpcCallbacks => ({
      callback: (ctx: CallResolveContext): void => ctx.next?.(),
      errback: (ctx: CallRejectContext): void => ctx.next?.(),
      lost: (): void => {
        this.keepCancel(msgId);
        this.restoreAcks(acks);
      },
      timeout: callTimer(Date.now() + this.callTimeout, (): void => this.host.deleteCallbacks(id)),
    }));
  }


  // Keeps a cancel for the next connection. At most maxPendingRpc are kept: the oldest is dropped
  // for a new one, the server expires the record of an old call anyway.
  private keepCancel(msgId: number): void {
    if (this.cancels.length >= this.maxPendingRpc) {
      this.cancels.shift();
    }
    this.cancels.push(msgId);
  }


  // The callbacks of a command whose call settled locally stay for one call timeout, to acknowledge
  // a late reply. Then they are dropped, so a server that never replies does not grow them. The timers
  // are owned here, and cleared when the client disconnects for good.
  private forgetCommand(id: number): void {
    const timer: CallTimer = callTimer(Date.now() + this.callTimeout, (): void => {
      this.forgets.delete(timer);
      this.host.deleteCallbacks(id);
    });
    this.forgets.add(timer);
  }


  // A reply of the server: it settles the call, or only acknowledges it when the call settled
  // before (aborted or expired).
  private replied(call: TrackedRpc, ctx: CallResolveContext): void {
    this.answered(call, ctx, (): void => {
      const result: any = ctx.reply.rpc || {};
      call.resolve({
        data: result.data,
        cached: result.cached === true,
      });
    });
  }


  // An error reply of the server. A refusal with the retry signal did not run the call: it is sent again,
  // and the error is not acknowledged (the server did not record it). Any other error is surfaced unchanged.
  private failed(call: TrackedRpc, ctx: CallRejectContext): void {
    if (!call.settled && this.resend(call, retrySignal(ctx.error))) {
      ctx.next?.();
      return;
    }
    this.answered(call, ctx, (): void => call.reject(ctx.error));
  }


  // Sends a refused call again, within its attempts. Returns false when the error has to be surfaced.
  private resend(call: TrackedRpc, signal: RetrySignal): boolean {
    if (signal === rpcRetryNewMsgId && call.newIdResends < maxNewIdResends) {
      // Past the deadline the error is surfaced: every resend is inside it.
      if (call.deadline <= Date.now()) {
        return false;
      }
      call.newIdResends++;
      // The refused id was never recorded by the server: the call takes an id of the corrected clock,
      // and it is not sent yet until the new id goes out.
      this.msgIds.resync(this.host.serverTime());
      call.msgId = this.msgIds.next(this.host.serverTime());
      call.sent = false;
      this.send(call, false);
      return true;
    }
    if (signal === rpcRetryLater && call.laterResends < maxLaterResends) {
      const delay: number = laterResendBase * 2 ** call.laterResends;
      call.laterResends++;
      // Waits as a waiting call: a connection lost meanwhile sends it with the reconnect.
      call.commandId = 0;
      if (call.resend !== null) {
        call.resend.clear();
      }
      call.resend = callTimer(Math.min(Date.now() + delay, call.deadline), (): void => this.resendLater(call));
      return true;
    }
    return false;
  }


  // The delay of a later attempt is over. The deadline timer settles the call once the deadline passed.
  private resendLater(call: TrackedRpc): void {
    call.resend = null;
    if (call.settled || call.deadline <= Date.now()) {
      return;
    }
    if (call.commandId === 0 && this.host.isConnected()) {
      this.send(call, true);
    }
  }


  // Shared by replies and errors: acknowledges the call, and settles it with the outcome unless it
  // settled before.
  private answered(call: TrackedRpc, ctx: { next?: () => void }, outcome: () => void): void {
    this.queue.addAck(call.msgId);
    if (!call.settled) {
      this.settle(call);
      outcome();
    }
    ctx.next?.();
  }


  // The connection was lost before the reply: the call waits for the next connection. The
  // acknowledgements of the command that carried it were not delivered, so they are sent again.
  private lost(call: TrackedRpc, acks: number[]): void {
    call.commandId = 0;
    this.restoreAcks(acks);
  }


  // The deadline of the call passed: a call that went out gets a cancel, as one that was aborted.
  private expire(call: TrackedRpc): void {
    this.end(call, errorCodes.timeout, 'timeout');
  }


  // The signal of the call aborted: the call settles locally, and the server gets a cancel when it may
  // have received the call.
  private abort(call: TrackedRpc): void {
    this.end(call, errorCodes.cancelled, 'cancelled');
  }


  // Settles a call that is not settled yet with an error. A call that went out gets a cancel, on the
  // current connection or with the next one: the server may have received it.
  private end(call: TrackedRpc, code: number, message: string): void {
    if (call.settled) {
      return;
    }
    const sent: boolean = call.sent;
    this.settleSent(call);
    if (sent) {
      this.cancelSent(call.msgId);
    }
    call.reject(this.host.createError(code, message));
  }


  // Cancels a sent call: now on the connection when it carries cancels, else with the next connection
  // that does (the server may have received the call before the connection was lost).
  private cancelSent(msgId: number): void {
    if (this.host.isConnected() && this.host.rpcOptionsSupported()) {
      this.cancel(msgId);
    } else {
      this.keepCancel(msgId);
    }
  }


  // Settles a call that may wait for its reply on the current connection (see forgetCommand).
  private settleSent(call: TrackedRpc): void {
    const id: number = call.commandId;
    this.settle(call);
    if (id > 0) {
      this.forgetCommand(id);
    }
  }


  // Settles the call: no timer, no abort listener, not tracked anymore.
  private settle(call: TrackedRpc): void {
    call.settled = true;
    if (call.timer !== null) {
      call.timer.clear();
      call.timer = null;
    }
    if (call.resend !== null) {
      call.resend.clear();
      call.resend = null;
    }
    call.detach();
    this.queue.remove(call);
  }
}
