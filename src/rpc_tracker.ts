import { errorCodes } from './codes';
import { CallTimer, callTimer, hasHeaders, raceCall } from './extensions';
import { MsgIdGenerator, RpcQueue, TrackedRpc } from './rpc_queue';
import type { CallRejectContext, CallResolveContext } from './centrifuge';
import { RpcOptions, RpcResult } from './types';


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


  constructor(host: RpcHost, maxPendingRpc: number, callTimeout: number) {
    this.host = host;
    this.maxPendingRpc = maxPendingRpc;
    this.callTimeout = callTimeout;
  }


  /**
   * Runs an rpc call. It waits for the connection and for a refresh in flight, then goes out: tracked
   * when the connection supports RPC options and the client has an instance, else as a plain request.
   * The deadline counts from the rpc() call, across the wait and the attempts.
   */
  async call(method: string, data: any, opts: RpcOptions | undefined, deadline: number): Promise<RpcResult> {
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


  /**
   * Tracks a call: it is sent on the current connection, or on the next one if it is lost. The call
   * rejects with a timeout at its deadline, counted from the moment of the rpc() call. Rejects with
   * errorCodes.limitExceeded when maxPendingRpc calls are tracked already.
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
      if (this.queue.size >= this.maxPendingRpc) {
        reject(this.host.createError(errorCodes.limitExceeded, 'too many pending rpc calls'));
        return;
      }
      const call: TrackedRpc = {
        msgId: this.msgIds.next(this.host.serverTime()),
        method: method,
        data: data,
        headers: headers,
        deadline: deadline,
        commandId: 0,
        timer: null,
        settled: false,
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
    const cancels: number[] = this.cancels;
    this.cancels = [];
    if (!this.host.rpcOptionsSupported()) {
      for (const call of this.queue.waiting()) {
        this.settle(call);
        call.reject(this.host.createError(errorCodes.connectionClosed, 'connection closed'));
      }
      return;
    }
    for (const msgId of cancels) {
      this.cancel(msgId);
    }
    for (const call of this.queue.waiting()) {
      if (call.deadline <= Date.now()) {
        this.expire(call);
      } else {
        this.send(call, true);
      }
    }
  }


  /** Rejects all tracked calls, e.g. on a disconnect that will not be followed by a reconnect. */
  rejectAll(code: number, message: string): void {
    this.cancels = [];
    for (const call of this.queue.clear()) {
      this.settle(call);
      call.reject(this.host.createError(code, message));
    }
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
    const id: number = this.host.nextCommandId();
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
    call.commandId = id;
    const acks: number[] = this.takeAcks(rpc);
    this.host.setCallbacks(id, {
      callback: (ctx: CallResolveContext): void => this.replied(call, ctx),
      errback: (ctx: CallRejectContext): void => this.failed(call, ctx),
      lost: (): void => this.lost(call, acks),
      timeout: null,
    });
    this.host.addCommand({ id: id, rpc: rpc });
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
  // until the reply or the disconnect, so a slow reply does not lose the cancel.
  private cancel(msgId: number): void {
    const id: number = this.host.nextCommandId();
    const rpc: any = { cancel: msgId };
    const acks: number[] = this.takeAcks(rpc);
    this.host.setCallbacks(id, {
      callback: (ctx: CallResolveContext): void => ctx.next?.(),
      errback: (ctx: CallRejectContext): void => ctx.next?.(),
      lost: (): void => {
        this.cancels.push(msgId);
        this.restoreAcks(acks);
      },
      timeout: null,
    });
    this.host.addCommand({ id: id, rpc: rpc });
  }


  // The callbacks of a command whose call settled locally stay for one call timeout, to acknowledge
  // a late reply. Then they are dropped, so a server that never replies does not grow them.
  private forgetCommand(id: number): void {
    setTimeout((): void => this.host.deleteCallbacks(id), this.callTimeout);
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


  // An error reply of the server, surfaced unchanged. It is not retried: the server's codes for a
  // rejected message id are configurable, so there is no signal to retry on.
  private failed(call: TrackedRpc, ctx: CallRejectContext): void {
    this.answered(call, ctx, (): void => call.reject(ctx.error));
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


  private expire(call: TrackedRpc): void {
    call.timer = null;
    if (call.settled) {
      return;
    }
    this.settleSent(call);
    call.reject(this.host.createError(errorCodes.timeout, 'timeout'));
  }


  // The signal of the call aborted: the call settles locally, and the server gets a cancel.
  private abort(call: TrackedRpc): void {
    if (call.settled) {
      return;
    }
    const sent: boolean = call.commandId > 0;
    this.settleSent(call);
    if (sent) {
      this.cancel(call.msgId);
    } else {
      this.cancels.push(call.msgId);
    }
    call.reject(this.host.createError(errorCodes.cancelled, 'cancelled'));
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
    call.detach();
    this.queue.remove(call.msgId);
  }
}
