import { CallTimer } from './call_timer';
import type { CallRequest } from './rpc_tracker';
import { RpcResult } from './types';

/**
 * A tracked rpc call: registered with a message id, from the call until it settles. It is
 * sent on the current connection (commandId is set), or it waits for the next connection
 * to be sent again (commandId is 0).
 */
export interface TrackedRpc {
  msgId: number;
  method: string;
  data: any;
  headers: Record<string, string> | undefined;
  // Local time in milliseconds after which the call rejects with a timeout, across all attempts.
  deadline: number;
  // The call as the application made it: sent as a plain request when it was never written on a connection
  // that carries the capability.
  request: CallRequest;
  commandId: number;
  // Set once the call went out on a connection, for the first time or as a retransmit.
  sent: boolean;
  timer: CallTimer | null;
  // Set once the promise settled: a reply that still comes for it only acknowledges it.
  settled: boolean;
  // Sends of the call refused with a new message id, and later attempts, so far (see RpcTracker).
  newIdResends: number;
  laterResends: number;
  // The timer of a later attempt that waits, see RpcTracker.
  resend: CallTimer | null;
  // Removes the abort listener of the call's signal, if any.
  detach: () => void;
  // Gives back the slot of maxPendingRpc the call holds: once, when the call settles.
  release: () => void;
  resolve: (result: RpcResult) => void;
  reject: (error: any) => void;
}

// Sorts calls by message id: the order the calls are sent in on a connection.
function byMsgId(calls: TrackedRpc[]): TrackedRpc[] {
  return calls.sort((a: TrackedRpc, b: TrackedRpc): number => a.msgId - b.msgId);
}

/**
 * The tracked rpc calls of a client, and the message ids of the replies the client received, to
 * acknowledge with its next request. Calls are kept by identity: a call with a message id that
 * another call has too is tracked on its own.
 */
export class RpcQueue {
  private calls: Set<TrackedRpc> = new Set();
  private acks: number[] = [];

  /** Registers a call. */
  add(call: TrackedRpc): void {
    this.calls.add(call);
  }

  /** Removes a call, e.g. when it settled. */
  remove(call: TrackedRpc): void {
    this.calls.delete(call);
  }

  /** The calls waiting for a connection, in message id order. */
  waiting(): TrackedRpc[] {
    return byMsgId(Array.from(this.calls).filter((call: TrackedRpc): boolean => call.commandId === 0));
  }

  /** Removes all calls and returns them, in message id order. */
  clear(): TrackedRpc[] {
    const result: TrackedRpc[] = byMsgId(Array.from(this.calls));
    this.calls.clear();
    return result;
  }

  /** Records the message id of a reply received, to acknowledge with the next request. */
  addAck(msgId: number): void {
    this.acks.push(msgId);
  }

  /** The message ids to acknowledge, emptied: each id is sent once. */
  takeAcks(): number[] {
    const result: number[] = this.acks;
    this.acks = [];
    return result;
  }

  /** Drops the message ids to acknowledge, e.g. those of a session that ended. */
  clearAcks(): void {
    this.acks = [];
  }
}
