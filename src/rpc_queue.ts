import { RpcResult } from './types';

/** Bits of the sequence part of a message id. */
export const msgIdSeqBits: number = 20;

const msgIdSeqLimit: number = 2 ** msgIdSeqBits;

/** Largest time part of a message id: the bits left of a safe integer after the sequence. */
export const msgIdMaxSeconds: number = 2 ** 33 - 1;

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
  commandId: number;
  timer: ReturnType<typeof setTimeout> | null;
  // Set once the promise settled: a reply that still comes for it only acknowledges it.
  settled: boolean;
  // Removes the abort listener of the call's signal, if any.
  detach: () => void;
  resolve: (result: RpcResult) => void;
  reject: (error: any) => void;
}

/**
 * Generates message ids as unix seconds << 20 | seq. The ids are strictly increasing per
 * generator, and their 53 bits stay exact in a JavaScript number. The time part follows the
 * server time given, but never goes back: a clock that moves backwards keeps the last second.
 * A time beyond msgIdMaxSeconds (the year 2242) is clamped to it.
 */
export class MsgIdGenerator {
  private lastSeconds: number = -1;
  private lastSeq: number = 0;

  /** Next message id for the server time in milliseconds. Throws a RangeError once the id space is used up. */
  next(serverTimeMs: number): number {
    let seconds: number = Math.min(Math.floor(serverTimeMs / 1000), msgIdMaxSeconds);
    if (seconds < this.lastSeconds) {
      seconds = this.lastSeconds;
    }
    let seq: number = 0;
    if (seconds === this.lastSeconds) {
      seq = this.lastSeq + 1;
      if (seq >= msgIdSeqLimit) {
        // More calls in one second than the sequence holds: move on to the next second.
        if (seconds >= msgIdMaxSeconds) {
          throw new RangeError('message id space exhausted');
        }
        seconds += 1;
        seq = 0;
      }
    }
    this.lastSeconds = seconds;
    this.lastSeq = seq;
    return seconds * msgIdSeqLimit + seq;
  }
}

/**
 * The tracked rpc calls of a client, in message id order, and the message ids of the replies
 * the client received, to acknowledge with its next request.
 */
export class RpcQueue {
  private calls: Map<number, TrackedRpc> = new Map();
  private acks: number[] = [];

  /** Number of calls not settled yet. */
  get size(): number {
    return this.calls.size;
  }

  /** Registers a call. Calls are added in increasing message id order. */
  add(call: TrackedRpc): void {
    this.calls.set(call.msgId, call);
  }

  /** Removes a call, e.g. when it settled. */
  remove(msgId: number): void {
    this.calls.delete(msgId);
  }

  /** The calls waiting for a connection, in message id order. */
  waiting(): TrackedRpc[] {
    const result: TrackedRpc[] = [];
    for (const call of this.calls.values()) {
      if (call.commandId === 0) {
        result.push(call);
      }
    }
    return result;
  }

  /** Removes all calls and returns them, in message id order. */
  clear(): TrackedRpc[] {
    const result: TrackedRpc[] = Array.from(this.calls.values());
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
}
