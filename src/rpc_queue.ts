import { CallTimer } from './call_timer';
import { RpcResult } from './types';

/** Bits of the sequence part of a message id. */
const msgIdSeqBits: number = 20;

const msgIdSeqLimit: number = 2 ** msgIdSeqBits;

/** Largest time part of a message id: the bits left of a safe integer after the sequence. */
export const msgIdMaxSeconds: number = 2 ** 33 - 1;

/** Lanes are drawn from [0, msgIdLaneLimit): a second always holds at least this many ids. */
const msgIdLaneLimit: number = 2 ** 19;

// A random lane: a sequence of a second starts there, so two generators in one second rarely share ids.
function drawLane(): number {
  return Math.floor(Math.random() * msgIdLaneLimit);
}

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

/**
 * Generates message ids as unix seconds << 20 | seq, unique per generator within the server window.
 * The sequence of a second starts at a random lane, so generators created in the same second do not
 * share ids. The time part follows the server time given, but never goes back: a clock that moves
 * backwards keeps the last second. A time beyond msgIdMaxSeconds (the year 2242) is clamped to it.
 * The 53 bits of an id stay exact in a JavaScript number.
 */
export class MsgIdGenerator {
  private lane: number = drawLane();
  private lastSeconds: number = -1;
  private lastSeq: number = 0;

  /** Next message id for the server time in milliseconds. Throws a RangeError once the id space is used up. */
  next(serverTimeMs: number): number {
    let seconds: number = this.clampSeconds(serverTimeMs);
    if (seconds < this.lastSeconds) {
      seconds = this.lastSeconds;
    }
    let seq: number = this.lane;
    if (seconds === this.lastSeconds) {
      seq = this.lastSeq + 1;
      if (seq >= msgIdSeqLimit) {
        // More calls in one second than the sequence holds: move on to the next second.
        if (seconds >= msgIdMaxSeconds) {
          throw new RangeError('message id space exhausted');
        }
        seconds += 1;
        seq = this.lane;
      }
    }
    this.lastSeconds = seconds;
    this.lastSeq = seq;
    return seconds * msgIdSeqLimit + seq;
  }

  /**
   * Re-syncs the generator to the corrected server time, after the server refused an id as outside its
   * window: the next id takes the time part of that second and a fresh lane. Only the server's signal
   * calls this, since the window of the server is not known otherwise. A second already in use keeps its
   * sequence. The seconds re-covered after a correction backwards may hold ids the server accepted before,
   * so a collision there is bounded by the lane overlap, not excluded.
   */
  resync(serverTimeMs: number): void {
    const seconds: number = this.clampSeconds(serverTimeMs);
    if (seconds === this.lastSeconds) {
      // The second is in use: its sequence goes on, so no id of it is issued twice.
      return;
    }
    this.lane = drawLane();
    this.lastSeconds = seconds;
    this.lastSeq = this.lane - 1;
  }

  private clampSeconds(serverTimeMs: number): number {
    return Math.min(Math.floor(serverTimeMs / 1000), msgIdMaxSeconds);
  }
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
