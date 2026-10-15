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
