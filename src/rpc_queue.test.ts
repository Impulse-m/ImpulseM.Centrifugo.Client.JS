import { MsgIdGenerator, RpcQueue, TrackedRpc, msgIdMaxSeconds } from './rpc_queue';

// The sequence part of a message id: 20 bits, as the server reads it.
const seqLimit: number = 2 ** 20;

function trackedCall(msgId: number, commandId: number): TrackedRpc {
  return {
    msgId: msgId,
    method: 'x',
    data: null,
    headers: undefined,
    deadline: 0,
    request: { method: 'x', data: null, opts: undefined, deadline: 0, release: (): void => {} },
    commandId: commandId,
    sent: commandId > 0,
    timer: null,
    settled: false,
    newIdResends: 0,
    laterResends: 0,
    resend: null,
    detach: (): void => {},
    release: (): void => {},
    resolve: (): void => {},
    reject: (): void => {},
  };
}

// A generator whose lane is the given one: the lane is drawn from Math.random when it is created.
function generatorWithLane(lane: number): MsgIdGenerator {
  const random: jest.SpyInstance = jest.spyOn(Math, 'random').mockReturnValue(lane / 2 ** 19);
  const gen: MsgIdGenerator = new MsgIdGenerator();
  random.mockRestore();
  return gen;
}

describe('MsgIdGenerator', () => {
  test('the id is the unix second shifted by the sequence bits, plus the lane', () => {
    const gen: MsgIdGenerator = generatorWithLane(5);
    const id: number = gen.next(1_700_000_000_500);
    expect(id).toBe(1_700_000_000 * seqLimit + 5);
  });

  test('ids in the same second are consecutive and increasing', () => {
    const gen: MsgIdGenerator = generatorWithLane(0);
    const first: number = gen.next(1_700_000_000_000);
    const second: number = gen.next(1_700_000_000_000);
    const third: number = gen.next(1_700_000_000_999);
    expect(second).toBe(first + 1);
    expect(third).toBe(first + 2);
  });

  test('a later second restarts the sequence at the lane', () => {
    const gen: MsgIdGenerator = generatorWithLane(9);
    gen.next(1_700_000_000_000);
    gen.next(1_700_000_000_000);
    const id: number = gen.next(1_700_000_001_000);
    expect(id).toBe(1_700_000_001 * seqLimit + 9);
  });

  test('a clock that goes back keeps the time part and the ids keep increasing', () => {
    const gen: MsgIdGenerator = generatorWithLane(0);
    const first: number = gen.next(1_700_000_005_000);
    const second: number = gen.next(1_700_000_001_000);
    expect(second).toBeGreaterThan(first);
    expect(Math.floor(second / seqLimit)).toBe(1_700_000_005);
  });

  test('a second that runs out of sequence moves to the next second, at the lane', () => {
    const lane: number = 3;
    const gen: MsgIdGenerator = generatorWithLane(lane);
    const seconds: number = 1_700_000_000;
    let last: number = gen.next(seconds * 1000);
    for (let i: number = 1; i < seqLimit - lane; i++) {
      const id: number = gen.next(seconds * 1000);
      expect(id).toBe(last + 1);
      last = id;
    }
    // The sequence of the second is used up: the next id is in the following second, at the lane.
    const next: number = gen.next(seconds * 1000);
    expect(next).toBeGreaterThan(last);
    expect(next).toBe((seconds + 1) * seqLimit + lane);
  });

  test('resync lowers the time part to the corrected second and takes a fresh lane', () => {
    const gen: MsgIdGenerator = generatorWithLane(0);
    const seconds: number = 1_700_000_000;
    gen.next((seconds + 120) * 1000);
    const random: jest.SpyInstance = jest.spyOn(Math, 'random').mockReturnValue(7 / 2 ** 19);
    gen.resync(seconds * 1000);
    random.mockRestore();
    const id: number = gen.next(seconds * 1000);
    expect(Math.floor(id / seqLimit)).toBe(seconds);
    expect(id % seqLimit).toBe(7);
  });

  test('resync in the second in use keeps the sequence, so no id is issued twice', () => {
    const gen: MsgIdGenerator = generatorWithLane(3);
    const seconds: number = 1_700_000_000;
    const issued: number[] = [gen.next(seconds * 1000), gen.next(seconds * 1000)];
    const random: jest.SpyInstance = jest.spyOn(Math, 'random').mockReturnValue(9 / 2 ** 19);
    gen.resync(seconds * 1000);
    random.mockRestore();
    const next: number = gen.next(seconds * 1000);
    expect(next).toBe(issued[1] + 1);
    expect(issued).not.toContain(next);
  });

  test('two generators created in the same second give different first ids', () => {
    for (let i: number = 0; i < 20; i++) {
      const first: MsgIdGenerator = new MsgIdGenerator();
      const second: MsgIdGenerator = new MsgIdGenerator();
      expect(first.next(1_700_000_000_000)).not.toBe(second.next(1_700_000_000_000));
    }
  });

  test('ids fit a JavaScript safe integer until the year 2242', () => {
    const gen: MsgIdGenerator = new MsgIdGenerator();
    const year2242Ms: number = Date.UTC(2242, 0, 1);
    const id: number = gen.next(year2242Ms);
    expect(Number.isSafeInteger(id)).toBe(true);
    expect(id).toBeLessThan(Number.MAX_SAFE_INTEGER);
  });

  test('a server time beyond the year 2242 is clamped, and the ids stay safe integers', () => {
    const gen: MsgIdGenerator = new MsgIdGenerator();
    const farFutureMs: number = Date.UTC(3000, 0, 1);
    const first: number = gen.next(farFutureMs);
    const second: number = gen.next(farFutureMs);
    expect(Math.floor(first / seqLimit)).toBe(msgIdMaxSeconds);
    expect(Number.isSafeInteger(first)).toBe(true);
    expect(Number.isSafeInteger(second)).toBe(true);
    expect(second).toBe(first + 1);
    expect(second).toBeLessThanOrEqual(Number.MAX_SAFE_INTEGER);
  });

  test('a clamped second that runs out of sequence throws instead of losing precision', () => {
    const gen: MsgIdGenerator = generatorWithLane(0);
    const farFutureMs: number = Date.UTC(3000, 0, 1);
    gen.next(farFutureMs);
    for (let i: number = 1; i < seqLimit; i++) {
      gen.next(farFutureMs);
    }
    expect(() => gen.next(farFutureMs)).toThrow(RangeError);
  });
});

describe('RpcQueue', () => {
  test('waiting calls are the calls without a command id, in message id order', () => {
    const queue: RpcQueue = new RpcQueue();
    queue.add(trackedCall(1, 5));
    queue.add(trackedCall(2, 0));
    queue.add(trackedCall(3, 0));
    queue.add(trackedCall(4, 6));
    expect(queue.waiting().map((call: TrackedRpc): number => call.msgId)).toEqual([2, 3]);
  });

  test('a call is removed by its identity, so calls sharing a message id stay apart', () => {
    const queue: RpcQueue = new RpcQueue();
    const first: TrackedRpc = trackedCall(5, 1);
    const second: TrackedRpc = trackedCall(5, 0);
    queue.add(first);
    queue.add(second);
    queue.remove(first);
    expect(queue.waiting()).toEqual([second]);
    expect(queue.clear()).toEqual([second]);
  });

  test('clear returns all calls in message id order and empties the queue', () => {
    const queue: RpcQueue = new RpcQueue();
    queue.add(trackedCall(7, 1));
    queue.add(trackedCall(9, 0));
    expect(queue.clear().map((call: TrackedRpc): number => call.msgId)).toEqual([7, 9]);
    expect(queue.clear()).toEqual([]);
  });

  test('waiting calls stay in message id order when a call is added behind a higher id', () => {
    const queue: RpcQueue = new RpcQueue();
    queue.add(trackedCall(200, 0));
    queue.add(trackedCall(150, 0));
    expect(queue.waiting().map((call: TrackedRpc): number => call.msgId)).toEqual([150, 200]);
  });

  test('acks are taken once', () => {
    const queue: RpcQueue = new RpcQueue();
    queue.addAck(10);
    queue.addAck(11);
    expect(queue.takeAcks()).toEqual([10, 11]);
    expect(queue.takeAcks()).toEqual([]);
  });
});
