import { MsgIdGenerator, RpcQueue, TrackedRpc, msgIdSeqBits } from './rpc_queue';

const seqLimit: number = 2 ** msgIdSeqBits;

function trackedCall(msgId: number, commandId: number): TrackedRpc {
  return {
    msgId: msgId,
    method: 'x',
    data: null,
    headers: undefined,
    deadline: 0,
    commandId: commandId,
    timer: null,
    settled: false,
    detach: () => {},
    resolve: () => {},
    reject: () => {},
  };
}

describe('MsgIdGenerator', () => {
  test('the id is the unix second shifted by the sequence bits, plus the sequence', () => {
    const gen = new MsgIdGenerator();
    const id: number = gen.next(1_700_000_000_500);
    expect(id).toBe(1_700_000_000 * seqLimit);
  });

  test('ids in the same second are consecutive and increasing', () => {
    const gen = new MsgIdGenerator();
    const first: number = gen.next(1_700_000_000_000);
    const second: number = gen.next(1_700_000_000_000);
    const third: number = gen.next(1_700_000_000_999);
    expect(second).toBe(first + 1);
    expect(third).toBe(first + 2);
  });

  test('ids stay increasing when the clock goes back', () => {
    const gen = new MsgIdGenerator();
    const first: number = gen.next(1_700_000_005_000);
    const second: number = gen.next(1_700_000_001_000);
    expect(second).toBeGreaterThan(first);
    expect(Math.floor(second / seqLimit)).toBe(1_700_000_005);
  });

  test('a second that runs out of sequence moves to the next second', () => {
    const gen = new MsgIdGenerator();
    const seconds: number = 1_700_000_000;
    let last: number = gen.next(seconds * 1000);
    for (let i = 1; i < seqLimit; i++) {
      const id: number = gen.next(seconds * 1000);
      expect(id).toBeGreaterThan(last);
      last = id;
    }
    // The sequence of the second is used up: the next id is in the following second.
    const next: number = gen.next(seconds * 1000);
    expect(next).toBeGreaterThan(last);
    expect(Math.floor(next / seqLimit)).toBe(seconds + 1);
    expect(next % seqLimit).toBe(0);
  });

  test('ids fit a JavaScript safe integer until the year 2242', () => {
    const gen = new MsgIdGenerator();
    const year2242Ms: number = Date.UTC(2242, 0, 1);
    const id: number = gen.next(year2242Ms);
    expect(Number.isSafeInteger(id)).toBe(true);
    expect(id).toBeLessThan(Number.MAX_SAFE_INTEGER);
  });
});

describe('RpcQueue', () => {
  test('waiting calls are the calls without a command id, in message id order', () => {
    const queue = new RpcQueue();
    queue.add(trackedCall(1, 5));
    queue.add(trackedCall(2, 0));
    queue.add(trackedCall(3, 0));
    queue.add(trackedCall(4, 6));
    expect(queue.waiting().map(call => call.msgId)).toEqual([2, 3]);
  });

  test('size counts the calls not removed', () => {
    const queue = new RpcQueue();
    queue.add(trackedCall(1, 1));
    queue.add(trackedCall(2, 2));
    expect(queue.size).toBe(2);
    queue.remove(1);
    expect(queue.size).toBe(1);
    expect(queue.get(2)?.msgId).toBe(2);
    expect(queue.get(1)).toBeUndefined();
  });

  test('clear returns all calls in message id order and empties the queue', () => {
    const queue = new RpcQueue();
    queue.add(trackedCall(7, 1));
    queue.add(trackedCall(9, 0));
    expect(queue.clear().map(call => call.msgId)).toEqual([7, 9]);
    expect(queue.size).toBe(0);
  });

  test('acks are taken once', () => {
    const queue = new RpcQueue();
    queue.addAck(10);
    queue.addAck(11);
    expect(queue.takeAcks()).toEqual([10, 11]);
    expect(queue.takeAcks()).toEqual([]);
  });
});
