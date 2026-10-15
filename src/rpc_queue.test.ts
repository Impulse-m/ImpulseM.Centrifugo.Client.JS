import { RpcQueue, TrackedRpc } from './rpc_queue';

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
