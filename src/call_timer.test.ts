import { addWaiter, callTimer, CallTimer, raceCall } from './call_timer';
import { errorCodes } from './codes';

describe('callTimer', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('runs at the due time when the timer is on time', () => {
    const onDue: jest.Mock = jest.fn();
    callTimer(Date.now() + 100, onDue);

    jest.advanceTimersByTime(99);
    expect(onDue).not.toHaveBeenCalled();
    jest.advanceTimersByTime(5);
    expect(onDue).toHaveBeenCalledTimes(1);
  });

  test('waits a grace period when the timer runs late, so data already received is processed first', () => {
    const onDue: jest.Mock = jest.fn();
    callTimer(Date.now() - 2000, onDue);

    jest.advanceTimersByTime(999);
    expect(onDue).not.toHaveBeenCalled();
    jest.advanceTimersByTime(5);
    expect(onDue).toHaveBeenCalledTimes(1);
  });

  test('does not run once cleared, also during the grace period', () => {
    const onDue: jest.Mock = jest.fn();
    const timer: CallTimer = callTimer(Date.now() - 2000, onDue);

    jest.advanceTimersByTime(0);
    timer.clear();
    jest.advanceTimersByTime(5000);
    expect(onDue).not.toHaveBeenCalled();
  });
});

describe('addWaiter', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('a waiter that is resolved leaves the waiters, with its timer and its abort listener', () => {
    const waiters: Record<number, any> = {};
    const controller: AbortController = new AbortController();
    const resolve: jest.Mock = jest.fn();
    addWaiter(waiters, 1, 100, controller.signal, resolve, jest.fn());

    waiters[1].resolve(undefined);

    expect(resolve).toHaveBeenCalledTimes(1);
    expect(waiters).toEqual({});
    expect(jest.getTimerCount()).toBe(0);
  });

  test('a waiter that times out rejects with the timeout and leaves the waiters', () => {
    const waiters: Record<number, any> = {};
    const reject: jest.Mock = jest.fn();
    addWaiter(waiters, 1, 100, undefined, jest.fn(), reject);

    jest.advanceTimersByTime(101);

    expect(reject).toHaveBeenCalledWith({ code: errorCodes.timeout, message: 'timeout' });
    expect(waiters).toEqual({});
  });

  test('a waiter whose signal aborts rejects with cancelled and leaves the waiters, with its timer', () => {
    const waiters: Record<number, any> = {};
    const controller: AbortController = new AbortController();
    const reject: jest.Mock = jest.fn();
    addWaiter(waiters, 1, 100, controller.signal, jest.fn(), reject);

    controller.abort();

    expect(reject).toHaveBeenCalledWith({ code: errorCodes.cancelled, message: 'cancelled' });
    expect(waiters).toEqual({});
    expect(jest.getTimerCount()).toBe(0);
  });

  test('a waiter with no timeout waits until it is settled', () => {
    const waiters: Record<number, any> = {};
    const resolve: jest.Mock = jest.fn();
    addWaiter(waiters, 1, null, undefined, resolve, jest.fn());

    jest.advanceTimersByTime(60000);
    expect(resolve).not.toHaveBeenCalled();
    expect(Object.keys(waiters)).toHaveLength(1);
  });

  test('a waiter whose signal is aborted already rejects at once, leaves no waiter and arms no timer', () => {
    const waiters: Record<number, any> = {};
    const controller: AbortController = new AbortController();
    controller.abort();
    const reject: jest.Mock = jest.fn();
    addWaiter(waiters, 1, 100, controller.signal, jest.fn(), reject);

    expect(reject).toHaveBeenCalledWith({ code: errorCodes.cancelled, message: 'cancelled' });
    expect(waiters).toEqual({});
    expect(jest.getTimerCount()).toBe(0);
  });
});

describe('raceCall', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('settles with the promise and clears its timer', async () => {
    const race: Promise<number> = raceCall(Promise.resolve(7), Date.now() + 100, undefined, () => 'timeout', () => 'cancelled');

    await expect(race).resolves.toBe(7);
    expect(jest.getTimerCount()).toBe(0);
  });

  test('rejects with the timeout error at the deadline', async () => {
    const race: Promise<number> = raceCall(new Promise<number>(() => {}), Date.now() + 100, undefined, () => 'timeout', () => 'cancelled');

    jest.advanceTimersByTime(101);

    await expect(race).rejects.toBe('timeout');
  });

  test('rejects with the cancelled error at once when the signal is aborted already, and arms no timer', async () => {
    const controller: AbortController = new AbortController();
    controller.abort();
    const race: Promise<number> = raceCall(new Promise<number>(() => {}), Date.now() + 100, controller.signal, () => 'timeout', () => 'cancelled');

    await expect(race).rejects.toBe('cancelled');
    expect(jest.getTimerCount()).toBe(0);
  });
});
