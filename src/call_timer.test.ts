import { callTimer, CallTimer } from './call_timer';

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
