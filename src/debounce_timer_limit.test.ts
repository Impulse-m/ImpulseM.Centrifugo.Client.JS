import { Subscription } from './subscription';
import { maxTimerDelay } from './call_timer';

// A server may send a publish debounce longer than the longest timer. A timer cannot wait for more than the longest
// timer, so the window closes at the longest timer: it sends the pending publish and restarts, and it does not close
// at once.

describe('publish debounce above the longest timer', () => {
  beforeEach(() => {
    jest.useFakeTimers();
  });

  afterEach(() => {
    jest.useRealTimers();
  });

  test('a window longer than the longest timer closes at the longest timer, and not before', () => {
    const publish: jest.Mock = jest.fn((): Promise<{}> => Promise.resolve({}));
    const centrifuge: any = { publish: publish, mapPublish: jest.fn() };
    const sub: any = new Subscription(centrifuge, 'debounced');
    sub._debounceMs = maxTimerDelay + 1000;

    sub._debouncedPublish('', 1, false);
    sub._debouncedPublish('', 2, false);
    expect(publish).toHaveBeenCalledTimes(1);

    jest.advanceTimersByTime(maxTimerDelay - 1);
    expect(publish).toHaveBeenCalledTimes(1);

    jest.advanceTimersByTime(1);
    expect(publish).toHaveBeenCalledTimes(2);
    expect(publish).toHaveBeenLastCalledWith('debounced', 2);
  });
});
