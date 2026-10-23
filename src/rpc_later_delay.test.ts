import { laterResendDelay } from './rpc_tracker';

// The later attempt of a refused call waits a uniform delay over [0, 250 ms * 2^n] (full jitter). The range is capped at
// the time left before the deadline of the call, so a draw never waits past it.
describe('the delay of a later attempt', (): void => {
  afterEach((): void => {
    jest.restoreAllMocks();
  });

  test('laterResendDelayIsCappedAtTheRemainingDeadline', (): void => {
    const random: jest.SpyInstance = jest.spyOn(Math, 'random').mockReturnValue(0.9999999);

    // The bound 250 ms * 2^2 = 1000 ms is above the 300 ms left: the draw at its top gives the time left.
    expect(laterResendDelay(2, 300)).toBe(300);
    // The bound 250 ms is below the time left: the draw at its top gives the bound, as before the cap.
    expect(laterResendDelay(0, 5000)).toBe(250);

    random.mockReturnValue(0);
    expect(laterResendDelay(2, 300)).toBe(0);
  });
});
