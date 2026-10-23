import { refreshRetryCeiling, refreshRetryDelay } from './refresh_retry';

// The retry after a failed timer refresh, of the connection and of a subscription: the delay is 10 s plus a draw over
// the growing ceiling, the draw at 0 gives 10 s and the draw at its top gives the ceiling of the step, which doubles
// from 20 s up to tokenRetryCap. The cap of the default option is 300 s.
describe('the delay of a retry after a failed timer refresh', (): void => {
  const defaultCap: number = 300000;

  afterEach((): void => {
    jest.restoreAllMocks();
  });

  test('retryDelayEscalatesAndStopsAtTheCap', (): void => {
    const random: jest.SpyInstance = jest.spyOn(Math, 'random');
    const expected: Array<[number, number, number]> = [
      [0, 10000, 20000],
      [1, 10000, 30000],
      [2, 10000, 50000],
      [3, 10000, 90000],
      [4, 10000, 170000],
      [5, 10000, 300000],
    ];

    for (const [step, low, high] of expected) {
      random.mockReturnValue(0);
      expect(refreshRetryDelay(step, defaultCap)).toBe(low);
      random.mockReturnValue(0.9999999);
      expect(refreshRetryDelay(step, defaultCap)).toBe(high);
    }
    expect(refreshRetryCeiling(5, defaultCap)).toBe(300000);
    expect(refreshRetryCeiling(40, defaultCap)).toBe(300000);
  });

  test('a cap below 20 seconds keeps the first ceiling of 20 seconds', (): void => {
    jest.spyOn(Math, 'random').mockReturnValue(0.9999999);

    expect(refreshRetryCeiling(0, 10000)).toBe(20000);
    expect(refreshRetryDelay(3, 10000)).toBe(20000);
  });
});
