import { defaultTokenRetryCap, normaliseTokenRetryCap, refreshRetryCeiling, refreshRetryDelay } from './refresh_retry';

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

  test('an unset, null, NaN, negative or infinite tokenRetryCap normalises to the default', (): void => {
    expect(defaultTokenRetryCap).toBe(300000);
    expect(normaliseTokenRetryCap(undefined)).toBe(defaultTokenRetryCap);
    expect(normaliseTokenRetryCap(null as unknown as number)).toBe(defaultTokenRetryCap);
    expect(normaliseTokenRetryCap(Number.NaN)).toBe(defaultTokenRetryCap);
    expect(normaliseTokenRetryCap(-1)).toBe(defaultTokenRetryCap);
    expect(normaliseTokenRetryCap(Number.POSITIVE_INFINITY)).toBe(defaultTokenRetryCap);
    expect(normaliseTokenRetryCap('60000' as unknown as number)).toBe(defaultTokenRetryCap);
  });

  test('a finite tokenRetryCap from 0 up is kept as it is', (): void => {
    expect(normaliseTokenRetryCap(0)).toBe(0);
    expect(normaliseTokenRetryCap(60000)).toBe(60000);
  });

  test('a retry delay with an unset or NaN tokenRetryCap, once normalised, is the default delay, never 0 or NaN', (): void => {
    const random: jest.SpyInstance = jest.spyOn(Math, 'random');
    const cases: Array<number | undefined> = [undefined, Number.NaN];

    for (const tokenRetryCap of cases) {
      const cap: number = normaliseTokenRetryCap(tokenRetryCap);
      random.mockReturnValue(0);
      expect(refreshRetryDelay(0, cap)).toBe(10000);
      random.mockReturnValue(0.9999999);
      expect(refreshRetryDelay(0, cap)).toBe(20000);
      random.mockReturnValue(0.9999999);
      expect(refreshRetryDelay(5, cap)).toBe(300000);
    }
  });
});
