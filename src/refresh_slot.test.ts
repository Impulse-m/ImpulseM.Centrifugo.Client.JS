import { refreshKeepsConnection, refreshRetries } from './refresh_slot';

// The policy of a timer-driven refresh for each server error: retried, kept, or disconnected. An explicit
// refresh() is never retried, so it does not use this policy.

describe('timer-driven refresh errors', (): void => {
  test('a temporary error, a client error and 109 are retried with a fresh token', (): void => {
    expect(refreshRetries(100, true)).toBe(true);
    expect(refreshRetries(3, false)).toBe(true);
    expect(refreshRetries(109, false)).toBe(true);
  });

  test('a permanent error with a code of 100 or more is not retried', (): void => {
    expect(refreshRetries(100, false)).toBe(false);
    expect(refreshRetries(103, false)).toBe(false);
    expect(refreshRetries(3000, false)).toBe(false);
  });

  test('103 keeps the connection, and no other code does', (): void => {
    expect(refreshKeepsConnection(103)).toBe(true);
    expect(refreshKeepsConnection(109)).toBe(false);
    expect(refreshKeepsConnection(3000)).toBe(false);
  });
});
