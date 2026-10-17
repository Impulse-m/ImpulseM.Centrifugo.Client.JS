import { errorCodes } from './codes';
import { refreshKeepsConnection, refreshRetries, RefreshSlot, RefreshSlotHost } from './refresh_slot';

// The policy of a timer-driven refresh for each server error: retried, kept, or disconnected. An explicit
// refresh() is never retried, so it does not use this policy.

// A host of the refresh slot that records the commands sent and the retries armed. The connection state is a flag
// the test sets: a flag set right after a send() call runs in the microtask gap between the claim of the command
// slot and the write of the command, where nothing else of the client runs.
class FakeRefreshHost implements RefreshSlotHost {
  connected: boolean = true;
  sent: any[] = [];
  armed: number = 0;
  private tokenValue: string = 'old';

  isConnected(): boolean {
    return this.connected;
  }

  waitConnected(): Promise<void> {
    return this.connected ? Promise.resolve() : new Promise<void>((): void => {});
  }

  createError(code: number, message: string): any {
    return { code: code, message: message };
  }

  sendCommand(command: any): Promise<any> {
    this.sent.push(command);
    return Promise.resolve({ reply: { refresh: { ttl: 60 } }, next: (): void => {} });
  }

  hasGetToken(): boolean {
    return false;
  }

  getToken(): Promise<string> {
    return Promise.resolve('fetched');
  }

  tokenGeneration(): number {
    return 0;
  }

  token(): string {
    return this.tokenValue;
  }

  setToken(token: string): void {
    this.tokenValue = token;
  }

  debug(): void {}

  applyReply(): void {}

  armRefreshRetry(): void {
    this.armed++;
  }

  identity(): any {
    return { context: (ttl: number): any => ({ ttl: ttl }) };
  }

  dispatchFailed(): void {}
}

describe('a refresh command that finds the client disconnected after its slot is claimed', (): void => {
  test('send() releases the slot and rejects as unsent, without writing the command', async (): Promise<void> => {
    const host: FakeRefreshHost = new FakeRefreshHost();
    const slot: RefreshSlot = new RefreshSlot(host);
    const pending: Promise<any> = slot.send('token');
    host.connected = false;
    await expect(pending).rejects.toMatchObject({ unsent: true, error: { code: errorCodes.clientDisconnected } });
    expect(host.sent).toEqual([]);
    host.connected = true;
    await expect(slot.send('next')).resolves.toMatchObject({ reply: { refresh: { ttl: 60 } } });
    expect(host.sent).toEqual([{ refresh: { token: 'next' } }]);
  });

  test('sendTimer() arms the refresh retry and resolves null, without writing the command', async (): Promise<void> => {
    const host: FakeRefreshHost = new FakeRefreshHost();
    const slot: RefreshSlot = new RefreshSlot(host);
    const pending: Promise<any> = slot.sendTimer('token', slot.currentGeneration());
    host.connected = false;
    await expect(pending).resolves.toBeNull();
    expect(host.armed).toBe(1);
    expect(host.sent).toEqual([]);
    host.connected = true;
    await expect(slot.send('next')).resolves.toMatchObject({ reply: { refresh: { ttl: 60 } } });
    expect(host.sent).toEqual([{ refresh: { token: 'next' } }]);
  });
});

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
