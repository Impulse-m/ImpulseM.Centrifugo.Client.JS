import type { RefreshSlot, RefreshSlotHost } from '../refresh_slot';

// A host of the refresh slot that records the commands sent and the retries armed. The connection state is a flag
// the test sets: a flag set right after a send() call runs in the microtask gap between the claim of the command
// slot and the write of the command, where nothing else of the client runs.
export class FakeRefreshHost implements RefreshSlotHost {
  connected: boolean = true;
  sent: any[] = [];
  armed: number = 0;
  // Whether the client has a getToken function, and whether the token it gives comes from the token exchange.
  getTokenSet: boolean = false;
  exchange: boolean = false;
  // The slot the host serves: its token generation is read from it.
  slot: RefreshSlot | null = null;
  // Settles the getToken call in flight.
  resolveFetch: (token: string) => void = (): void => {};
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
    return this.getTokenSet;
  }

  getToken(): Promise<string> {
    return new Promise<string>((resolve: (token: string) => void): void => {
      this.resolveFetch = resolve;
    });
  }

  tokenSuperseded(generation: number): boolean {
    return this.exchange && this.slot !== null && this.slot.currentTokenGeneration() !== generation;
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
