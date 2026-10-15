import { Centrifuge } from './centrifuge';
import { FakeCentrifugoServer } from './fakeServer';
import { State, TransportName } from './types';

import WebSocket from 'ws';

// The connect timeout of a transport that is created and never opens, run on the fake clock: the test does not depend
// on the wall clock. The waits are on the transports themselves (the moment each one is created), not on counters.
// The other transport failures are in transport_init_error.test.ts.

// A WebSocket replacement whose constructor succeeds, but which never opens or reports a close.
function hangingWebSocket(): any {
  return function (this: any): void {
    this.onopen = null;
    this.onerror = null;
    this.onclose = null;
    this.onmessage = null;
    this.send = (): void => {};
    this.close = (): void => {};
  } as any;
}

describe('connect timeout of a transport that never reports', () => {
  let server: FakeCentrifugoServer;
  const clients: Centrifuge[] = [];

  beforeEach(async (): Promise<void> => {
    server = await FakeCentrifugoServer.start();
  });

  afterEach(async (): Promise<void> => {
    clients.forEach((c: Centrifuge): void => c.disconnect());
    clients.length = 0;
    await server.close();
  });

  test('a hanging transport falls through to the next one, on the fake clock', async (): Promise<void> => {
    const hanging: string = 'ws://hanging.invalid/connection/websocket';
    let onHangingCreated: () => void = (): void => {};
    let onNextCreated: () => void = (): void => {};
    const hangingCreated: Promise<void> = new Promise<void>((resolve: () => void): void => {
      onHangingCreated = resolve;
    });
    const nextCreated: Promise<void> = new Promise<void>((resolve: () => void): void => {
      onNextCreated = resolve;
    });
    const hangingSocket: any = hangingWebSocket();
    const websocket: any = function (this: any, url: string): any {
      if (url === hanging) {
        onHangingCreated();
        return new hangingSocket();
      }
      onNextCreated();
      return new WebSocket(url);
    };
    const c: Centrifuge = new Centrifuge([
      { transport: 'websocket' as TransportName, endpoint: hanging },
      { transport: 'websocket' as TransportName, endpoint: server.url },
    ], {
      websocket: websocket,
      timeout: 200,
      minReconnectDelay: 20,
      maxReconnectDelay: 20,
      networkEventTarget: new EventTarget(),
    });
    clients.push(c);
    const errors: string[] = [];
    c.on('error', (ctx: any): void => {
      errors.push(`${ctx.type}:${ctx.error.message}`);
    });

    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      c.connect();
      await hangingCreated;
      // The connect timeout of the hanging transport, then the reconnect delay that starts the next transport.
      jest.advanceTimersByTime(200);
      jest.advanceTimersByTime(20);
      await nextCreated;
      await c.ready(3000);
    } finally {
      jest.useRealTimers();
    }

    expect(c.state).toBe(State.Connected);
    expect(errors).toEqual(['transport:connect timeout']);
  });
});
