import { Centrifuge } from './centrifuge';
import { SubscribedContext, TransportName } from './types';
import { FakeCentrifugoServer } from './fakeServer';

import WebSocket from 'ws';

// Publish debouncing is a Centrifugo PRO feature (client_publish_debounce_interval), so these
// tests use the in-process FakeCentrifugoServer: the subscribe reply carries publish_debounce and
// the server records every publish the client sends. The window itself is checked end to end
// against a PRO server in debounce.test.ts; the failure and reconnect paths are in
// debounce_publish_error.test.ts and debounce_reconnect.test.ts.

const debounceMs = 30;

// Long enough for the window to close and its trailing timer to find nothing left to send.
const windowsDelay = debounceMs * 3;

function createClient(url: string): Centrifuge {
  return new Centrifuge([{
    transport: 'websocket' as TransportName,
    endpoint: url,
  }], {
    websocket: WebSocket,
    minReconnectDelay: 10,
    maxReconnectDelay: 50,
  });
}

function waitForEvent<T>(emitter: any, event: string, timeout = 5000): Promise<T> {
  return new Promise<T>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting for '${event}'`)), timeout);
    emitter.on(event, (ctx: T) => {
      clearTimeout(timer);
      resolve(ctx);
    });
  });
}

const delay = (ms: number) => new Promise<void>(r => setTimeout(r, ms));

// Publish commands as the server received them: stream publishes carry the data, map publishes
// add the key and type 1, and a map remove is a publish with removed set.
function publishes(server: FakeCentrifugoServer): any[] {
  return server.received.filter(cmd => cmd.publish !== undefined).map(cmd => cmd.publish);
}

function mapWrites(server: FakeCentrifugoServer, key: string): any[] {
  return publishes(server).filter(pub => pub.key === key);
}

function mapData(server: FakeCentrifugoServer, key: string): any[] {
  return mapWrites(server, key).filter(pub => pub.removed !== true).map(pub => pub.data);
}

function mapRemoves(server: FakeCentrifugoServer, key: string): any[] {
  return mapWrites(server, key).filter(pub => pub.removed === true);
}

describe('publish debounce on the fake server', () => {
  let server: FakeCentrifugoServer;
  let c: Centrifuge;

  beforeEach(async () => {
    server = await FakeCentrifugoServer.start();
    server.onSubscribe = () => ({ publish_debounce: debounceMs } as any);
  });

  afterEach(async () => {
    c?.disconnect();
    await server.close();
  });

  test('stream publishes inside the window are coalesced to the latest value', async () => {
    c = createClient(server.url);
    c.connect();

    const sub = c.newSubscription('debounced');
    const subscribedPromise = waitForEvent<SubscribedContext>(sub, 'subscribed');
    sub.subscribe();
    await subscribedPromise;

    // The first publish goes out at once and opens the window.
    await sub.publish(1);
    // The next two land in the open window: they resolve at once and only replace the pending value.
    const second = sub.publish(2);
    const third = sub.publish(3);
    await Promise.all([second, third]);
    expect(publishes(server).map(pub => pub.data)).toEqual([1]);

    // When the window closes, the latest value goes out once; the value 2 never reaches the server.
    await delay(windowsDelay);
    expect(publishes(server).map(pub => pub.data)).toEqual([1, 3]);
  });

  test('a stream publish after an idle window is sent at once', async () => {
    c = createClient(server.url);
    c.connect();

    const sub = c.newSubscription('debounced');
    const subscribedPromise = waitForEvent<SubscribedContext>(sub, 'subscribed');
    sub.subscribe();
    await subscribedPromise;

    await sub.publish(1);
    await delay(windowsDelay);

    // No publish happened in the last window, so the next one is a window opener and goes out at once.
    await sub.publish(2);
    expect(publishes(server).map(pub => pub.data)).toEqual([1, 2]);
  });

  test('map keys debounce in independent windows', async () => {
    c = createClient(server.url);
    c.connect();

    const sub = c.newMapSubscription('debounced-map');
    const subscribedPromise = waitForEvent<SubscribedContext>(sub, 'subscribed');
    sub.subscribe();
    await subscribedPromise;

    await sub.publish('a', 1);
    // Key 'b' has its own window, so its first publish goes out while the window of 'a' is still open.
    await sub.publish('b', 1);
    expect(mapData(server, 'a')).toEqual([1]);
    expect(mapData(server, 'b')).toEqual([1]);

    // Each key coalesces its own burst to the latest value.
    await Promise.all([
      sub.publish('a', 2),
      sub.publish('b', 2),
      sub.publish('a', 3),
    ]);
    expect(mapData(server, 'a')).toEqual([1]);
    expect(mapData(server, 'b')).toEqual([1]);

    await delay(windowsDelay);
    expect(mapData(server, 'a')).toEqual([1, 3]);
    expect(mapData(server, 'b')).toEqual([1, 2]);
  });

  test('remove of a key drops its pending publish and sends the remove', async () => {
    c = createClient(server.url);
    c.connect();

    const sub = c.newMapSubscription('debounced-map');
    const subscribedPromise = waitForEvent<SubscribedContext>(sub, 'subscribed');
    sub.subscribe();
    await subscribedPromise;

    await sub.publish('a', 1);
    // Held in the open window of 'a'.
    await sub.publish('a', 2);

    await sub.remove('a');
    expect(mapRemoves(server, 'a')).toHaveLength(1);

    // The pending value 2 must not follow the remove when the window closes.
    await delay(windowsDelay);
    expect(mapData(server, 'a')).toEqual([1]);
    expect(mapRemoves(server, 'a')).toHaveLength(1);
  });

  test('unsubscribe with a pending value sends nothing more', async () => {
    c = createClient(server.url);
    c.connect();

    const sub = c.newSubscription('debounced');
    const subscribedPromise = waitForEvent<SubscribedContext>(sub, 'subscribed');
    sub.subscribe();
    await subscribedPromise;

    await sub.publish(1);
    // Held in the open window; resolves at once.
    await sub.publish(2);

    sub.unsubscribe();
    await delay(windowsDelay);

    expect(server.received.some(cmd => cmd.unsubscribe !== undefined)).toBe(true);
    expect(publishes(server).map(pub => pub.data)).toEqual([1]);
  });
});
