import { SubscribedContext, PublicationContext } from './types';
import { FakeCentrifugoServer } from './fakeServer';
import { CodecCase, codecCases, until } from './testing/codecs';

// Delta without positioning is a Centrifugo PRO feature (keep_latest_publication), so the
// at-most-once delta path is exercised against the in-process FakeCentrifugoServer: the subscribe
// reply negotiates delta, the first publication is sent full and the next one as a fossil patch
// against it, with no offset on either. Every case runs over both codecs (see testing/codecs.ts).

const channel: string = 'delta-fake';

// The first publication, as the server sends it: the JSON text of the payload.
const sourceText: string = '{"text":"hello world, this is a fairly long payload","n":1}';

// A fossil patch that turns sourceText into the next publication: copy up to the value of n, insert 2,
// copy the closing brace. Its header is the size of the target, its trailer the checksum of the target.
const patchText: string = 'w\nu@0,1:21@v,22yJY5;';

const targetValue: any = { text: 'hello world, this is a fairly long payload', n: 2 };

describe.each(codecCases)('delta without positioning over $name', (codecCase: CodecCase) => {
  let server: FakeCentrifugoServer;
  const clients: any[] = [];

  // The wire carries the JSON text as a string on the JSON codec and as its bytes on the protobuf codec.
  function wire(text: string): any {
    return codecCase.serverCodec === 'json' ? text : new TextEncoder().encode(text);
  }

  beforeEach(async () => {
    server = await FakeCentrifugoServer.start(codecCase.serverCodec);
    server.onSubscribe = (): any => ({ delta: true });
  });

  afterEach(async () => {
    for (const c of clients) {
      c.disconnect();
    }
    clients.length = 0;
    await server.close();
  });

  test('a fossil patch without an offset is decoded against the previous publication', async () => {
    const c: any = codecCase.createClient(server.url);
    clients.push(c);
    c.connect();

    const sub: any = c.newSubscription(channel, { delta: 'fossil' });
    const received: any[] = [];
    sub.on('publication', (ctx: PublicationContext): void => {
      received.push(codecCase.decode(ctx.data));
    });
    const subscribedPromise: Promise<SubscribedContext> = new Promise<SubscribedContext>((resolve: (ctx: SubscribedContext) => void): void => {
      sub.once('subscribed', resolve);
    });
    sub.subscribe();
    await subscribedPromise;

    server.publish(channel, wire(sourceText));
    server.publish(channel, wire(patchText), { delta: true });
    await until((): boolean => received.length === 2);

    expect(received[0]).toEqual({ text: 'hello world, this is a fairly long payload', n: 1 });
    expect(received[1]).toEqual(targetValue);

    const stats: any = sub.deltaStats();
    expect(stats.numFullPayloads).toBe(1);
    expect(stats.numDeltaPayloads).toBe(1);
    expect(stats.bytesReceived).toBeLessThan(stats.bytesDecoded);
  });
});
