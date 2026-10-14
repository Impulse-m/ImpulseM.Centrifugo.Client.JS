import { Centrifuge } from './centrifuge';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { FakeCentrifugoServer, FakeCodec } from './fakeServer';
import { AuthContext, Options, State, TransportName } from './types';
import WebSocket from 'ws';

// The identity of a connection: the user and auth class of the connect and refresh replies,
// the 'auth' event and the refresh() call. Every case runs over both codecs: the fake server
// speaks JSON or protobuf, and the client is the matching Centrifuge class.

// RPC data is raw bytes on the protobuf wire, JSON on the JSON wire.
interface CodecCase {
  name: string;
  serverCodec: FakeCodec;
  createClient: (url: string, options: Partial<Options>) => Centrifuge;
}

const codecCases: CodecCase[] = [
  {
    name: 'JSON',
    serverCodec: 'json',
    createClient: (url: string, options: Partial<Options>): Centrifuge => new Centrifuge([{
      transport: 'websocket' as TransportName,
      endpoint: url,
    }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      ...options,
    }),
  },
  {
    name: 'protobuf',
    serverCodec: 'protobuf',
    createClient: (url: string, options: Partial<Options>): Centrifuge => new ProtobufCentrifuge([{
      transport: 'websocket' as TransportName,
      endpoint: url,
    }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      ...options,
    }),
  },
];

describe.each(codecCases)('auth identity over $name', (codecCase: CodecCase) => {
  let server: FakeCentrifugoServer;
  const clients: Centrifuge[] = [];
  const authEvents: AuthContext[] = [];

  beforeEach(async () => {
    server = await FakeCentrifugoServer.start(codecCase.serverCodec);
    authEvents.length = 0;
  });

  afterEach(async () => {
    for (const c of clients) {
      c.disconnect();
    }
    clients.length = 0;
    await server.close();
  });

  function create(options: Partial<Options> = {}): Centrifuge {
    const c = codecCase.createClient(server.url, options);
    clients.push(c);
    c.on('auth', (ctx: AuthContext) => authEvents.push(ctx));
    return c;
  }

  async function connect(c: Centrifuge): Promise<void> {
    const connected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    c.connect();
    await connected;
    expect(c.state).toBe(State.Connected);
  }

  test('connected context carries the user and auth class of the connect reply', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    const c = create();
    const contexts: any[] = [];
    c.on('connected', (ctx: any) => contexts.push(ctx));

    await connect(c);

    expect(contexts[0].user).toBe('alice');
    expect(contexts[0].authClass).toBe('authenticated');
  });

  test('connected context has empty user and auth class for a legacy connect reply', async () => {
    const c = create();
    const contexts: any[] = [];
    c.on('connected', (ctx: any) => contexts.push(ctx));

    await connect(c);

    expect(contexts[0].user).toBe('');
    expect(contexts[0].authClass).toBe('');
  });

  test('auth event is emitted on connect with the identity of the reply', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'bot', ttl: 120 };
    const c = create();

    await connect(c);

    expect(authEvents).toEqual([{ user: 'alice', authClass: 'bot', ttl: 120 }]);
  });

  test('auth event on a legacy connect reply carries empty user and auth class', async () => {
    const c = create();

    await connect(c);

    expect(authEvents).toEqual([{ user: '', authClass: '', ttl: 0 }]);
  });

  test('auth event is emitted again on every connect', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    const c = create();
    await connect(c);

    const reconnected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    server.closeConnection();
    await reconnected;

    expect(authEvents).toHaveLength(2);
    expect(authEvents[1]).toEqual({ user: 'alice', authClass: 'authenticated', ttl: 0 });
  });

  test('a reconnect with a changed identity carries the new identity in the connected context', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    const c = create();
    const contexts: any[] = [];
    c.on('connected', (ctx: any) => contexts.push(ctx));
    await connect(c);

    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'bob', auth_class: 'bot' };
    const reconnected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    server.closeConnection();
    await reconnected;

    expect(contexts[1].user).toBe('bob');
    expect(contexts[1].authClass).toBe('bot');
    expect(authEvents.map(ctx => ctx.user)).toEqual(['alice', 'bob']);
  });
});
