import { Centrifuge } from './centrifuge';
import { FakeCentrifugoServer, holdReply } from './fakeServer';
import { errorCodes } from './codes';
import { AuthContext, Options, State } from './types';
import { CodecCase, codecCases } from './test_codecs';
import { maxRefreshCalls } from './refresh_slot';

// The identity of a connection: the user and auth class of the connect and refresh replies,
// the 'auth' event and the refresh() call. Every case runs over both codecs (see test_codecs.ts).

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

  function connectCommands(): any[] {
    return server.received.filter(cmd => cmd.connect !== undefined);
  }

  function refreshCommands(): any[] {
    return server.received.filter(cmd => cmd.refresh !== undefined);
  }

  test('refresh sends the given token now and resolves the identity the server confirms', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'bot', expires: true, ttl: 300 },
    } : undefined);
    const c = create();
    await connect(c);

    const result = await c.refresh('new-token');

    expect(result).toEqual({ user: 'bob', authClass: 'bot', ttl: 300 });
    expect(refreshCommands()).toHaveLength(1);
    expect(refreshCommands()[0].refresh.token).toBe('new-token');
    expect(authEvents.map(ctx => ctx.user)).toEqual(['alice', 'bob']);
  });

  test('the token of a refresh is used by the reconnect that follows', async () => {
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
    const c = create();
    await connect(c);
    await c.refresh('new-token');

    const reconnected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    server.closeConnection();
    await reconnected;

    expect(connectCommands()).toHaveLength(2);
    expect(connectCommands()[1].connect.token).toBe('new-token');
  });

  test('refresh uses the token from getToken when no token is given', async () => {
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
    const c = create({ getToken: async () => 'token-from-get-token' });
    await connect(c);

    await c.refresh();

    expect(refreshCommands()[0].refresh.token).toBe('token-from-get-token');
  });

  test('refresh without a token and without getToken rejects with a configuration error and sends nothing', async () => {
    const c = create();
    await connect(c);

    await expect(c.refresh()).rejects.toMatchObject({ code: errorCodes.badConfiguration });
    expect(refreshCommands()).toHaveLength(0);
    expect(c.state).toBe(State.Connected);
  });

  test('refresh while disconnected is sent on the next connect', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'authenticated' },
    } : undefined);
    const c = create();
    c.disconnect();

    const refreshing = c.refresh('new-token');
    await connect(c);
    const result = await refreshing;

    expect(result).toEqual({ user: 'bob', authClass: 'authenticated', ttl: 0 });
    expect(connectCommands()[0].connect.token).toBe('new-token');
    expect(refreshCommands()).toHaveLength(1);
  });

  test('a refresh waiting for a reconnect longer than the timeout is still sent on that connect', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'authenticated' },
    } : undefined);
    const c = create({ timeout: 100, minReconnectDelay: 400, maxReconnectDelay: 400 });
    await connect(c);

    const reconnecting = new Promise<void>(resolve => c.once('connecting', () => resolve()));
    server.closeConnection();
    await reconnecting;
    const result = await c.refresh('new-token');

    expect(result).toEqual({ user: 'bob', authClass: 'authenticated', ttl: 0 });
    expect(connectCommands()).toHaveLength(2);
    expect(connectCommands()[1].connect.token).toBe('new-token');
    expect(refreshCommands()).toHaveLength(1);
  });

  test('calls made while a refresh of the same token is pending share its promise', async () => {
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? holdReply : undefined);
    const c = create();
    await connect(c);

    const first = c.refresh('new-token');

    expect(c.refresh('new-token')).toBe(first);
    expect(c.refresh()).toBe(first);
    const sent = await server.waitFor(cmd => cmd.refresh !== undefined);
    server.send({ id: sent.id, refresh: {} });
    await first;
    expect(refreshCommands()).toHaveLength(1);
  });

  test('a refresh with another token waits for the one in flight and gets the identity of its own token', async () => {
    server.onCommand = (cmd: any) => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      return cmd.refresh.token === 'anon-token'
        ? holdReply
        : { id: cmd.id, refresh: { user: 'alice', auth_class: 'authenticated' } };
    };
    const c = create();
    await connect(c);

    const anon = c.refresh('anon-token');
    const user = c.refresh('user-token');

    expect(user).not.toBe(anon);
    const anonCommand = await server.waitFor(cmd => cmd.refresh !== undefined && cmd.refresh.token === 'anon-token');
    expect(refreshCommands()).toHaveLength(1);

    server.send({ id: anonCommand.id, refresh: { user: 'anon', auth_class: 'anonymous' } });
    await expect(anon).resolves.toEqual({ user: 'anon', authClass: 'anonymous', ttl: 0 });
    await expect(user).resolves.toEqual({ user: 'alice', authClass: 'authenticated', ttl: 0 });
    expect(refreshCommands().map(cmd => cmd.refresh.token)).toEqual(['anon-token', 'user-token']);
  });

  test('rpc waits for a refresh in flight, so it goes out under the new identity', async () => {
    let refreshReceived: (id: number) => void = () => undefined;
    const refreshSeen = new Promise<number>(resolve => {
      refreshReceived = resolve;
    });
    const order: string[] = [];
    server.onCommand = (cmd: any) => {
      if (cmd.refresh !== undefined) {
        order.push('refresh');
        refreshReceived(cmd.id);
        return holdReply;
      }
      if (cmd.rpc !== undefined) {
        order.push('rpc');
        return { id: cmd.id, rpc: {} };
      }
      return undefined;
    };
    const c = create();
    await connect(c);

    const refreshing = c.refresh('new-token');
    const calling = c.rpc('echo', codecCase.encode({}));
    const refreshId = await refreshSeen;
    // The rpc goes out only after the refresh reply: no rpc command has reached the server yet.
    expect(server.received.some(cmd => cmd.rpc !== undefined)).toBe(false);
    expect(order).toEqual(['refresh']);

    server.send({ id: refreshId, refresh: { user: 'bob', auth_class: 'authenticated' } });
    await refreshing;
    await calling;

    expect(order).toEqual(['refresh', 'rpc']);
  });

  test('a refused refresh rejects and keeps the identity of the connection', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
      id: cmd.id,
      error: { code: 103, message: 'transition not allowed' },
    } : undefined);
    const c = create();
    await connect(c);

    await expect(c.refresh('new-token')).rejects.toMatchObject({ code: 103, message: 'transition not allowed' });
    expect(c.state).toBe(State.Connected);
    expect(authEvents.map(ctx => ctx.user)).toEqual(['alice']);
  });

  test('a refused refresh keeps the previous token for the reconnect that follows', async () => {
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
      id: cmd.id,
      error: { code: 103, message: 'transition not allowed' },
    } : undefined);
    const c = create({ token: 'old-token' });
    await connect(c);

    await expect(c.refresh('refused-token')).rejects.toMatchObject({ code: 103 });
    const reconnected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    server.closeConnection();
    await reconnected;

    expect(connectCommands()).toHaveLength(2);
    expect(connectCommands()[1].connect.token).toBe('old-token');
  });

  test('a refresh reply of a legacy server gives empty user and auth class', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
    const c = create();
    await connect(c);

    const result = await c.refresh('new-token');

    expect(result).toEqual({ user: '', authClass: '', ttl: 0 });
    expect(authEvents[1]).toEqual({ user: '', authClass: '', ttl: 0 });
  });

  test('auth event on refresh is emitted only when the identity changed', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    let replyUser = 'alice';
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: replyUser, auth_class: 'authenticated' },
    } : undefined);
    const c = create();
    await connect(c);

    await c.refresh('same-identity');
    expect(authEvents).toHaveLength(1);

    replyUser = 'bob';
    await c.refresh('new-identity');
    expect(authEvents.map(ctx => ctx.user)).toEqual(['alice', 'bob']);
  });

  test('refresh reschedules the refresh timer from the ttl of the reply', async () => {
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { expires: true, ttl: 1 },
    } : undefined);
    const c = create({ getToken: async () => 'timer-token' });
    await connect(c);

    const timerRefresh = server.waitFor((cmd: any) => cmd.refresh !== undefined && refreshCommands().length >= 2);
    await c.refresh('new-token');
    await timerRefresh;

    expect(refreshCommands()[1].refresh.token).toBe('timer-token');
  });

  test('a timer refresh and an explicit refresh never have two refresh commands in flight', async () => {
    const inFlight: number[] = [];
    let overlapped: boolean = false;
    let timerRequested: () => void = () => undefined;
    const timerRequest = new Promise<void>(resolve => {
      timerRequested = resolve;
    });
    server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
    server.onCommand = (cmd: any) => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      if (inFlight.length > 0) {
        overlapped = true;
      }
      inFlight.push(cmd.id);
      return holdReply;
    };
    const c = create({
      token: 'initial-token',
      getToken: async () => {
        timerRequested();
        return 'timer-token';
      },
    });
    await connect(c);

    const explicit = c.refresh('explicit-token');
    // The timer fires on the ttl of the connect reply, while the explicit refresh is still in flight.
    await timerRequest;
    const explicitCommand = await server.waitFor(cmd => cmd.refresh !== undefined && cmd.refresh.token === 'explicit-token');
    // The timer refresh waits for the explicit one: it is sent only after that reply.
    server.send({ id: explicitCommand.id, refresh: { expires: true, ttl: 60 } });
    inFlight.shift();
    await explicit;

    const timerCommand = await server.waitFor(cmd => cmd.refresh !== undefined && cmd.refresh.token === 'timer-token');
    server.send({ id: timerCommand.id, refresh: {} });
    inFlight.shift();

    expect(overlapped).toBe(false);
    expect(refreshCommands().map(cmd => cmd.refresh.token)).toEqual(['explicit-token', 'timer-token']);
  });

  test('the token of the next connect is the last accepted refresh, a timer refresh queued behind an explicit one included', async () => {
    let timerTokenRequested: () => void = () => undefined;
    const timerRequest = new Promise<void>(resolve => {
      timerTokenRequested = resolve;
    });
    server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
    server.onCommand = (cmd: any) => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      return cmd.refresh.token === 'anon-token' ? holdReply : { id: cmd.id, refresh: {} };
    };
    const c: Centrifuge = create({
      token: 'initial-token',
      getToken: async () => {
        timerTokenRequested();
        return 'timer-token';
      },
    });
    await connect(c);

    const anon: Promise<AuthContext> = c.refresh('anon-token');
    // The timer fires while the explicit refresh is in flight, and waits for it.
    await timerRequest;
    const anonCommand: any = await server.waitFor(cmd => cmd.refresh !== undefined && cmd.refresh.token === 'anon-token');
    server.send({ id: anonCommand.id, refresh: {} });
    await anon;
    await server.waitFor(cmd => cmd.refresh !== undefined && cmd.refresh.token === 'timer-token');

    const reconnected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    server.closeConnection();
    await reconnected;

    expect(connectCommands()[1].connect.token).toBe('timer-token');
  });

  test('an rpc waiting for a refresh ends at its deadline, also when the refresh waits for a reconnect', async () => {
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? holdReply : undefined);
    const c: Centrifuge = create({ timeout: 300, minReconnectDelay: 5000, maxReconnectDelay: 5000 });
    await connect(c);
    const first: Promise<AuthContext> = c.refresh('token-a');
    const second: Promise<AuthContext> = c.refresh('token-b');
    first.catch(() => undefined);
    second.catch(() => undefined);
    await server.waitFor(cmd => cmd.refresh !== undefined && cmd.refresh.token === 'token-a');

    const started: number = Date.now();
    const calling: Promise<unknown> = c.rpc('echo', codecCase.encode({}));
    // The connection drops: the refresh in flight is lost, and the one behind it waits for a late reconnect.
    server.closeConnection();

    await expect(calling).rejects.toMatchObject({ code: errorCodes.timeout });
    expect(Date.now() - started).toBeLessThan(2000);
  });

  test('refresh calls waiting for a connection are bounded by maxRefreshCalls', async () => {
    const c: Centrifuge = create();
    server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
    const pending: Promise<AuthContext>[] = [];
    for (let i: number = 0; i < maxRefreshCalls; i++) {
      pending.push(c.refresh(`token-${i}`));
    }
    await expect(c.refresh('one-too-many')).rejects.toMatchObject({ code: errorCodes.limitExceeded });

    await connect(c);
    await Promise.all(pending);

    const tokens: string[] = Array.from({ length: maxRefreshCalls }, (_: unknown, i: number): string => `token-${i}`);
    expect(refreshCommands().map((cmd: any): string => cmd.refresh.token)).toEqual(tokens);
  });

  for (const code of [103, 109]) {
    test(`a timer refresh refused with ${code} keeps the connection and reports an error`, async () => {
      server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
      server.onCommand = (cmd: any) => (cmd.refresh !== undefined ? {
        id: cmd.id,
        error: { code: code, message: 'refused' },
      } : undefined);
      const c: Centrifuge = create({ getToken: async () => 'timer-token' });
      const failed: Promise<any> = new Promise<any>(resolve => c.once('error', (ctx: any) => resolve(ctx)));
      await connect(c);

      const reported: any = await failed;

      expect(reported).toMatchObject({ type: 'refresh', error: { code: code } });
      expect(c.state).toBe(State.Connected);
      expect(refreshCommands()).toHaveLength(1);
    });
  }
});
