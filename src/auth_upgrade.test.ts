import { Centrifuge } from './centrifuge';
import { errorCodes } from './codes';
import { maxRefreshCalls } from './refresh_slot';
import { AuthContext, Options, State } from './types';
import { CodecCase, WaitableFakeServer, codecCases, connectClient, disconnectAll, holdReply, until } from './test_codecs';

// The identity of a connection: the user and auth class of the connect and refresh replies,
// the 'auth' event and the refresh() call. Every case runs over both codecs (see test_codecs.ts).

describe.each(codecCases)('auth identity over $name', (codecCase: CodecCase): void => {
  let server: WaitableFakeServer;
  const clients: Centrifuge[] = [];
  const authEvents: AuthContext[] = [];

  beforeEach(async (): Promise<void> => {
    server = await WaitableFakeServer.start(codecCase.serverCodec);
    authEvents.length = 0;
  });

  afterEach(async (): Promise<void> => {
    disconnectAll(clients);
    await server.close();
  });

  function create(options: Partial<Options> = {}): Centrifuge {
    const c: Centrifuge = codecCase.createClient(server.url, options);
    clients.push(c);
    c.on('auth', (ctx: AuthContext): void => {
      authEvents.push(ctx);
    });
    return c;
  }

  async function connect(c: Centrifuge): Promise<void> {
    await connectClient(c);
    expect(c.state).toBe(State.Connected);
  }

  function connectCommands(): any[] {
    return server.received.filter((cmd: any): boolean => cmd.connect !== undefined);
  }

  function refreshCommands(): any[] {
    return server.received.filter((cmd: any): boolean => cmd.refresh !== undefined);
  }

  // Lets the pending callbacks and replies run: a few turns of the event loop, no wait for a time.
  async function flushTimers(): Promise<void> {
    for (let i: number = 0; i < 10; i++) {
      await new Promise<void>((resolve: () => void): void => {
        setImmediate(resolve);
      });
    }
  }

  test('connected context carries the user and auth class of the connect reply', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    const c: Centrifuge = create();
    const contexts: any[] = [];
    c.on('connected', (ctx: any): void => {
      contexts.push(ctx);
    });

    await connect(c);

    expect(contexts[0].user).toBe('alice');
    expect(contexts[0].authClass).toBe('authenticated');
  });

  test('connected context has empty user and auth class for a legacy connect reply', async (): Promise<void> => {
    const c: Centrifuge = create();
    const contexts: any[] = [];
    c.on('connected', (ctx: any): void => {
      contexts.push(ctx);
    });

    await connect(c);

    expect(contexts[0].user).toBe('');
    expect(contexts[0].authClass).toBe('');
  });

  test('auth event is emitted on connect with the identity of the reply', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'bot', ttl: 120 };
    const c: Centrifuge = create();

    await connect(c);

    expect(authEvents).toEqual([{ user: 'alice', authClass: 'bot', ttl: 120 }]);
  });

  test('auth event on a legacy connect reply carries empty user and auth class', async (): Promise<void> => {
    const c: Centrifuge = create();

    await connect(c);

    expect(authEvents).toEqual([{ user: '', authClass: '', ttl: 0 }]);
  });

  test('auth event is emitted again on every connect', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    const c: Centrifuge = create();
    await connect(c);

    const reconnected: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
    server.closeConnection();
    await reconnected;

    expect(authEvents).toHaveLength(2);
    expect(authEvents[1]).toEqual({ user: 'alice', authClass: 'authenticated', ttl: 0 });
  });

  test('a reconnect with a changed identity carries the new identity in the connected context', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    const c: Centrifuge = create();
    const contexts: any[] = [];
    c.on('connected', (ctx: any): void => {
      contexts.push(ctx);
    });
    await connect(c);

    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'bob', auth_class: 'bot' };
    const reconnected: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
    server.closeConnection();
    await reconnected;

    expect(contexts[1].user).toBe('bob');
    expect(contexts[1].authClass).toBe('bot');
    expect(authEvents.map((ctx: AuthContext): string => ctx.user)).toEqual(['alice', 'bob']);
  });

  test('refresh sends the given token now and resolves the identity the server confirms', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'bot', expires: true, ttl: 300 },
    } : undefined);
    const c: Centrifuge = create();
    await connect(c);

    const result: AuthContext = await c.refresh('new-token');

    expect(result).toEqual({ user: 'bob', authClass: 'bot', ttl: 300 });
    expect(refreshCommands()).toHaveLength(1);
    expect(refreshCommands()[0].refresh.token).toBe('new-token');
    expect(authEvents.map((ctx: AuthContext): string => ctx.user)).toEqual(['alice', 'bob']);
  });

  test('the token of a refresh is used by the reconnect that follows', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
    const c: Centrifuge = create();
    await connect(c);
    await c.refresh('new-token');

    const reconnected: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
    server.closeConnection();
    await reconnected;

    expect(connectCommands()).toHaveLength(2);
    expect(connectCommands()[1].connect.token).toBe('new-token');
  });

  test('refresh uses the token from getToken when no token is given', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
    const c: Centrifuge = create({ getToken: async (): Promise<string> => 'token-from-get-token' });
    await connect(c);

    await c.refresh();

    expect(refreshCommands()[0].refresh.token).toBe('token-from-get-token');
  });

  test('refresh without a token and without getToken rejects with a configuration error and sends nothing', async (): Promise<void> => {
    const c: Centrifuge = create();
    await connect(c);

    await expect(c.refresh()).rejects.toMatchObject({ code: errorCodes.badConfiguration });
    expect(refreshCommands()).toHaveLength(0);
    expect(c.state).toBe(State.Connected);
  });

  test('refresh with getToken set to undefined rejects with a configuration error and sends nothing', async (): Promise<void> => {
    // A token is set, so the connect does not need getToken; the refresh finds none and reports the configuration.
    const c: Centrifuge = create({ token: 'token-a', getToken: undefined });
    await connect(c);

    await expect(c.refresh()).rejects.toMatchObject({ code: errorCodes.badConfiguration });
    expect(refreshCommands()).toHaveLength(0);
    expect(c.state).toBe(State.Connected);
  });

  test('a refresh with a new token that is never sent, the client being disconnected for good, leaves the token accepted before', async (): Promise<void> => {
    const c: Centrifuge = create({ token: 'token-a' });
    await connect(c);
    const connecting: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connecting', (): void => resolve());
    });
    server.closeConnection();
    await connecting;

    // Waits for the connection with the new token set; the disconnect for good ends the wait.
    const refreshing: Promise<AuthContext> = c.refresh('token-b');
    refreshing.catch((): void => undefined);
    c.disconnect();
    await expect(refreshing).rejects.toMatchObject({ code: errorCodes.clientDisconnected });
    expect(refreshCommands()).toHaveLength(0);

    await connect(c);
    expect(connectCommands()[connectCommands().length - 1].connect.token).toBe('token-a');
  });

  test('refresh while disconnected is sent on the next connect', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'authenticated' },
    } : undefined);
    const c: Centrifuge = create();
    c.disconnect();

    const refreshing: Promise<AuthContext> = c.refresh('new-token');
    await connect(c);
    const result: AuthContext = await refreshing;

    expect(result).toEqual({ user: 'bob', authClass: 'authenticated', ttl: 0 });
    expect(connectCommands()[0].connect.token).toBe('new-token');
    expect(refreshCommands()).toHaveLength(1);
  });

  test('a refresh waiting for a reconnect longer than the timeout is still sent on that connect', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'authenticated' },
    } : undefined);
    const c: Centrifuge = create({ timeout: 100, minReconnectDelay: 400, maxReconnectDelay: 400 });
    await connect(c);

    const reconnecting: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connecting', (): void => resolve());
    });
    server.closeConnection();
    await reconnecting;
    const result: AuthContext = await c.refresh('new-token');

    expect(result).toEqual({ user: 'bob', authClass: 'authenticated', ttl: 0 });
    expect(connectCommands()).toHaveLength(2);
    expect(connectCommands()[1].connect.token).toBe('new-token');
    expect(refreshCommands()).toHaveLength(1);
  });

  test('calls made while a refresh of the same token is pending share its promise', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? holdReply : undefined);
    const c: Centrifuge = create();
    await connect(c);

    const first: Promise<AuthContext> = c.refresh('new-token');

    expect(c.refresh('new-token')).toBe(first);
    expect(c.refresh()).toBe(first);
    const sent: any = await server.waitFor((cmd: any): boolean => cmd.refresh !== undefined);
    server.send({ id: sent.id, refresh: {} });
    await first;
    expect(refreshCommands()).toHaveLength(1);
  });

  test('a refresh with another token waits for the one in flight and gets the identity of its own token', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      return cmd.refresh.token === 'anon-token'
        ? holdReply
        : { id: cmd.id, refresh: { user: 'alice', auth_class: 'authenticated' } };
    };
    const c: Centrifuge = create();
    await connect(c);

    const anon: Promise<AuthContext> = c.refresh('anon-token');
    const user: Promise<AuthContext> = c.refresh('user-token');

    expect(user).not.toBe(anon);
    const anonCommand: any = await server.waitFor((cmd: any): boolean => cmd.refresh !== undefined && cmd.refresh.token === 'anon-token');
    expect(refreshCommands()).toHaveLength(1);

    server.send({ id: anonCommand.id, refresh: { user: 'anon', auth_class: 'anonymous' } });
    await expect(anon).resolves.toEqual({ user: 'anon', authClass: 'anonymous', ttl: 0 });
    await expect(user).resolves.toEqual({ user: 'alice', authClass: 'authenticated', ttl: 0 });
    expect(refreshCommands().map((cmd: any): string => cmd.refresh.token)).toEqual(['anon-token', 'user-token']);
  });

  test('rpc waits for a refresh in flight, so it goes out under the new identity', async (): Promise<void> => {
    let refreshReceived: (id: number) => void = (): void => undefined;
    const refreshSeen: Promise<number> = new Promise<number>((resolve: (id: number) => void): void => {
      refreshReceived = resolve;
    });
    const order: string[] = [];
    server.onCommand = (cmd: any): any => {
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
    const c: Centrifuge = create();
    await connect(c);

    const refreshing: Promise<AuthContext> = c.refresh('new-token');
    const calling: Promise<unknown> = c.rpc('echo', codecCase.encode({}));
    const refreshId: number = await refreshSeen;
    // The rpc goes out only after the refresh reply: no rpc command has reached the server yet.
    expect(server.received.some((cmd: any): boolean => cmd.rpc !== undefined)).toBe(false);
    expect(order).toEqual(['refresh']);

    server.send({ id: refreshId, refresh: { user: 'bob', auth_class: 'authenticated' } });
    await refreshing;
    await calling;

    expect(order).toEqual(['refresh', 'rpc']);
  });

  test('a refused refresh rejects and keeps the identity of the connection', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      error: { code: 103, message: 'transition not allowed' },
    } : undefined);
    const c: Centrifuge = create();
    await connect(c);

    await expect(c.refresh('new-token')).rejects.toMatchObject({ code: 103, message: 'transition not allowed' });
    expect(c.state).toBe(State.Connected);
    expect(authEvents.map((ctx: AuthContext): string => ctx.user)).toEqual(['alice']);
  });

  test('a refused refresh keeps the previous token for the reconnect that follows', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      error: { code: 103, message: 'transition not allowed' },
    } : undefined);
    const c: Centrifuge = create({ token: 'old-token' });
    await connect(c);

    await expect(c.refresh('refused-token')).rejects.toMatchObject({ code: 103 });
    const reconnected: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
    server.closeConnection();
    await reconnected;

    expect(connectCommands()).toHaveLength(2);
    expect(connectCommands()[1].connect.token).toBe('old-token');
  });

  test('a refresh reply of a legacy server gives empty user and auth class', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
    const c: Centrifuge = create();
    await connect(c);

    const result: AuthContext = await c.refresh('new-token');

    expect(result).toEqual({ user: '', authClass: '', ttl: 0 });
    expect(authEvents[1]).toEqual({ user: '', authClass: '', ttl: 0 });
  });

  test('auth event on refresh is emitted only when the identity changed', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    let replyUser: string = 'alice';
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: replyUser, auth_class: 'authenticated' },
    } : undefined);
    const c: Centrifuge = create();
    await connect(c);

    await c.refresh('same-identity');
    expect(authEvents).toHaveLength(1);

    replyUser = 'bob';
    await c.refresh('new-identity');
    expect(authEvents.map((ctx: AuthContext): string => ctx.user)).toEqual(['alice', 'bob']);
  });

  test('refresh reschedules the refresh timer from the ttl of the reply', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { expires: true, ttl: 1 },
    } : undefined);
    const c: Centrifuge = create({ getToken: async (): Promise<string> => 'timer-token' });
    await connect(c);

    const timerRefresh: Promise<any> = server.waitFor((cmd: any): boolean => cmd.refresh !== undefined && refreshCommands().length >= 2);
    await c.refresh('new-token');
    await timerRefresh;

    expect(refreshCommands()[1].refresh.token).toBe('timer-token');
  });

  test('a timer refresh and an explicit refresh never have two refresh commands in flight', async (): Promise<void> => {
    const inFlight: number[] = [];
    let overlapped: boolean = false;
    let timerRequested: () => void = (): void => undefined;
    const timerRequest: Promise<void> = new Promise<void>((resolve: () => void): void => {
      timerRequested = resolve;
    });
    server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
    server.onCommand = (cmd: any): any => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      if (inFlight.length > 0) {
        overlapped = true;
      }
      inFlight.push(cmd.id);
      return holdReply;
    };
    const c: Centrifuge = create({
      token: 'initial-token',
      getToken: async (): Promise<string> => {
        timerRequested();
        return 'timer-token';
      },
    });
    await connect(c);

    const explicit: Promise<AuthContext> = c.refresh('explicit-token');
    // The timer fires on the ttl of the connect reply, while the explicit refresh is still in flight.
    await timerRequest;
    const explicitCommand: any = await server.waitFor((cmd: any): boolean => cmd.refresh !== undefined && cmd.refresh.token === 'explicit-token');
    // The timer refresh waits for the explicit one: it is sent only after that reply.
    server.send({ id: explicitCommand.id, refresh: { expires: true, ttl: 60 } });
    inFlight.shift();
    await explicit;

    // The timer token was fetched while the explicit refresh was in flight, and the explicit token is accepted
    // since: the timer token is superseded, and not sent. Its getToken is not called again.
    await flushTimers();

    expect(overlapped).toBe(false);
    expect(refreshCommands().map((cmd: any): string => cmd.refresh.token)).toEqual(['explicit-token']);
  });

  test('the token of the next connect is the last accepted refresh, a timer refresh queued behind an explicit one included', async (): Promise<void> => {
    let timerTokenRequested: () => void = (): void => undefined;
    const timerRequest: Promise<void> = new Promise<void>((resolve: () => void): void => {
      timerTokenRequested = resolve;
    });
    server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
    server.onCommand = (cmd: any): any => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      return cmd.refresh.token === 'anon-token' ? holdReply : { id: cmd.id, refresh: {} };
    };
    const c: Centrifuge = create({
      token: 'initial-token',
      getToken: async (): Promise<string> => {
        timerTokenRequested();
        return 'timer-token';
      },
    });
    await connect(c);

    const anon: Promise<AuthContext> = c.refresh('anon-token');
    // The timer fires while the explicit refresh is in flight, and waits for it.
    await timerRequest;
    const anonCommand: any = await server.waitFor((cmd: any): boolean => cmd.refresh !== undefined && cmd.refresh.token === 'anon-token');
    server.send({ id: anonCommand.id, refresh: {} });
    await anon;
    await flushTimers();

    const reconnected: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
    server.closeConnection();
    await reconnected;

    expect(connectCommands()[1].connect.token).toBe('anon-token');
  });

  test('an rpc waiting for a refresh ends at its deadline, also when the refresh waits for a reconnect', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? holdReply : undefined);
    const c: Centrifuge = create({ timeout: 300, minReconnectDelay: 5000, maxReconnectDelay: 5000 });
    await connect(c);
    const first: Promise<AuthContext> = c.refresh('token-a');
    const second: Promise<AuthContext> = c.refresh('token-b');
    first.catch((): void => undefined);
    second.catch((): void => undefined);
    await server.waitFor((cmd: any): boolean => cmd.refresh !== undefined && cmd.refresh.token === 'token-a');

    const calling: Promise<unknown> = c.rpc('echo', codecCase.encode({}));
    // The connection drops: the refresh in flight is lost, and the one behind it waits for a late reconnect.
    server.closeConnection();

    await expect(calling).rejects.toMatchObject({ code: errorCodes.timeout });
  });

  test('refresh calls waiting for a connection are bounded by maxRefreshCalls', async (): Promise<void> => {
    const c: Centrifuge = create();
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? { id: cmd.id, refresh: {} } : undefined);
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

  test('a timer refresh refused with 103 keeps the connection, reports an error and is not retried', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? { id: cmd.id, error: { code: 103, message: 'refused' } } : undefined);
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const c: Centrifuge = create({ getToken: async (): Promise<string> => 'timer-token' });
      const failed: Promise<any> = new Promise<any>((resolve: (ctx: any) => void): void => {
        c.once('error', (ctx: any): void => resolve(ctx));
      });
      await connect(c);

      jest.advanceTimersByTime(1000);
      await expect(failed).resolves.toMatchObject({ type: 'refresh', error: { code: 103 } });
      await server.waitForCount((cmd: any): boolean => cmd.refresh !== undefined, 1);
      expect(c.state).toBe(State.Connected);
      // No retry is scheduled: the refresh timer has nothing left to run.
      expect(jest.getTimerCount()).toBe(0);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a timer refresh refused with 109 is reported and retried with a fresh token after the backoff', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
    let refreshes: number = 0;
    server.onCommand = (cmd: any): any => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      refreshes++;
      return refreshes === 1
        ? { id: cmd.id, error: { code: 109, message: 'token expired' } }
        : { id: cmd.id, refresh: {} };
    };
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      let tokens: number = 0;
      const c: Centrifuge = create({ getToken: async (): Promise<string> => `timer-token-${++tokens}` });
      const failed: Promise<any> = new Promise<any>((resolve: (ctx: any) => void): void => {
        c.once('error', (ctx: any): void => resolve(ctx));
      });
      await connect(c);

      jest.advanceTimersByTime(1000);
      await expect(failed).resolves.toMatchObject({ type: 'refresh', error: { code: 109 } });
      expect(c.state).toBe(State.Connected);

      // The retry waits for its backoff of 5 to 10 seconds, then asks for a token again.
      jest.advanceTimersByTime(10000);
      await server.waitForCount((cmd: any): boolean => cmd.refresh !== undefined, 2);
      expect(refreshCommands()[1].refresh.token).not.toBe(refreshCommands()[0].refresh.token);
      expect(c.state).toBe(State.Connected);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a timer refresh whose token arrives after a refresh() call started is dropped, and the identity of the call stays', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated', expires: true, ttl: 1 };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'bot' },
    } : undefined);
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      // The first token fetch of the timer is answered late, with a token of another user.
      let fetches: number = 0;
      let answerFirst: (token: string) => void = (): void => {};
      const getToken: () => Promise<string> = (): Promise<string> => {
        fetches++;
        if (fetches === 1) {
          return new Promise<string>((resolve: (token: string) => void): void => {
            answerFirst = resolve;
          });
        }
        return Promise.resolve('t2');
      };
      const c: Centrifuge = create({ token: 't0', getToken: getToken });
      await connect(c);

      jest.advanceTimersByTime(1000);
      await until((): boolean => fetches === 1);
      const explicit: AuthContext = await c.refresh('t2');
      expect(explicit).toMatchObject({ user: 'bob', authClass: 'bot' });

      answerFirst('t1');
      await new Promise<void>((resolve: () => void): void => {
        setImmediate(resolve);
      });
      // The superseded token is not sent, and the token is not fetched again: the explicit token of the connection
      // is accepted, and the timer is not armed for a token without expiry.
      jest.advanceTimersByTime(60000);
      await flushTimers();
      expect(fetches).toBe(1);
    } finally {
      jest.useRealTimers();
    }

    expect(refreshCommands().map((cmd: any): string => cmd.refresh.token)).toEqual(['t2']);
    expect(authEvents[authEvents.length - 1]).toMatchObject({ user: 'bob', authClass: 'bot' });
  });

  test('a timer refresh superseded by a refused refresh() is retried by the timer, which the refusal left unarmed', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated', expires: true, ttl: 1 };
    server.onCommand = (cmd: any): any => {
      if (cmd.refresh === undefined) {
        return undefined;
      }
      return cmd.refresh.token === 't2'
        ? { id: cmd.id, error: { code: 103, message: 'transition not allowed' } }
        : { id: cmd.id, refresh: { expires: true, ttl: 60 } };
    };
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      let fetches: number = 0;
      let answerFirst: (token: string) => void = (): void => {};
      const getToken: () => Promise<string> = (): Promise<string> => {
        fetches++;
        if (fetches === 1) {
          return new Promise<string>((resolve: (token: string) => void): void => {
            answerFirst = resolve;
          });
        }
        return Promise.resolve('t3');
      };
      const c: Centrifuge = create({ token: 't0', getToken: getToken });
      await connect(c);

      jest.advanceTimersByTime(1000);
      await until((): boolean => fetches === 1);
      const refused: Promise<AuthContext> = c.refresh('t2');
      await expect(refused).rejects.toMatchObject({ code: 103 });

      // The timer token is superseded by the refused refresh(): it is not sent. The refusal left the timer unarmed,
      // so the superseded timer arms it, and the connection refreshes again when that retry is due.
      answerFirst('t1');
      await flushTimers();
      expect(refreshCommands().map((cmd: any): string => cmd.refresh.token)).toEqual(['t2']);

      jest.advanceTimersByTime(20000);
      await until((): boolean => fetches === 2);
      await server.waitForCount((cmd: any): boolean => cmd.refresh !== undefined, 2);
    } finally {
      jest.useRealTimers();
    }

    expect(refreshCommands().map((cmd: any): string => cmd.refresh.token)).toEqual(['t2', 't3']);
  });

  test('an auth handler that throws on a refresh does not fail the refresh, whose token is accepted', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', user: 'alice', auth_class: 'authenticated' };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? {
      id: cmd.id,
      refresh: { user: 'bob', auth_class: 'bot' },
    } : undefined);
    const c: Centrifuge = create();
    c.on('auth', (ctx: AuthContext): void => {
      if (ctx.user === 'bob') {
        throw new Error('handler failed');
      }
    });
    // Applications see the exception as an unhandled rejection; captured here instead.
    const reported: any[] = [];
    (c as any)._reportDispatchError = (err: any): void => {
      reported.push(err);
    };
    await connect(c);

    await expect(c.refresh('new-token')).resolves.toMatchObject({ user: 'bob', authClass: 'bot' });
    expect(reported.map((err: any): string => err.message)).toEqual(['handler failed']);
  });

  test('a refresh the connection loss ends keeps its token for the reconnect, which carries it', async (): Promise<void> => {
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? holdReply : undefined);
    const c: Centrifuge = create({ token: 'A' });
    const refreshing: Promise<AuthContext> = c.refresh('B');
    refreshing.catch((): void => {});
    const connected: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
    c.connect();
    await connected;
    await server.waitFor((cmd: any): boolean => cmd.refresh !== undefined);

    server.closeConnection();
    await expect(refreshing).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    const reconnected: Promise<void> = new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
    await reconnected;

    expect(connectCommands().map((cmd: any): string => cmd.connect.token)).toEqual(['B', 'B']);
  });

  test('a timer refresh refused with 101 disconnects with that code, as before', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', expires: true, ttl: 1 };
    server.onCommand = (cmd: any): any => (cmd.refresh !== undefined ? { id: cmd.id, error: { code: 101, message: 'unauthorized' } } : undefined);
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
    try {
      const c: Centrifuge = create({ getToken: async (): Promise<string> => 'timer-token' });
      const disconnected: Promise<any> = new Promise<any>((resolve: (ctx: any) => void): void => {
        c.once('disconnected', (ctx: any): void => resolve(ctx));
      });
      await connect(c);

      jest.advanceTimersByTime(1000);
      await expect(disconnected).resolves.toMatchObject({ code: 101 });
      expect(c.state).toBe(State.Disconnected);
    } finally {
      jest.useRealTimers();
    }
  });
});
