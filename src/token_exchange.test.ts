import { Centrifuge, UnauthorizedError } from './centrifuge';
import { CodecCase, WaitableFakeServer, codecCases, connectClient, disconnectAll, holdReply, until } from './testing/codecs';
import { Options, State, TokenExchange, TokenExchangeContext, TokenExchangeResult } from './types';

// The token exchange of the tokenExchange option, over both codecs. The fake issuer answers the exchange rpc, and every
// exchange rotates the refresh token: the app starts with refresh-1, and the n-th exchange issues access-n and
// refresh-(n+1). The order of what the server and the app saw is kept in one list: connect, rpc:<method>, refresh,
// store:<token>.

/** The identity of a refresh reply, with its ttl. */
function refreshReply(cmd: any, ttl: number): any {
  return { id: cmd.id, refresh: { expires: true, ttl: ttl, user: 'alice', auth_class: 'authenticated' } };
}

/** The kind of a command: the one field of it that is set, besides its id. */
function commandKind(cmd: any): string {
  return Object.keys(cmd).find((key: string): boolean => key !== 'id' && cmd[key] !== null && cmd[key] !== undefined) ?? '';
}

/** Records the events of a client as strings: the kind, and the type, channel or code the event carries. */
function record(c: Centrifuge): string[] {
  const events: string[] = [];
  c.on('connected', (): void => {
    events.push('connected');
  });
  c.on('auth', (): void => {
    events.push('auth');
  });
  c.on('subscribed', (ctx: any): void => {
    events.push('subscribed:' + ctx.channel);
  });
  c.on('unsubscribed', (ctx: any): void => {
    events.push('unsubscribed:' + ctx.channel);
  });
  c.on('error', (ctx: any): void => {
    events.push('error:' + ctx.type);
  });
  c.on('disconnected', (ctx: any): void => {
    events.push('disconnected:' + ctx.code);
  });
  return events;
}

/** Lets the socket deliver what it has: a few turns of the event loop, no wait for a time. */
async function flush(): Promise<void> {
  for (let i: number = 0; i < 20; i++) {
    await new Promise<void>((resolve: () => void): void => {
      setImmediate(resolve);
    });
  }
}

describe.each(codecCases)('token exchange over $name', (codecCase: CodecCase): void => {
  let server: WaitableFakeServer;
  const clients: Centrifuge[] = [];
  let order: string[];
  let stored: string;
  let issued: number;

  beforeEach(async (): Promise<void> => {
    server = await WaitableFakeServer.start(codecCase.serverCodec);
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 0 };
    order = [];
    stored = 'refresh-1';
    issued = 0;
  });

  afterEach(async (): Promise<void> => {
    jest.useRealTimers();
    disconnectAll(clients);
    await server.close();
  });

  // The fake issuer: the exchange rpc gets its reply, and the access token and the rotated refresh token are the next ones.
  async function issue(ctx: TokenExchangeContext): Promise<TokenExchangeResult> {
    issued++;
    await ctx.rpc('exchange', codecCase.encode({ refresh: ctx.refreshToken }));
    return { token: `access-${issued}`, refreshToken: `refresh-${issued + 1}` };
  }

  // The options of the app's storage and issuer: each one is replaced by the overrides given.
  function tokenExchange(overrides: Partial<TokenExchange> = {}): TokenExchange {
    return {
      load: (): string => stored,
      store: (refreshToken: string): void => {
        order.push('store:' + refreshToken);
        stored = refreshToken;
      },
      exchange: (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => issue(ctx),
      ...overrides,
    };
  }

  // Lets the timers run on a fake clock: the socket and the event loop keep their real turns.
  function useFakeClock(): void {
    jest.useFakeTimers({ doNotFake: ['nextTick', 'queueMicrotask', 'setImmediate'] });
  }

  // Answers the connection: rpc calls get a reply, refresh calls the answer given (by default an identity with a ttl).
  function serve(options: { refresh?: (cmd: any) => any; connect?: (cmd: any) => any; ttl?: number } = {}): void {
    server.onCommand = (cmd: any): any => {
      if (cmd.connect !== undefined) {
        order.push('connect');
        return options.connect ? options.connect(cmd) : undefined;
      }
      if (cmd.rpc !== undefined) {
        order.push('rpc:' + cmd.rpc.method);
        return { id: cmd.id, rpc: { data: codecCase.encode({ method: cmd.rpc.method }) } };
      }
      if (cmd.refresh !== undefined) {
        order.push('refresh');
        return options.refresh ? options.refresh(cmd) : refreshReply(cmd, options.ttl ?? 900);
      }
      return undefined;
    };
  }

  function create(overrides: Partial<TokenExchange> = {}, options: Partial<Options> = {}): Centrifuge {
    const c: Centrifuge = codecCase.createClient(server.url, { ...options, tokenExchange: tokenExchange(overrides) });
    clients.push(c);
    return c;
  }

  // The commands the server received of one kind, in order.
  function commands(kind: string): any[] {
    return server.received.filter((cmd: any): boolean => commandKind(cmd) === kind);
  }

  function nextConnected(c: Centrifuge): Promise<void> {
    return new Promise<void>((resolve: () => void): void => {
      c.once('connected', (): void => resolve());
    });
  }

  test('bootstraps over the connection: exchange, refresh, then one connected event', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create();
    const events: string[] = record(c);
    const identities: any[] = [];
    c.on('connected', (ctx: any): void => {
      identities.push({ user: ctx.user, authClass: ctx.authClass });
    });
    await connectClient(c);

    expect(order).toEqual(['connect', 'rpc:exchange', 'store:refresh-2', 'refresh']);
    expect(codecCase.decode(commands('rpc')[0].rpc.data)).toEqual({ refresh: 'refresh-1' });
    expect(commands('refresh')[0].refresh.token).toBe('access-1');
    expect(identities).toEqual([{ user: 'alice', authClass: 'authenticated' }]);
    expect(events).toEqual(['connected', 'auth']);
  });

  test('stores the rotated refresh token before the access token it came with is sent', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create();
    await connectClient(c);

    expect(order).toEqual(['connect', 'rpc:exchange', 'store:refresh-2', 'refresh']);
    expect(stored).toBe('refresh-2');
  });

  test('does not store when the issuer returns no rotated token, or the same one', async (): Promise<void> => {
    serve();
    const none: Centrifuge = create({
      exchange: async (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => {
        await ctx.rpc('exchange', codecCase.encode({}));
        return { token: 'access-none' };
      },
    });
    await connectClient(none);
    expect(order.filter((entry: string): boolean => entry.startsWith('store:'))).toEqual([]);
    disconnectAll([none]);

    stored = 'refresh-same';
    const same: Centrifuge = create({
      exchange: async (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => {
        await ctx.rpc('exchange', codecCase.encode({}));
        return { token: 'access-same', refreshToken: 'refresh-same' };
      },
    });
    await connectClient(same);
    expect(order.filter((entry: string): boolean => entry.startsWith('store:'))).toEqual([]);
    expect(stored).toBe('refresh-same');
  });

  test('a kept valid access token connects with the token: no exchange, no refresh', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create();
    await connectClient(c);

    const reconnected: Promise<void> = nextConnected(c);
    server.closeConnection();
    await reconnected;

    expect(commands('connect').map((cmd: any): string => cmd.connect.token ?? '')).toEqual(['', 'access-1']);
    expect(commands('rpc')).toHaveLength(1);
    expect(commands('refresh')).toHaveLength(1);
  });

  test('an expired access token is dropped on the reconnect, which then bootstraps again', async (): Promise<void> => {
    serve({ ttl: 10 });
    useFakeClock();
    try {
      const c: Centrifuge = create();
      await connectClient(c);

      const reconnected: Promise<void> = nextConnected(c);
      server.closeConnection();
      await until((): boolean => c.state === State.Connecting);
      // The expiry passes while the client is disconnected; the reconnect runs after that.
      jest.setSystemTime(Date.now() + 11000);
      jest.advanceTimersByTime(100);
      await reconnected;

      expect(commands('connect').map((cmd: any): string => cmd.connect.token ?? '')).toEqual(['', '']);
      expect(commands('rpc')).toHaveLength(2);
      expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2']);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a state invalidated disconnect drops the access token: the reconnect is anonymous and bootstraps again', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create();
    await connectClient(c);

    const reconnected: Promise<void> = nextConnected(c);
    server.disconnect(3014, 'state invalidated');
    await reconnected;

    expect(commands('connect').map((cmd: any): string => cmd.connect.token ?? '')).toEqual(['', '']);
    expect(commands('rpc')).toHaveLength(2);
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2']);
    expect(c.state).toBe(State.Connected);
  });

  test('a token refused at connect with 109 reconnects anonymously and bootstraps, without a configuration error', async (): Promise<void> => {
    serve({
      connect: (cmd: any): any => (cmd.connect.token ? { id: cmd.id, error: { code: 109, message: 'token expired' } } : undefined),
    });
    const c: Centrifuge = create();
    const events: string[] = record(c);
    await connectClient(c);

    const reconnected: Promise<void> = nextConnected(c);
    server.closeConnection();
    await reconnected;

    expect(events.filter((entry: string): boolean => entry.startsWith('error:'))).toEqual(['error:connect']);
    expect(commands('connect').map((cmd: any): string => cmd.connect.token ?? '')).toEqual(['', 'access-1', '']);
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2']);
    expect(c.state).toBe(State.Connected);
  });

  test('renews at a random moment between 0.7 and 0.9 of the ttl, over the connection', async (): Promise<void> => {
    serve({ ttl: 10 });
    useFakeClock();
    try {
      for (let run: number = 0; run < 20; run++) {
        const c: Centrifuge = create();
        const events: string[] = record(c);
        await connectClient(c);
        const rpcs: number = commands('rpc').length;
        const refreshes: number = commands('refresh').length;

        // Not before 0.7 of the ttl: 6.9 seconds pass, and no exchange has gone out.
        jest.advanceTimersByTime(6900);
        await flush();
        expect(commands('rpc')).toHaveLength(rpcs);

        // Not after 0.9 of the ttl: the exchange and the refresh have gone out by 9 seconds.
        jest.advanceTimersByTime(2100);
        await until((): boolean => commands('refresh').length === refreshes + 1);
        expect(commands('rpc')).toHaveLength(rpcs + 1);
        expect(commands('refresh')[refreshes].refresh.token).not.toBe(commands('refresh')[refreshes - 1].refresh.token);
        // The identity is the same: no auth event.
        expect(events).toEqual(['connected', 'auth']);
        disconnectAll(clients);
      }
    } finally {
      jest.useRealTimers();
    }
  });

  test('an UnauthorizedError with the same refresh token is terminal: disconnected with code 1, no reconnect, no store', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create({
      exchange: async (): Promise<TokenExchangeResult> => {
        throw new UnauthorizedError('refused');
      },
    });
    const events: string[] = record(c);
    c.connect();
    await until((): boolean => c.state === State.Disconnected);
    await flush();

    expect(events).toEqual(['disconnected:1']);
    expect(commands('connect')).toHaveLength(1);
    expect(order.filter((entry: string): boolean => entry.startsWith('store:'))).toEqual([]);
  });

  test('an UnauthorizedError with another refresh token is retried once at once, without an event', async (): Promise<void> => {
    serve();
    let loads: number = 0;
    const exchanged: string[] = [];
    const c: Centrifuge = create({
      load: (): string => (loads++ === 0 ? 'refresh-1' : 'refresh-2'),
      exchange: async (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => {
        exchanged.push(ctx.refreshToken);
        if (exchanged.length === 1) {
          throw new UnauthorizedError('refused');
        }
        return issue(ctx);
      },
    });
    const events: string[] = record(c);
    await connectClient(c);

    expect(exchanged).toEqual(['refresh-1', 'refresh-2']);
    expect(events.filter((entry: string): boolean => entry.startsWith('error:') || entry.startsWith('disconnected:'))).toEqual([]);
    expect(commands('connect')).toHaveLength(1);
  });

  test('an UnauthorizedError from load at bootstrap is terminal, as at renewal: disconnected with code 1, no retry', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create({
      load: (): string => {
        throw new UnauthorizedError('refresh token deleted');
      },
    });
    const events: string[] = record(c);
    c.connect();
    await until((): boolean => c.state === State.Disconnected);
    await flush();

    expect(events).toEqual(['disconnected:1']);
    expect(commands('connect')).toHaveLength(1);
    expect(commands('rpc')).toHaveLength(0);
  });

  test('a rejected exchange at bootstrap reports connectToken and retries in place after the backoff', async (): Promise<void> => {
    serve();
    useFakeClock();
    try {
      let failed: boolean = false;
      const c: Centrifuge = create({
        exchange: (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => {
          if (!failed) {
            failed = true;
            return Promise.reject(new Error('network down'));
          }
          return issue(ctx);
        },
      });
      const events: string[] = record(c);
      const connected: Promise<void> = nextConnected(c);
      c.connect();
      await until((): boolean => events.includes('error:connectToken'));
      expect(c.state).toBe(State.Connecting);

      jest.advanceTimersByTime(10000);
      await connected;

      expect(commands('connect')).toHaveLength(1);
      expect(commands('refresh')).toHaveLength(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a rejected exchange at renewal reports refreshToken and retries after the backoff', async (): Promise<void> => {
    serve({ ttl: 10 });
    useFakeClock();
    try {
      let exchanges: number = 0;
      const c: Centrifuge = create({
        exchange: (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => {
          exchanges++;
          if (exchanges === 2) {
            return Promise.reject(new Error('network down'));
          }
          return issue(ctx);
        },
      });
      const events: string[] = record(c);
      await connectClient(c);

      jest.advanceTimersByTime(9000);
      await until((): boolean => events.includes('error:refreshToken'));
      expect(c.state).toBe(State.Connected);

      jest.advanceTimersByTime(10000);
      await until((): boolean => commands('refresh').length === 2);
      expect(commands('refresh')[1].refresh.token).toBe('access-2');
      expect(c.state).toBe(State.Connected);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a refresh refused with 103 at bootstrap disconnects with 103, without a retry', async (): Promise<void> => {
    serve({ refresh: (cmd: any): any => ({ id: cmd.id, error: { code: 103, message: 'denied' } }) });
    const c: Centrifuge = create();
    const events: string[] = record(c);
    c.connect();
    await until((): boolean => c.state === State.Disconnected);
    await flush();

    expect(events).toEqual(['disconnected:103']);
    expect(commands('connect')).toHaveLength(1);
  });

  test('a refresh refused with 109 at bootstrap reports connect and retries with a new exchange after the backoff', async (): Promise<void> => {
    let refreshes: number = 0;
    serve({
      refresh: (cmd: any): any => {
        refreshes++;
        return refreshes === 1 ? { id: cmd.id, error: { code: 109, message: 'expired' } } : refreshReply(cmd, 900);
      },
    });
    useFakeClock();
    try {
      const c: Centrifuge = create();
      const events: string[] = record(c);
      const connected: Promise<void> = nextConnected(c);
      c.connect();
      await until((): boolean => events.includes('error:connect'));
      expect(c.state).toBe(State.Connecting);

      jest.advanceTimersByTime(10000);
      await connected;

      expect(commands('connect')).toHaveLength(1);
      expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2']);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a subscribe push between the exchange and the refresh reply gives one subscribed event, after connected', async (): Promise<void> => {
    serve({
      refresh: (cmd: any): any => {
        server.sendPush({ channel: 'news', subscribe: { epoch: 'e1', offset: 5, recoverable: false, positioned: false } });
        return refreshReply(cmd, 900);
      },
    });
    const c: Centrifuge = create();
    const events: string[] = record(c);
    await connectClient(c);
    await flush();

    expect(events).toEqual(['connected', 'auth', 'subscribed:news']);
  });

  test('an unsubscribe push for a subscription held by the bootstrap gives neither event', async (): Promise<void> => {
    serve({
      refresh: (cmd: any): any => {
        server.sendPush({ channel: 'news', subscribe: { epoch: 'e1', offset: 5, recoverable: false, positioned: false } });
        server.unsubscribe('news', 2500, 'gone');
        return refreshReply(cmd, 900);
      },
    });
    const c: Centrifuge = create();
    const events: string[] = record(c);
    await connectClient(c);
    await flush();

    expect(events).toEqual(['connected', 'auth']);
  });

  test('a connection lost during the exchange bootstraps again, and the late result of the first exchange is ignored', async (): Promise<void> => {
    serve();
    let calls: number = 0;
    let release: (result: TokenExchangeResult) => void = (): void => {};
    const c: Centrifuge = create({
      exchange: (ctx: TokenExchangeContext): Promise<TokenExchangeResult> => {
        calls++;
        if (calls === 1) {
          return new Promise<TokenExchangeResult>((resolve: (result: TokenExchangeResult) => void): void => {
            release = resolve;
          });
        }
        return issue(ctx);
      },
    });
    const events: string[] = record(c);
    const connected: Promise<void> = nextConnected(c);
    c.connect();
    await until((): boolean => calls === 1);
    server.closeConnection();
    await connected;

    release({ token: 'stale-access', refreshToken: 'stale-refresh' });
    await flush();

    expect(calls).toBe(2);
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1']);
    expect(order.filter((entry: string): boolean => entry.startsWith('store:'))).toEqual(['store:refresh-2']);
    expect(stored).toBe('refresh-2');
    expect(events.filter((entry: string): boolean => entry === 'connected')).toHaveLength(1);
  });

  test('a connection lost during the exchange stores its rotated token when nothing rotated meanwhile, and uses no access token', async (): Promise<void> => {
    let calls: number = 0;
    let release: (result: TokenExchangeResult) => void = (): void => {};
    const c: Centrifuge = create({
      exchange: (): Promise<TokenExchangeResult> => {
        calls++;
        return new Promise<TokenExchangeResult>((resolve: (result: TokenExchangeResult) => void): void => {
          release = resolve;
        });
      },
    });
    const events: string[] = record(c);
    serve();
    c.connect();
    await until((): boolean => calls === 1);
    c.disconnect();

    release({ token: 'stale-access', refreshToken: 'refresh-2' });
    await flush();

    expect(order).toEqual(['connect', 'store:refresh-2']);
    expect(stored).toBe('refresh-2');
    expect(commands('refresh')).toHaveLength(0);
    expect(events).not.toContain('connected');
  });

  test('a connection lost during the bootstrap refresh drops the unaccepted access token: the reconnect is anonymous and bootstraps', async (): Promise<void> => {
    let refreshes: number = 0;
    serve({
      refresh: (cmd: any): any => {
        refreshes++;
        return refreshes === 1 ? holdReply : refreshReply(cmd, 900);
      },
    });
    const c: Centrifuge = create();
    const reconnected: Promise<void> = nextConnected(c);
    c.connect();
    await until((): boolean => commands('refresh').length === 1);
    server.closeConnection();
    await reconnected;

    expect(commands('connect').map((cmd: any): string => cmd.connect.token ?? '')).toEqual(['', '']);
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2']);
    expect(c.state).toBe(State.Connected);
  });

  test('refresh() during the connect waits for the connection, then exchanges over it', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create();
    const connected: Promise<void> = nextConnected(c);
    c.connect();
    const refreshed: Promise<any> = c.refresh();
    await connected;
    await refreshed;

    expect(order).toEqual(['connect', 'rpc:exchange', 'store:refresh-2', 'refresh', 'rpc:exchange', 'store:refresh-3', 'refresh']);
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2']);
  });

  test('an empty refresh token at bootstrap leaves the connection anonymous, with no rpc', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create({ load: (): null => null });
    const events: string[] = record(c);
    await connectClient(c);

    expect(c.state).toBe(State.Connected);
    expect(commands('rpc')).toHaveLength(0);
    expect(commands('refresh')).toHaveLength(0);
    expect(events.filter((entry: string): boolean => entry.startsWith('error:'))).toEqual([]);
  });

  test('an empty refresh token at renewal is terminal: disconnected with code 1', async (): Promise<void> => {
    serve({ ttl: 10 });
    useFakeClock();
    try {
      let loads: number = 0;
      const c: Centrifuge = create({ load: (): string | null => (loads++ === 0 ? 'refresh-1' : null) });
      const events: string[] = record(c);
      await connectClient(c);

      jest.advanceTimersByTime(9000);
      await until((): boolean => events.includes('disconnected:1'));

      expect(c.state).toBe(State.Disconnected);
      expect(commands('connect')).toHaveLength(1);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a failing store reports tokenStore, and the bootstrap completes', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create({
      store: (): void => {
        throw new Error('disk full');
      },
    });
    const events: string[] = record(c);
    await connectClient(c);

    expect(events).toContain('error:tokenStore');
    expect(c.state).toBe(State.Connected);
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1']);
  });

  test('an error handler that throws on tokenStore does not end the exchange: the bootstrap completes with its access token', async (): Promise<void> => {
    serve();
    const reported: any[] = [];
    const c: Centrifuge = create({
      store: (): void => {
        throw new Error('disk full');
      },
    });
    (c as any)._reportDispatchError = (err: any): void => {
      reported.push(err);
    };
    c.on('error', (ctx: any): void => {
      if (ctx.type === 'tokenStore') {
        throw new Error('broken logger');
      }
    });
    const events: string[] = record(c);
    await connectClient(c);

    expect(reported.map((err: any): string => err.message)).toEqual(['broken logger']);
    expect(events).toContain('connected');
    expect(c.state).toBe(State.Connected);
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1']);
  });

  test('tokenExchange together with getToken throws at construction', async (): Promise<void> => {
    expect(() => codecCase.createClient(server.url, {
      getToken: async (): Promise<string> => 'token',
      tokenExchange: tokenExchange(),
    })).toThrow('tokenExchange and getToken are exclusive');
  });

  test('refresh() without a token exchanges over the connection, and refresh(token) does not', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create();
    await connectClient(c);

    const identity: any = await c.refresh();
    expect(identity).toMatchObject({ user: 'alice', authClass: 'authenticated' });
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2']);
    expect(order.filter((entry: string): boolean => entry.startsWith('rpc:'))).toHaveLength(2);

    await c.refresh('manual-token');
    expect(commands('refresh').map((cmd: any): string => cmd.refresh.token)).toEqual(['access-1', 'access-2', 'manual-token']);
    expect(order.filter((entry: string): boolean => entry.startsWith('rpc:'))).toHaveLength(2);
  });

  test('an rpc call made before connect() goes out after the bootstrap refresh', async (): Promise<void> => {
    serve();
    const c: Centrifuge = create();
    c.connect();
    const reply: any = await c.rpc('app.method', codecCase.encode({ x: 1 }));

    expect(codecCase.decode(reply.data)).toEqual({ method: 'app.method' });
    expect(order.indexOf('rpc:exchange')).toBeLessThan(order.indexOf('refresh'));
    expect(order.indexOf('refresh')).toBeLessThan(order.indexOf('rpc:app.method'));
  });

  test('the debug output and the flow keep neither token', async (): Promise<void> => {
    serve();
    const spy: jest.SpyInstance = jest.spyOn(console, 'debug').mockImplementation((): void => {});
    try {
      const c: Centrifuge = create({}, { debug: true });
      await connectClient(c);

      const logged: string = JSON.stringify(spy.mock.calls);
      expect(logged).toContain('connection token exchanged');
      expect(logged).not.toContain('refresh-1');
      expect(logged).not.toContain('access-1');
      const flow: any = c['_tokenExchange'];
      expect(Object.values(flow)).not.toContain('refresh-1');
      // The flow's own state, without the client and the host it reaches: no refresh token in any of it, and no bootstrap left.
      const kept: string = JSON.stringify(Object.entries(flow).filter((entry: [string, any]): boolean => entry[0] !== 'client' && entry[0] !== 'host'));
      expect(kept).not.toContain('refresh-');
      expect(flow.bootstrap).toBeNull();
      disconnectAll(clients);
    } finally {
      spy.mockRestore();
    }
  });

  test('a connect reply that already carries a user is not bootstrapped: no exchange, connected with that user', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 0, user: 'proxy-user' };
    serve();
    const c: Centrifuge = create();
    const users: string[] = [];
    c.on('connected', (ctx: any): void => {
      users.push(ctx.user);
    });
    await connectClient(c);

    expect(users).toEqual(['proxy-user']);
    expect(commands('rpc')).toHaveLength(0);
    expect(commands('refresh')).toHaveLength(0);
    expect(stored).toBe('refresh-1');
  });

  test('without the option the client holds no flow, and a connect creates no extra timer', async (): Promise<void> => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 0, expires: true, ttl: 10 };
    useFakeClock();
    try {
      const c: Centrifuge = codecCase.createClient(server.url, { token: 'static-token' });
      clients.push(c);
      expect(c['_tokenExchange']).toBeNull();

      const before: number = jest.getTimerCount();
      await connectClient(c);
      // The refresh timer of the ttl, as upstream arms it: nothing else.
      expect(jest.getTimerCount() - before).toBe(1);
    } finally {
      jest.useRealTimers();
    }
  });
});
