import { Centrifuge } from './centrifuge';
import { FakeCentrifugoServer } from './fakeServer';
import { errorCodes } from './codes';
import { Options, State } from './types';
import { CodecCase, codecCases, rpcOptionsWireFlag } from './testing/codecs';
import { loggedConfig } from './rpc_options';
import { format } from 'util';

// RPC options (per-call headers), the rpc capability, server error fields and server
// time. Every case runs over both codecs (see testing/codecs.ts).

describe.each(codecCases)('RPC options over $name', (codecCase: CodecCase) => {
  let server: FakeCentrifugoServer;
  const clients: Centrifuge[] = [];

  beforeEach(async () => {
    server = await FakeCentrifugoServer.start(codecCase.serverCodec);
  });

  afterEach(async () => {
    for (const c of clients) {
      c.disconnect();
    }
    clients.length = 0;
    await server.close();
  });

  async function connect(options?: Partial<Options>): Promise<Centrifuge> {
    const c = codecCase.createClient(server.url, options);
    clients.push(c);
    const connected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    c.connect();
    await connected;
    expect(c.state).toBe(State.Connected);
    return c;
  }

  function rpcCommands(): any[] {
    return server.received.filter(cmd => cmd.rpc !== undefined);
  }

  test('advertises bit 32 in the connect request', async () => {
    await connect();
    const connectCmd = server.received.find(cmd => cmd.connect !== undefined);
    expect(connectCmd.connect.flag).toBe(rpcOptionsWireFlag);
  });

  test('capability is on when the connect reply echoes bit 32', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', flag: rpcOptionsWireFlag };
    const c = await connect();
    expect(c.capabilities.rpcOptions).toBe(true);
  });

  test('capability is on when bit 32 comes with other bits', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', flag: rpcOptionsWireFlag + 1 };
    const c = await connect();
    expect(c.capabilities.rpcOptions).toBe(true);
  });

  test('capability is off when the connect reply has no bit 32', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', flag: 1 };
    const c = await connect();
    expect(c.capabilities.rpcOptions).toBe(false);
  });

  test('capability is off when the connect reply has no flag', async () => {
    const c = await connect();
    expect(c.capabilities.rpcOptions).toBe(false);
  });

  test('capabilities is read-only', async () => {
    const c = await connect();
    expect(() => {
      (c as any).capabilities = { rpcOptions: true };
    }).toThrow();
    expect(c.capabilities.rpcOptions).toBe(false);
  });

  test('capability is cleared when the transport closes and set again by the next connect reply', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', flag: rpcOptionsWireFlag };
    const c = await connect();
    expect(c.capabilities.rpcOptions).toBe(true);

    // The next connect command is the reconnect: the capability must already be cleared then.
    let capabilityOnReconnect: boolean | undefined;
    const reconnecting = new Promise<void>(resolve => {
      server.onCommand = (cmd: any) => {
        if (cmd.connect !== undefined) {
          capabilityOnReconnect = c.capabilities.rpcOptions;
          resolve();
        }
        return undefined;
      };
    });
    const reconnected = new Promise<void>(resolve => c.once('connected', () => resolve()));

    server.closeConnection();
    await reconnecting;
    await reconnected;

    expect(capabilityOnReconnect).toBe(false);
    expect(c.capabilities.rpcOptions).toBe(true);
  });

  test('headers are sent with the rpc when the server supports them', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', flag: rpcOptionsWireFlag };
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: codecCase.encode({ ok: true }) } } : undefined);
    const c = await connect();

    const result = await c.rpc('echo', codecCase.encode({ value: 1 }), { headers: { 'example-header': 'example-value' } });

    expect(codecCase.decode(result.data)).toEqual({ ok: true });
    expect(rpcCommands()[0].rpc.headers).toEqual({ 'example-header': 'example-value' });
    expect(rpcCommands()[0].rpc.method).toBe('echo');
  });

  test('headers are rejected locally without sending the call in legacy mode', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: codecCase.encode({}) } } : undefined);
    const c = await connect();

    await expect(c.rpc('echo', codecCase.encode({}), { headers: { 'example-header': 'example-value' } })).rejects.toMatchObject({
      code: errorCodes.unsupported,
    });
    expect(rpcCommands()).toHaveLength(0);
  });

  test('an empty headers object sends the call without headers in legacy mode', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: codecCase.encode('done') } } : undefined);
    const c = await connect();

    const result = await c.rpc('echo', codecCase.encode({}), { headers: {} });

    expect(codecCase.decode(result.data)).toBe('done');
    expect(Object.keys(rpcCommands()[0].rpc.headers ?? {})).toHaveLength(0);
  });

  test('the two-argument rpc sends no headers and works in both modes', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: codecCase.encode('plain') } } : undefined);
    const legacy = await connect();

    const result = await legacy.rpc('echo', codecCase.encode({ value: 1 }));

    expect(codecCase.decode(result.data)).toBe('plain');
    expect(rpcCommands()[0].rpc).toEqual({ method: 'echo', data: codecCase.encode({ value: 1 }) });
    expect(Object.keys(rpcCommands()[0].rpc.headers ?? {})).toHaveLength(0);
  });

  test('app error fields are surfaced on a failed rpc', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? {
      id: cmd.id,
      error: {
        code: 100,
        message: 'application failure',
        temporary: true,
        app_code: 1234,
        app_type: 'EXAMPLE_RETRY_AFTER',
        metadata: { 'example-key': 'example-value' },
      },
    } : undefined);
    const c = await connect();

    await expect(c.rpc('echo', codecCase.encode({}))).rejects.toMatchObject({
      code: 100,
      message: 'application failure',
      temporary: true,
      appCode: 1234,
      appType: 'EXAMPLE_RETRY_AFTER',
      metadata: { 'example-key': 'example-value' },
    });
  });

  test('app error fields stay absent when the server did not send them', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, error: { code: 100, message: 'plain failure' } } : undefined);
    const c = await connect();

    let caught: any;
    try {
      await c.rpc('echo', codecCase.encode({}));
    } catch (e) {
      caught = e;
    }

    expect(caught.code).toBe(100);
    expect(caught.appCode).toBeUndefined();
    expect(caught.appType).toBeUndefined();
    expect(caught.metadata).toBeUndefined();
    expect(caught).not.toHaveProperty('appCode');
    expect(caught).not.toHaveProperty('appType');
    expect(caught).not.toHaveProperty('metadata');
  });

  test('serverTime is local time before any reply carried a server time', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: codecCase.encode(null) } } : undefined);
    const c = await connect();
    await c.rpc('echo', codecCase.encode({}));

    expect(Math.abs(c.serverTime() - Date.now())).toBeLessThan(1000);
  });

  test('serverTime follows the server time of a reply', async () => {
    const skewMs = 100000;
    server.onCommand = (cmd: any) => (cmd.rpc ? {
      id: cmd.id,
      time: Date.now() + skewMs,
      rpc: { data: codecCase.encode(null) },
    } : undefined);
    const c = await connect();
    await c.rpc('echo', codecCase.encode({}));

    expect(Math.abs(c.serverTime() - (Date.now() + skewMs))).toBeLessThan(1000);
  });

  // The late reply of a call that timed out arrives after the call settled: it answers no pending command, so its
  // server time does not move the offset. The next reply (no time) follows it on the same connection, so once it is
  // handled the late one was handled too.
  test('a late reply to a timed out call does not set the server time', async () => {
    const skewMs = 100000;
    const lateCommandIds: number[] = [];
    server.onCommand = (cmd: any) => {
      if (cmd.rpc === undefined) {
        return undefined;
      }
      if (cmd.rpc.method === 'slow') {
        lateCommandIds.push(cmd.id);
        return undefined;
      }
      return { id: cmd.id, rpc: { data: codecCase.encode(null) } };
    };
    const c = await connect({ timeout: 200 });
    await expect(c.rpc('slow', codecCase.encode({}))).rejects.toMatchObject({ code: errorCodes.timeout });

    server.send({
      id: lateCommandIds[0],
      time: Date.now() + skewMs,
      rpc: { data: codecCase.encode(null) },
    });
    await c.rpc('echo', codecCase.encode({}));

    expect(Math.abs(c.serverTime() - Date.now())).toBeLessThan(1000);
  });

  test('an rpc timeout above the client timeout is refused before the call is sent', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: codecCase.encode(null) } } : undefined);
    const c = await connect({ timeout: 5000 });

    await expect(c.rpc('echo', codecCase.encode({}), { timeout: 6000 })).rejects.toMatchObject({
      code: errorCodes.badConfiguration,
    });
    await expect(c.rpc('echo', codecCase.encode({}), { timeout: 0 })).rejects.toMatchObject({
      code: errorCodes.badConfiguration,
    });
    expect(rpcCommands()).toHaveLength(0);
  });

  test('an rpc timeout shorter than the client timeout ends the call at its own deadline', async () => {
    server.onCommand = (): undefined => undefined;
    const c = await connect({ timeout: 5000 });

    await expect(c.rpc('silent', codecCase.encode({}), { timeout: 200 })).rejects.toMatchObject({
      code: errorCodes.timeout,
    });
    expect(rpcCommands()).toHaveLength(1);
  });

  test('old-shape replies without the new fields still work', async () => {
    server.onCommand = (cmd: any) => {
      if (cmd.rpc === undefined) {
        return undefined;
      }
      return cmd.rpc.method === 'fail'
        ? { id: cmd.id, error: { code: 100, message: 'legacy failure' } }
        : { id: cmd.id, rpc: { data: codecCase.encode(7) } };
    };
    const c = await connect();

    const result = await c.rpc('echo', codecCase.encode({}));
    expect(codecCase.decode(result.data)).toBe(7);
    expect(c.capabilities.rpcOptions).toBe(false);
    expect(Math.abs(c.serverTime() - Date.now())).toBeLessThan(1000);

    await expect(c.rpc('fail', codecCase.encode({}))).rejects.toMatchObject({ code: 100, message: 'legacy failure' });
  });
});

describe('loggedConfig', () => {
  test('the debug config shows whether a token is set, not the token', () => {
    const config: Options = { token: 'secret-jwt-value', instance: '' } as Options;
    const logged: Options = loggedConfig(config);
    expect(logged.token).toBe('<set>');
    expect(JSON.stringify(logged)).not.toContain('secret-jwt-value');
  });

  test('an unset token stays unset in the debug config', () => {
    const config: Options = { token: '', instance: '' } as Options;
    expect(loggedConfig(config).token).toBe('');
  });

  test('the debug log of a client shows its config without the token', () => {
    const debugSpy: jest.SpyInstance = jest.spyOn(console, 'debug').mockImplementation((): void => {});
    try {
      const c: Centrifuge = new Centrifuge('ws://localhost:1/connection/websocket', { token: 'secret-jwt-value', debug: true });
      const logged: string = debugSpy.mock.calls.map((args: any[]): string => format(...args)).join('\n');

      expect(c.state).toBe(State.Disconnected);
      expect(debugSpy.mock.calls.some((args: any[]): boolean => args[0] === 'config')).toBe(true);
      expect(logged).not.toContain('secret-jwt-value');
    } finally {
      debugSpy.mockRestore();
    }
  });
});
