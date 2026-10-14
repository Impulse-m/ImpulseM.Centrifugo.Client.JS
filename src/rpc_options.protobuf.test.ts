import { Centrifuge } from './protobuf';
import { ProtobufCodec } from './protobuf.codec';
import { errorCodes } from './codes';
import { State, TransportName } from './types';
import { WebSocketServer } from 'ws';
import WebSocket from 'ws';

// RPC options, the rpc capability, server error fields and server time over the
// protobuf codec. The fake server here speaks binary protobuf: it decodes the commands
// with ProtobufCodec and encodes its replies with it, so the int64 and map fields
// take the same path as in a real protobuf connection.

// The connect flag of bit 32, as the wire carries it.
const rpcOptionsWireFlag: number = 4294967296;

// RPC data is raw bytes on the protobuf wire: JSON-encoded here for readability.
const bytes = (value: any): Uint8Array => new TextEncoder().encode(JSON.stringify(value));
const text = (value: Uint8Array): string => new TextDecoder().decode(value);

class BinaryFakeServer {
  readonly received: any[] = [];
  connectResult: any = { client: 'fake-client', version: '0.0.0', ping: 25 };
  // Returns the reply to send for a command, or undefined for the default handling.
  onCommand: ((cmd: any) => any | undefined) | null = null;
  private wss: WebSocketServer;
  private codec: ProtobufCodec = new ProtobufCodec();

  private constructor(wss: WebSocketServer) {
    this.wss = wss;
    this.wss.on('connection', (ws: WebSocket) => {
      ws.on('message', (data: Buffer) => {
        const replies: any[] = [];
        for (const cmd of this.codec.decodeCommands(new Uint8Array(data))) {
          this.received.push(cmd);
          replies.push(this.replyFor(cmd));
        }
        ws.send(this.codec.encodeReplies(replies));
      });
    });
  }

  static async start(): Promise<BinaryFakeServer> {
    const wss = new WebSocketServer({ port: 0 });
    await new Promise<void>(resolve => wss.on('listening', () => resolve()));
    return new BinaryFakeServer(wss);
  }

  get url(): string {
    const addr = this.wss.address();
    const port = typeof addr === 'string' ? 0 : addr.port;
    return `ws://localhost:${port}/connection/websocket`;
  }

  close(): Promise<void> {
    return new Promise<void>(resolve => this.wss.close(() => resolve()));
  }

  private replyFor(cmd: any): any {
    const custom = this.onCommand ? this.onCommand(cmd) : undefined;
    if (custom !== undefined) {
      return custom;
    }
    if (cmd.connect !== undefined) {
      return { id: cmd.id, connect: this.connectResult };
    }
    return { id: cmd.id };
  }
}

describe('RPC options over protobuf', () => {
  let server: BinaryFakeServer;
  const clients: Centrifuge[] = [];

  beforeEach(async () => {
    server = await BinaryFakeServer.start();
  });

  afterEach(async () => {
    for (const c of clients) {
      c.disconnect();
    }
    clients.length = 0;
    await server.close();
  });

  async function connect(): Promise<Centrifuge> {
    const c = new Centrifuge([{
      transport: 'websocket' as TransportName,
      endpoint: server.url,
    }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
    });
    clients.push(c);
    const connected = new Promise<void>(resolve => c.once('connected', () => resolve()));
    c.connect();
    await connected;
    expect(c.state).toBe(State.Connected);
    return c;
  }

  function rpcCommands(): any[] {
    return server.received.filter(cmd => cmd.rpc !== null && cmd.rpc !== undefined);
  }

  test('advertises bit 32 in the connect request', async () => {
    await connect();
    const connectCmd = server.received.find(cmd => cmd.connect !== null && cmd.connect !== undefined);
    expect(Number(connectCmd.connect.flag)).toBe(rpcOptionsWireFlag);
  });

  test('capability is on when the connect reply echoes bit 32 as a Long', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
    const c = await connect();
    expect(c.capabilities.rpcOptions).toBe(true);
  });

  test('capability is off when the connect reply has no flag', async () => {
    const c = await connect();
    expect(c.capabilities.rpcOptions).toBe(false);
  });

  test('headers are sent with the rpc when the server supports them', async () => {
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: bytes({ ok: true }) } } : undefined);
    const c = await connect();

    const result = await c.rpc('echo', bytes({ value: 1 }), { headers: { 'example-header': 'example-value' } });

    expect(JSON.parse(text(result.data))).toEqual({ ok: true });
    expect(rpcCommands()[0].rpc.headers).toEqual({ 'example-header': 'example-value' });
  });

  test('headers are rejected locally without sending the call in legacy mode', async () => {
    const c = await connect();

    await expect(c.rpc('echo', bytes({}), { headers: { 'example-header': 'example-value' } })).rejects.toMatchObject({
      code: errorCodes.unsupported,
    });
    expect(rpcCommands()).toHaveLength(0);
  });

  test('the two-argument rpc sends no headers', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, rpc: { data: bytes('plain') } } : undefined);
    const c = await connect();

    const result = await c.rpc('echo', bytes({ value: 1 }));

    expect(text(result.data)).toBe('"plain"');
    expect(rpcCommands()[0].rpc.headers).toEqual({});
  });

  test('app error fields are surfaced on a failed rpc', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? {
      id: cmd.id,
      error: {
        code: 100,
        message: 'application failure',
        app_code: 1234,
        app_type: 'EXAMPLE_RETRY_AFTER',
        metadata: { 'example-key': 'example-value' },
      },
    } : undefined);
    const c = await connect();

    await expect(c.rpc('echo', bytes({}))).rejects.toMatchObject({
      code: 100,
      appCode: 1234,
      appType: 'EXAMPLE_RETRY_AFTER',
      metadata: { 'example-key': 'example-value' },
    });
  });

  test('app error fields stay absent when the protobuf error has the zero values', async () => {
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, error: { code: 100, message: 'plain failure' } } : undefined);
    const c = await connect();

    let caught: any;
    try {
      await c.rpc('echo', bytes({}));
    } catch (e) {
      caught = e;
    }

    expect(caught.code).toBe(100);
    expect(caught).not.toHaveProperty('appCode');
    expect(caught).not.toHaveProperty('appType');
    expect(caught).not.toHaveProperty('metadata');
  });

  test('serverTime follows the server time of a reply carried as a Long', async () => {
    const skewMs = 100000;
    server.onCommand = (cmd: any) => (cmd.rpc ? { id: cmd.id, time: Date.now() + skewMs, rpc: { data: bytes(null) } } : undefined);
    const c = await connect();
    await c.rpc('echo', bytes({}));

    expect(Math.abs(c.serverTime() - (Date.now() + skewMs))).toBeLessThan(1000);
  });

  test('old-shape replies without the new fields still work', async () => {
    server.onCommand = (cmd: any) => {
      if (cmd.rpc === undefined || cmd.rpc === null) {
        return undefined;
      }
      return cmd.rpc.method === 'fail'
        ? { id: cmd.id, error: { code: 100, message: 'legacy failure' } }
        : { id: cmd.id, rpc: { data: bytes(7) } };
    };
    const c = await connect();

    const result = await c.rpc('echo', bytes({}));
    expect(text(result.data)).toBe('7');
    expect(c.capabilities.rpcOptions).toBe(false);
    expect(Math.abs(c.serverTime() - Date.now())).toBeLessThan(1000);

    await expect(c.rpc('fail', bytes({}))).rejects.toMatchObject({ code: 100, message: 'legacy failure' });
  });
});
