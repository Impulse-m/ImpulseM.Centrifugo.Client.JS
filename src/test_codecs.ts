import { Centrifuge } from './centrifuge';
import { Centrifuge as ProtobufCentrifuge } from './protobuf';
import { FakeCentrifugoServer, FakeCodec } from './fakeServer';
import { Options, TransportName } from './types';
import WebSocket, { WebSocketServer } from 'ws';

// The codec cases and the connect and cleanup helpers the suites over both codecs share.

// The codec cases the tests over both codecs share. The fake server speaks JSON or protobuf, and
// the client is the matching Centrifuge class. RPC data is raw bytes on the protobuf wire, JSON
// on the JSON wire.

/** The connect flag of bit 32 (RPC options), as the wire carries it. */
export const rpcOptionsWireFlag: number = 4294967296;

/**
 * Returned by a command handler to send no reply for the command yet: the test sends it later with send().
 */
export const holdReply: unique symbol = Symbol('holdReply');

/**
 * A fake server whose tests wait for the commands of the client: waitFor and waitForCount see every command
 * received, so a test waits for an event instead of polling. A command handler returning holdReply sends no reply.
 */
export class WaitableFakeServer extends FakeCentrifugoServer {
  private waiters: Array<() => boolean> = [];
  private closed: boolean = false;

  static async start(codec: FakeCodec = 'json'): Promise<WaitableFakeServer> {
    const wss: WebSocketServer = new WebSocketServer({ port: 0 });
    await new Promise<void>((resolve: () => void): void => {
      wss.on('listening', (): void => resolve());
    });
    return new WaitableFakeServer(wss, codec);
  }

  /** Resolves with the first command matching the predicate: one received already, or the next one received. */
  waitFor(predicate: (cmd: any) => boolean): Promise<any> {
    return new Promise<any>((resolve: (cmd: any) => void): void => {
      this.addWaiter((): boolean => {
        const found: any = this.received.find(predicate);
        if (found === undefined) {
          return false;
        }
        resolve(found);
        return true;
      });
    });
  }

  /** Resolves once at least count commands matching the predicate have been received. */
  waitForCount(predicate: (cmd: any) => boolean, count: number): Promise<void> {
    return new Promise<void>((resolve: () => void): void => {
      this.addWaiter((): boolean => {
        if (this.received.filter(predicate).length < count) {
          return false;
        }
        resolve();
        return true;
      });
    });
  }

  /** Stops the server. Calling it again does nothing. */
  close(): Promise<void> {
    if (this.closed) {
      return Promise.resolve();
    }
    this.closed = true;
    return super.close();
  }

  send(reply: any): void {
    if (reply === holdReply) {
      return;
    }
    super.send(reply);
  }

  protected dispatch(cmd: any): void {
    super.dispatch(cmd);
    this.waiters = this.waiters.filter((check: () => boolean): boolean => !check());
  }

  // Runs the check now and on every command after: it stays until it reports done.
  private addWaiter(check: () => boolean): void {
    if (!check()) {
      this.waiters.push(check);
    }
  }
}

/** A codec the tests run over: the fake server codec, a client factory and the RPC data mapping. */
export interface CodecCase {
  name: string;
  serverCodec: FakeCodec;
  createClient: (url: string, options?: Partial<Options>) => Centrifuge;
  encode: (value: any) => any;
  decode: (data: any) => any;
}

/** Encodes a value as the bytes of an RPC payload on the protobuf wire. */
export const bytes = (value: any): Uint8Array => new TextEncoder().encode(JSON.stringify(value));

/** Decodes the bytes of an RPC payload from the protobuf wire. */
export const fromBytes = (data: Uint8Array): any => JSON.parse(new TextDecoder().decode(data));

/** Connects the client and resolves once its connected event is emitted. */
export async function connectClient(c: Centrifuge): Promise<void> {
  const connected: Promise<void> = new Promise<void>((resolve: () => void): void => {
    c.once('connected', (): void => resolve());
  });
  c.connect();
  await connected;
}

/**
 * Lets the socket deliver what it has, until the condition holds. Timers are not advanced here: a test
 * that runs on a fake clock moves it itself.
 */
export async function until(condition: () => boolean): Promise<void> {
  for (let i: number = 0; i < 10000 && !condition(); i++) {
    await new Promise<void>((resolve: () => void): void => {
      setImmediate(resolve);
    });
  }
  expect(condition()).toBe(true);
}

/** Disconnects the clients and empties the list. */
export function disconnectAll(clients: Centrifuge[]): void {
  for (const c of clients) {
    c.disconnect();
  }
  clients.length = 0;
}

export const codecCases: CodecCase[] = [
  {
    name: 'JSON',
    serverCodec: 'json',
    createClient: (url: string, options?: Partial<Options>): Centrifuge => new Centrifuge([{
      transport: 'websocket' as TransportName,
      endpoint: url,
    }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      ...options,
    }),
    encode: (value: any): any => value,
    decode: (data: any): any => data,
  },
  {
    name: 'protobuf',
    serverCodec: 'protobuf',
    createClient: (url: string, options?: Partial<Options>): Centrifuge => new ProtobufCentrifuge([{
      transport: 'websocket' as TransportName,
      endpoint: url,
    }], {
      websocket: WebSocket,
      minReconnectDelay: 10,
      maxReconnectDelay: 50,
      ...options,
    }),
    encode: bytes,
    decode: fromBytes,
  },
];
