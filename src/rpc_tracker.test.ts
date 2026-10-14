import { RpcCallbacks, RpcHost, RpcTracker } from './rpc_tracker';
import { CallResolveContext } from './centrifuge';
import { RpcResult } from './types';

// The tracker against a host that records what it writes, without a client or a socket.

class StubHost implements RpcHost {
  connected: boolean = true;
  supported: boolean = true;
  // The commands the tracker added to the connection, in order.
  commands: any[] = [];
  callbacks: Map<number, RpcCallbacks> = new Map();
  private lastId: number = 0;

  nextCommandId(): number {
    this.lastId++;
    return this.lastId;
  }

  addCommand(command: any): void {
    this.commands.push(command);
  }

  setCallbacks(id: number, callbacks: RpcCallbacks): void {
    this.callbacks.set(id, callbacks);
  }

  deleteCallbacks(id: number): void {
    this.callbacks.delete(id);
  }

  createError(code: number, message: string): any {
    return { code: code, message: message };
  }

  serverTime(): number {
    return 1_700_000_000_000;
  }

  rpcOptionsSupported(): boolean {
    return this.supported;
  }

  isConnected(): boolean {
    return this.connected;
  }

  hasInstance(): boolean {
    return true;
  }

  ready(): Promise<void> {
    return Promise.resolve();
  }

  plainCall(): Promise<any> {
    return Promise.reject(new Error('not used'));
  }

  // Answers the command with the given id, as the server does.
  reply(id: number, data: any): void {
    const ctx: CallResolveContext = { reply: { rpc: { data: data } } };
    this.callbacks.get(id)?.callback(ctx);
  }
}

describe('RpcTracker', () => {
  test('a call whose connection is lost before it is written waits for the next connection, not a retransmit', async () => {
    const host: StubHost = new StubHost();
    const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
    // The connection is lost in the microtasks between the ready wait and the tracking of the call.
    host.ready = (): Promise<void> => {
      queueMicrotask((): void => {
        host.connected = false;
      });
      return Promise.resolve();
    };

    const call: Promise<RpcResult> = tracker.call('m', {}, undefined, Date.now() + 30000);
    // Lets the pending microtasks run: the call has passed the ready wait by then.
    await new Promise<void>((resolve: () => void): void => {
      setImmediate(resolve);
    });
    expect(host.commands).toHaveLength(0);

    host.connected = true;
    tracker.sendWaiting();
    expect(host.commands).toHaveLength(1);
    expect(host.commands[0].rpc.retransmit).toBeUndefined();
    expect(host.commands[0].rpc.msg_id).toBeDefined();

    host.reply(host.commands[0].id, 'done');
    await expect(call).resolves.toMatchObject({ data: 'done' });
  });
});
