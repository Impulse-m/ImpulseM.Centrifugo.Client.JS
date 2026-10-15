import { errorCodes } from './codes';
import { CallResolveContext, RpcCallbacks, RpcHost, RpcTracker } from './rpc_tracker';
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

  // The plain requests sent, answered with 'plain' at once.
  plainCommands: any[] = [];

  plainCall(command: any): Promise<any> {
    this.plainCommands.push(command);
    return Promise.resolve({ data: 'plain' });
  }

  // Answers the command with the given id, as the server does.
  reply(id: number, data: any): void {
    const ctx: CallResolveContext = { reply: { rpc: { data: data } } };
    this.callbacks.get(id)?.callback(ctx);
  }

  // Answers the command with the given id with an error, as the server does: the reply carries next.
  replyError(id: number, error: any): void {
    this.callbacks.get(id)?.errback({ error: error, next: (): void => {} });
  }

  // The connection ends before the command gets its reply: the command has no next.
  lose(id: number): void {
    this.callbacks.get(id)?.errback({ error: undefined });
  }

  // The codec refuses the command before any write: it never reaches the connection.
  refuse(id: number): void {
    this.callbacks.get(id)?.errback({ error: { code: errorCodes.transportWriteError, message: 'refused' }, refused: true });
  }
}

// Lets the pending microtasks run, so a call has passed its wait for the connection.
function settleMicrotasks(): Promise<void> {
  return new Promise<void>((resolve: () => void): void => {
    setImmediate(resolve);
  });
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

  test('a call never written goes out as a plain request when the next connection has no RPC options', async (): Promise<void> => {
    const host: StubHost = new StubHost();
    const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
    host.connected = false;
    const call: Promise<RpcResult> = tracker.call('m', {}, undefined, Date.now() + 30000);
    await settleMicrotasks();

    host.supported = false;
    tracker.sendWaiting();

    await expect(call).resolves.toMatchObject({ data: 'plain' });
    expect(host.commands).toHaveLength(0);
    expect(host.plainCommands).toHaveLength(1);
    expect(host.plainCommands[0].rpc.msg_id).toBeUndefined();
  });

  test('a never-written call with a signal is refused as unsupported on a connection without RPC options', async (): Promise<void> => {
    const host: StubHost = new StubHost();
    const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
    host.connected = false;
    const controller: AbortController = new AbortController();
    const call: Promise<RpcResult> = tracker.call('m', {}, { signal: controller.signal }, Date.now() + 30000);
    await settleMicrotasks();

    host.supported = false;
    tracker.sendWaiting();

    await expect(call).rejects.toMatchObject({ code: errorCodes.unsupported });
    expect(host.plainCommands).toHaveLength(0);
    expect(host.commands).toHaveLength(0);
  });

  test('a call written and lost is rejected with connectionClosed on a connection without RPC options', async (): Promise<void> => {
    const host: StubHost = new StubHost();
    const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
    const call: Promise<RpcResult> = tracker.call('m', {}, undefined, Date.now() + 30000);
    await settleMicrotasks();
    host.lose(host.commands[0].id);

    host.supported = false;
    tracker.sendWaiting();

    await expect(call).rejects.toMatchObject({ code: errorCodes.connectionClosed });
    expect(host.plainCommands).toHaveLength(0);
  });

  test('a terminal disconnect rejects a never-written call with cancelled and a written one with the given code', async (): Promise<void> => {
    const host: StubHost = new StubHost();
    const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
    host.connected = false;
    const unsent: Promise<RpcResult> = tracker.call('a', {}, undefined, Date.now() + 30000);
    await settleMicrotasks();

    host.connected = true;
    const sent: Promise<RpcResult> = tracker.call('b', {}, undefined, Date.now() + 30000);
    await settleMicrotasks();
    expect(host.commands).toHaveLength(1);
    host.lose(host.commands[0].id);
    host.connected = false;

    tracker.rejectAll(errorCodes.connectionClosed, 'connection closed');

    await expect(unsent).rejects.toMatchObject({ code: errorCodes.cancelled });
    await expect(sent).rejects.toMatchObject({ code: errorCodes.connectionClosed });
  });

  test('a call whose deadline passed when its later attempt is due is surfaced at once, with no cancel', async (): Promise<void> => {
    jest.useFakeTimers({ doNotFake: ['setImmediate'] });
    try {
      const host: StubHost = new StubHost();
      const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
      const call: Promise<RpcResult> = tracker.call('m', {}, undefined, Date.now() + 250);
      await settleMicrotasks();

      // The reply arrives after the deadline, before its timer ran: the call is not run again.
      jest.setSystemTime(Date.now() + 300);
      host.replyError(host.commands[0].id, { code: 100, message: 'dedup_unavailable', temporary: true, metadata: { 'x-centrifugo-retry': 'later' } });

      await expect(call).rejects.toMatchObject({ code: 100 });
      expect(host.commands).toHaveLength(1);
      expect(host.commands.some((command: any): boolean => command.rpc.cancel !== undefined)).toBe(false);
    } finally {
      jest.useRealTimers();
    }
  });

  test('a call the codec refuses is rejected at once, not resent, and its message id is not acknowledged', async (): Promise<void> => {
    const host: StubHost = new StubHost();
    const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
    const first: Promise<RpcResult> = tracker.call('a', {}, undefined, Date.now() + 30000);
    await settleMicrotasks();
    host.reply(host.commands[0].id, 'done');
    await expect(first).resolves.toMatchObject({ data: 'done' });
    const firstMsgId: number = host.commands[0].rpc.msg_id;

    const refused: Promise<RpcResult> = tracker.call('b', {}, undefined, Date.now() + 30000);
    await settleMicrotasks();
    const refusedMsgId: number = host.commands[1].rpc.msg_id;
    // The acknowledgement of the first call rode on the refused command: it is not lost with it.
    expect(host.commands[1].rpc.ack).toEqual([firstMsgId]);
    host.refuse(host.commands[1].id);
    await expect(refused).rejects.toMatchObject({ code: errorCodes.transportWriteError });
    expect(host.commands).toHaveLength(2);

    const next: Promise<RpcResult> = tracker.call('c', {}, undefined, Date.now() + 30000);
    await settleMicrotasks();
    expect(host.commands[2].rpc.ack).toEqual([firstMsgId]);
    expect(host.commands[2].rpc.ack).not.toContain(refusedMsgId);
    host.reply(host.commands[2].id, 'next');
    await expect(next).resolves.toMatchObject({ data: 'next' });
  });

  test('a cancel the codec refuses is not kept for the next connection', async (): Promise<void> => {
    const host: StubHost = new StubHost();
    const tracker: RpcTracker = new RpcTracker(host, 32, 30000);
    const controller: AbortController = new AbortController();
    const call: Promise<RpcResult> = tracker.call('m', {}, { signal: controller.signal }, Date.now() + 30000);
    await settleMicrotasks();

    controller.abort();
    await expect(call).rejects.toMatchObject({ code: errorCodes.cancelled });
    expect(host.commands).toHaveLength(2);
    host.refuse(host.commands[1].id);

    tracker.sendWaiting();
    expect(host.commands).toHaveLength(2);
  });
});
