import WebSocket from 'ws';
import { RpcPool } from './rpc_pool';
import { RpcResult } from './types';
import { WaitableFakeServer, holdReply, rpcOptionsWireFlag, until } from './testing/codecs';

// The RPC pool with frames of a mebibyte, against the fake server: every frame arrives whole and is answered, and the
// window of a connection holds. The heap is not measured: Node has no stable per-test bound. The pool's own counts are.

const frameSize: number = 1024 * 1024;

// A payload of a frame size, distinct per seed, so that a frame mixed up with another one is seen.
function payload(seed: number): Uint8Array {
  const data: Uint8Array = new Uint8Array(frameSize);
  for (let i: number = 0; i < frameSize; i++) {
    data[i] = (i * 31 + seed) & 0xff;
  }
  return data;
}

function sameBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) {
    return false;
  }
  for (let i: number = 0; i < left.length; i++) {
    if (left[i] !== right[i]) {
      return false;
    }
  }
  return true;
}

function isCall(cmd: any): boolean {
  return cmd.rpc !== undefined && cmd.rpc.cancel === undefined;
}

describe('RpcPool with large frames', () => {
  let server: WaitableFakeServer | null = null;
  let pool: RpcPool | null = null;

  afterEach(async (): Promise<void> => {
    pool?.close();
    pool = null;
    await server?.close();
    server = null;
  });

  test('four 1 MiB calls on a window of two arrive whole, the window holds, and every call is answered', async (): Promise<void> => {
    server = await WaitableFakeServer.start('protobuf');
    server.connectResult = { client: 'fake-client', version: '0.0.0', ping: 25, flag: rpcOptionsWireFlag };
    const held: any[] = [];
    const current: WaitableFakeServer = server;
    current.onCommand = (cmd: any): any => {
      if (isCall(cmd)) {
        held.push(cmd);
        return holdReply;
      }
      return undefined;
    };
    pool = new RpcPool({
      endpoints: (): string => current.url,
      connection: { websocket: WebSocket, minReconnectDelay: 10, maxReconnectDelay: 50 },
      maxConnectionsPerEndpoint: 1,
      maxInflightPerConnection: 2,
      maxQueued: 8,
    });
    const payloads: Uint8Array[] = [1, 2, 3, 4].map((seed: number): Uint8Array => payload(seed));
    const calls: Promise<RpcResult>[] = payloads.map((data: Uint8Array): Promise<RpcResult> => {
      const call: Promise<RpcResult> = pool!.rpc('part.write', data);
      call.catch((): void => {});
      return call;
    });

    expect(await until((): boolean => held.length === 2)).toBe(true);
    await new Promise<void>((resolve: () => void): void => {
      setTimeout(resolve, 50);
    });
    expect(held.length).toBe(2);
    expect(pool.stats()).toEqual({ connections: 1, inflight: 2, queued: 2, redirects: 0 });

    for (let i: number = 0; i < 4; i++) {
      expect(held[i].rpc.data.length).toBe(frameSize);
      expect(sameBytes(held[i].rpc.data, payloads[i])).toBe(true);
      current.send({ id: held[i].id, rpc: { data: held[i].rpc.data } });
      const result: RpcResult = await calls[i];
      expect(sameBytes(result.data, payloads[i])).toBe(true);
      expect(pool.stats().inflight).toBeLessThanOrEqual(2);
      expect(await until((): boolean => held.length === Math.min(i + 3, 4))).toBe(true);
    }
    expect(pool.stats()).toEqual({ connections: 1, inflight: 0, queued: 0, redirects: 0 });
  });
});
