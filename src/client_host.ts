import type { CallResolveContext, Centrifuge } from './centrifuge';
import type { RefreshSlotHost } from './refresh_slot';
import type { RpcCallbacks, RpcHost } from './rpc_tracker';
import type { ConnectionIdentity } from './identity';

/**
 * The host of the rpc tracker and of the refresh slot: the state and the commands of a client, as they use
 * them. The members of the client read here are its private ones, reached by name: the client keeps them out
 * of its API, and this is the one place that uses them for the rpc and refresh machinery.
 */
export function createClientHost(client: Centrifuge): RpcHost & RefreshSlotHost {
  return {
    isConnected: (): boolean => client['_isConnected'](),
    createError: (code: number, message: string): any => client['_createErrorObject'](code, message),
    hasInstance: (): boolean => !!client['_config'].instance,
    ready: (deadline: number): Promise<void> => client['_refreshSlot'].ready(deadline),
    plainCall: (command: any, deadline: number): Promise<any> => client['_callPromise'](command, (reply: any): any => reply.rpc, deadline),
    nextCommandId: (): number => client['_nextCommandId'](),
    addCommand: (command: any): void => client['_addCommand'](command),
    setCallbacks: (id: number, callbacks: RpcCallbacks): void => {
      client['_callbacks'][id] = callbacks;
    },
    deleteCallbacks: (id: number): void => {
      delete client['_callbacks'][id];
    },
    serverTime: (): number => client.serverTime(),
    rpcOptionsSupported: (): boolean => client['_rpcOptionsSupported'],
    waitConnected: (timeout: number | null): Promise<void> => client['_methodCall'](timeout),
    sendCommand: (command: any): Promise<CallResolveContext> => client['_call'](command, false),
    hasGetToken: (): boolean => !!client['_config'].getToken,
    getToken: (): Promise<string> => client['_getToken'](),
    token: (): string => client['_token'],
    setToken: (token: string): void => {
      client['_token'] = token;
    },
    debug: (message: string): void => client['_debug'](message),
    applyReply: (result: any): void => client['_refreshResponse'](result),
    identity: (): ConnectionIdentity => client['_identity'],
    dispatchFailed: (err: any): void => client['_dispatchFailed'](err),
  };
}
