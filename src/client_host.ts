import type { Centrifuge } from './centrifuge';
import type { RefreshSlotHost } from './refresh_slot';
import type { CallResolveContext, RpcCallbacks, RpcHost } from './rpc_tracker';
import type { ConnectionIdentity } from './identity';
import { ttlMilliseconds } from './utils';

/**
 * The host of the rpc tracker and of the refresh slot: the state and the commands of a client, as they use
 * them. The members of the client read here are its private ones, reached by name: the client keeps them out
 * of its API, and this is the one place that uses them for the rpc and refresh machinery.
 *
 * The private members reached here (a rename of any of them in centrifuge.ts must change this file too):
 * _isConnected, _createErrorObject, _config, _refreshSlot, _callPromise, _nextCommandId, _addCommand,
 * _callbacks, _rpcOptionsSupported, _methodCall, _call, _getToken, _token, _debug, _getRefreshRetryDelay,
 * _scheduleRefresh, _clearRefreshTimeout, _client, _identity, _dispatchFailed.
 * Accepted by the wave's contract (section 10); the bracket reads are not counted as uses by noUnusedLocals.
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
    armRefreshRetry: (): void => {
      client['_scheduleRefresh'](client['_getRefreshRetryDelay']());
    },
    applyReply: (result: any): void => {
      client['_clearRefreshTimeout']();
      if (result.expires) {
        // The client id of the connection only changes if the reply carries one — and
        // Centrifugo never sets that field of a refresh reply. Taking an absent one
        // leaves the client without an id, and the checks comparing against it stop
        // recognising a token fetch, or a reply, of a connection that is gone.
        if (result.client) {
          client['_client'] = result.client;
        }
        client['_scheduleRefresh'](ttlMilliseconds(result.ttl));
      }
      client['_identity'].applyRefresh(result);
    },
    identity: (): ConnectionIdentity => client['_identity'],
    dispatchFailed: (err: any): void => client['_dispatchFailed'](err),
  };
}
