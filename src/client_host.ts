import type { Centrifuge } from './centrifuge';
import { connectingCodes, errorCodes } from './codes';
import type { RefreshSlotHost } from './refresh_slot';
import type { CallResolveContext, RpcCallbacks, RpcHost } from './rpc_tracker';
import type { ConnectionIdentity } from './identity';
import type { WriteHost } from './write_commands';
import { ttlMilliseconds } from './utils';

/** The host of the rpc tracker, the refresh slot and the write of commands: see createClientHost. */
export interface ClientHost extends RpcHost, RefreshSlotHost, WriteHost {
  /** Arms the refresh timer in place of the one armed before: refresh runs after the delay in milliseconds. */
  scheduleRefresh(delay: number, refresh: () => void): void;
  /** Arms the refresh timer for the expiry of the connection token, from the ttl of the reply that set it. */
  scheduleExpiry(ttl: number, refresh: () => void): void;
}

/**
 * The host of the rpc tracker, of the refresh slot and of the write of commands: the state and the commands of a
 * client, as they use them. The members of the client read here are its private ones, reached by name: the client
 * keeps them out of its API, and this is the one place that uses them for the rpc, refresh and write machinery.
 *
 * The private members reached here (a rename of any of them in centrifuge.ts must change this file too):
 * _isConnected, _createErrorObject, _config, _refreshSlot, _callPromise, _nextCommandId, _addCommand,
 * _callbacks, _rpcOptionsSupported, _methodCall, _call, _getToken, _token, _debug, _getRefreshRetryDelay,
 * _clearRefreshTimeout, _refreshTimeout, _refresh, _client, _identity, _dispatchFailed, _rpc, _codec, _transport,
 * _disconnect, _tokenExchange.
 * Accepted by the wave's contract (section 10); the bracket reads are not counted as uses by noUnusedLocals.
 */
export function createClientHost(client: Centrifuge): ClientHost {
  const scheduleRefresh = (delay: number, refresh: () => void): void => {
    // The timer field may hold a timer that fired already, or one a newer token superseded: it is replaced.
    client['_clearRefreshTimeout']();
    client['_refreshTimeout'] = setTimeout(refresh, delay);
  };
  const scheduleExpiry = (ttl: number, refresh: () => void): void => {
    scheduleRefresh(client['_tokenExchange']?.expiryDelay(ttl) ?? ttlMilliseconds(ttl), refresh);
  };
  return {
    isConnected: (): boolean => client['_isConnected'](),
    createError: (code: number, message: string): any => client['_createErrorObject'](code, message),
    hasInstance: (): boolean => !!client['_config'].instance,
    ready: (deadline: number, signal?: AbortSignal): Promise<void> => client['_refreshSlot'].ready(deadline, signal),
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
    waitConnected: (timeout: number | null, signal?: AbortSignal): Promise<void> => client['_methodCall'](timeout, signal),
    sendCommand: (command: any): Promise<CallResolveContext> => client['_call'](command, false),
    hasGetToken: (): boolean => !!client['_config'].getToken || client['_tokenExchange'] !== null,
    getToken: (): Promise<string> => client['_getToken'](),
    token: (): string => client['_token'],
    setToken: (token: string): void => {
      client['_token'] = token;
    },
    debug: (message: string, ...args: any[]): void => client['_debug'](message, ...args),
    scheduleRefresh: scheduleRefresh,
    scheduleExpiry: scheduleExpiry,
    armRefreshRetry: (): void => {
      scheduleRefresh(client['_getRefreshRetryDelay'](), (): void => client['_refresh']());
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
        scheduleExpiry(result.ttl, (): void => client['_refresh']());
      }
      client['_identity'].applyRefresh(result);
    },
    identity: (): ConnectionIdentity => client['_identity'],
    dispatchFailed: (err: any): void => client['_dispatchFailed'](err),
    hasTransport: (): boolean => !!client['_transport'],
    encode: (commands: any[]): any => client['_codec'].encodeCommands(commands),
    send: (data: any, session: string, node: string): void => {
      client['_transport'].send(data, session, node);
    },
    handleWriteError: (commands: any[], refused: boolean): void => {
      for (const command of commands) {
        const id: number = command.id;
        if (!(id in client['_callbacks'])) {
          continue;
        }
        const callbacks: RpcCallbacks = client['_callbacks'][id];
        callbacks.timeout?.clear();
        delete client['_callbacks'][id];
        callbacks.errback({
          error: client['_createErrorObject'](errorCodes.transportWriteError, 'transport write error'),
          refused: refused,
          unwritten: true,
        });
      }
    },
    writeFailed: (commands: any[]): boolean => client['_rpc'].writeFailed(commands),
    disconnectWriteFailed: (): void => {
      client['_disconnect'](connectingCodes.transportClosed, 'transport write error', true);
    },
  };
}
