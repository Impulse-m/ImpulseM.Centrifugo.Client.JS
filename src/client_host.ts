import type { Centrifuge } from './centrifuge';
import { connectingCodes, errorCodes } from './codes';
import type { RefreshSlotHost } from './refresh_slot';
import type { CallResolveContext, RpcCallbacks, RpcHost } from './rpc_tracker';
import type { ConnectionIdentity } from './identity';
import { State } from './types';
import type { WriteHost } from './write_commands';
import { ttlMilliseconds } from './utils';

/** The host of the rpc tracker, the refresh slot and the write of commands: see createClientHost. */
export interface ClientHost extends RpcHost, RefreshSlotHost, WriteHost {
  /** Arms the refresh timer in place of the one armed before: refresh runs after the delay in milliseconds. */
  scheduleRefresh(delay: number, refresh: () => void): void;
  /** Called after a refresh reply is applied, so the listener sees the new token and identity. Null without a listener. */
  onRefreshAccepted: (() => void) | null;
  /** Called when a refresh fails while the client is connected, see RefreshSlotHost.refreshFailed. Null without a listener. */
  onRefreshFailed: ((error: any, explicit: boolean) => void) | null;
}

/**
 * The host of the token source of an RPC pool (see token_source.ts): the token, identity and state of a client, and the
 * one renewal it may start. Reached through createTokenSourceHost, which no client creates for itself.
 */
export interface TokenSourceHost {
  /** The connection token the client holds. */
  token(): string;
  /** The user and auth class the server confirmed for the connection. */
  identity(): ConnectionIdentity;
  /** The state of the client. */
  state(): State;
  /** Whether a token is the access token of a reply whose ttl has run out. */
  tokenExpired(token: string): boolean;
  /** Whether the client can renew its token: it has getToken or tokenExchange. */
  canRenew(): boolean;
  /** Starts one renewal of the token now, through the client's own refresh. False when a refresh is in flight already. */
  renewNow(): boolean;
  /** Sets the listener of the accepted refresh replies, or removes it with null. */
  setRefreshAccepted(listener: (() => void) | null): void;
  /** Sets the listener of the refresh failures, or removes it with null. */
  setRefreshFailed(listener: ((error: any, explicit: boolean) => void) | null): void;
  /** Sets the listener of the connect replies that confirm the identity, or removes it with null. */
  setConnectConfirmed(listener: (() => void) | null): void;
}

/**
 * The members the token exchange flow adds to the host, created by createTokenExchangeHost only when the tokenExchange
 * option is set: a client without the option holds none of them.
 */
export interface TokenExchangeHost extends ClientHost {
  /** Whether the server asked for a fresh connection token (a 109 at the last connect). */
  refreshRequired(): boolean;
  /** Resets the request for a fresh connection token, once the connect goes out without one. */
  clearRefreshRequired(): void;
  /** The id of the transport the client currently uses. */
  transportId(): number;
  /** Whether the client is in the connecting state. */
  isConnecting(): boolean;
  /** Writes a plain command on the current transport, without waiting for the connected state. */
  sendPlainCommand(command: any): Promise<CallResolveContext>;
  /** The client timeout, in milliseconds. */
  timeout(): number;
  /** The session and node of the connection, as the commands of the connection are written with them. */
  route(): { session: string; node: string };
  /** Reports an error to the application, then runs the continuation (see the client's emitError). */
  emitError(ctx: any, next: () => void): void;
  /** Ends the connection with a code and a reason, reconnecting or not (see the client's disconnect). */
  disconnect(code: number, reason: string, reconnect: boolean): void;
  /** Ends the session as unauthorized, with no reconnect. */
  failUnauthorized(): void;
  /** The generation of the connection token: moves on with every refresh(token) call. */
  tokenGeneration(): number;
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
 * _disconnect, _tokenExchange, _refreshRequired, _transportId, _isConnecting, _session, _node, _emitError,
 * _failUnauthorized, _host (createTokenSourceHost), and the in-flight fields callInFlight and
 * commandInFlight of _refreshSlot (createTokenSourceHost).
 * Accepted by the wave's contract (section 10); the bracket reads are not counted as uses by noUnusedLocals.
 */
export function createClientHost(client: Centrifuge): ClientHost {
  const scheduleRefresh = (delay: number, refresh: () => void): void => {
    // The timer field may hold a timer that fired already, or one a newer token superseded: it is replaced. The timer
    // refresh it arms is marked as running when it fires, see RefreshSlot.beginTimer.
    client['_clearRefreshTimeout']();
    client['_refreshSlot'].timerArmed();
    client['_refreshTimeout'] = setTimeout((): void => {
      client['_refreshSlot'].beginTimer(client['_client']);
      refresh();
    }, delay);
  };
  const host: ClientHost = {
    onRefreshAccepted: null,
    onRefreshFailed: null,
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
    armRefreshRetry: (): void => {
      scheduleRefresh(client['_getRefreshRetryDelay'](), (): void => client['_refresh']());
    },
    applyReply: (result: any): void => {
      client['_tokenExchange']?.tokenAccepted();
      client['_clearRefreshTimeout']();
      if (result.expires) {
        // The client id of the connection only changes if the reply carries one — and
        // Centrifugo never sets that field of a refresh reply. Taking an absent one
        // leaves the client without an id, and the checks comparing against it stop
        // recognising a token fetch, or a reply, of a connection that is gone.
        if (result.client) {
          client['_client'] = result.client;
        }
        scheduleRefresh(client['_tokenExchange']?.expiryDelay(result.ttl) ?? ttlMilliseconds(result.ttl), (): void => client['_refresh']());
      }
      client['_identity'].applyRefresh(result);
      host.onRefreshAccepted?.();
    },
    identity: (): ConnectionIdentity => client['_identity'],
    dispatchFailed: (err: any): void => client['_dispatchFailed'](err),
    refreshFailed: (error: any, explicit: boolean): void => {
      host.onRefreshFailed?.(error, explicit);
    },
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
    tokenSuperseded: (generation: number): boolean => client['_tokenExchange'] !== null
      && client['_refreshSlot'].currentTokenGeneration() !== generation,
  };
  return host;
}


/**
 * The host of the token source of an RPC pool, over the client whose token the pool lends. Made only for the client a
 * pool names as tokenFrom, so a client never used so holds none of its members.
 */
export function createTokenSourceHost(client: Centrifuge): TokenSourceHost {
  return {
    token: (): string => client['_token'],
    identity: (): ConnectionIdentity => client['_identity'],
    state: (): State => client.state,
    tokenExpired: (token: string): boolean => client['_tokenExchange']?.tokenExpired(token) ?? false,
    canRenew: (): boolean => client['_host'].hasGetToken(),
    renewNow: (): boolean => {
      // A renewal starts only for a connected client that is not busy: a refresh in flight, a refresh command waiting for
      // its reply, or a timer refresh in progress renews the token already. The in-flight fields are read here, not
      // through a method of the refresh slot.
      if (client.state !== State.Connected) {
        return false;
      }
      if (client['_refreshSlot']['callInFlight'] !== null
        || client['_refreshSlot']['commandInFlight'] !== null
        || client['_refreshSlot'].timerBusy(client['_client'])) {
        return false;
      }
      client['_host'].scheduleRefresh(0, (): void => client['_refresh']());
      return true;
    },
    setRefreshAccepted: (listener: (() => void) | null): void => {
      client['_host'].onRefreshAccepted = listener;
    },
    setRefreshFailed: (listener: ((error: any, explicit: boolean) => void) | null): void => {
      client['_host'].onRefreshFailed = listener;
    },
    setConnectConfirmed: (listener: (() => void) | null): void => {
      client['_identity'].onConnect = listener;
    },
  };
}


/**
 * The token a client presented at its last connect attempt that was not abandoned: the token of the connect its server
 * accepted, or refused. The client stores it only when its token fetch was not abandoned, so a token answered for an
 * attempt the client dropped is not presented, and a pool connection never names it as refused.
 */
export function presentedTokenOf(client: Centrifuge): string {
  return client['_token'];
}


/**
 * The host of the token exchange flow: the host of createClientHost plus the members only the flow uses. Called only when
 * the tokenExchange option is set, so a client without the option allocates none of them.
 */
export function createTokenExchangeHost(client: Centrifuge, host: ClientHost): TokenExchangeHost {
  return {
    ...host,
    refreshRequired: (): boolean => client['_refreshRequired'],
    clearRefreshRequired: (): void => {
      client['_refreshRequired'] = false;
    },
    transportId: (): number => client['_transportId'],
    isConnecting: (): boolean => client['_isConnecting'](),
    sendPlainCommand: (command: any): Promise<CallResolveContext> => client['_call'](command, true),
    timeout: (): number => client['_config'].timeout as number,
    route: (): { session: string; node: string } => ({ session: client['_session'], node: client['_node'] }),
    emitError: (ctx: any, next: () => void): void => {
      client['_emitError'](ctx, next);
    },
    disconnect: (code: number, reason: string, reconnect: boolean): void => {
      client['_disconnect'](code, reason, reconnect);
    },
    failUnauthorized: (): void => {
      client['_failUnauthorized']();
    },
    tokenGeneration: (): number => client['_refreshSlot'].currentTokenGeneration(),
  };
}
