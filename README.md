## Fork of centrifuge-js

This is a fork of [centrifuge-js](https://github.com/centrifugal/centrifuge-js), published to npm as [`@impulsem/centrifuge`](https://www.npmjs.com/package/@impulsem/centrifuge). Its API matches upstream, so the API documentation below applies unless noted otherwise. Install and import instructions in this README refer to `@impulsem/centrifuge`, not to the upstream `centrifuge` package.

The package has its own versioning, starting at `1.0.0`, independent of upstream `centrifuge-js` versions. Releases are listed on the GitHub releases page of the repository the package is published from.

This SDK provides a client to connect to [Centrifugo](https://github.com/centrifugal/centrifugo) or any [Centrifuge-based](https://github.com/centrifugal/centrifuge) server using pure WebSocket or one of the alternative transports (HTTP-streaming, SSE/EventSource, experimental WebTransport) from web browser, ReactNative, or NodeJS environments.

> [!IMPORTANT]  
> This library behaves according to a common [Centrifugo SDK spec](https://centrifugal.dev/docs/transports/client_api). It's recommended to read that before starting to work with this SDK as the spec covers common SDK behavior - describes client and subscription state transitions, main options and methods. Then proceed with this readme for more specifics about `centrifuge-js`.

The features implemented by this SDK can be found in [SDK feature matrix](https://centrifugal.dev/docs/transports/client_sdk#sdk-feature-matrix).

> `@impulsem/centrifuge` is based on `centrifuge-js` v5.x and is compatible with [Centrifugo](https://github.com/centrifugal/centrifugo) server v6, v5 and v4, and [Centrifuge](https://github.com/centrifugal/centrifuge) >= 0.25.0. For Centrifugo v2, Centrifugo v3 and Centrifuge < 0.25.0 you should use `centrifuge-js` v2.x.

* [Install](#install)
* [Quick start](#quick-start)
* [Supported real-time transports](#supported-real-time-transports)
    * [WebSocket transport](#websocket-transport)
    * [HTTP-based WebSocket fallbacks](#http-based-websocket-fallbacks)
    * [Using SockJS](#using-sockjs)
    * [WebTransport (experimental)](#webtransport-experimental)
* [Client API](#client-api)
    * [Client methods and events](#client-methods-and-events)
    * [Client options](#client-options)
    * [Connection token](#connection-token)
* [Subscription API](#subscription-api)
    * [Subscription methods and events](#subscription-methods-and-events)
    * [Subscription token](#subscription-token)
    * [Subscription options](#subscription-options)
* [Subscription management API](#subscription-management-api)
* [Stream subscriptions with `getState`](#stream-subscriptions-with-getstate)
* [Map subscriptions (experimental)](#map-subscriptions-experimental)
* [Shared poll subscriptions (experimental)](#shared-poll-subscriptions-experimental)
* [Message batching](#message-batching)
* [Server-side subscriptions](#server-side-subscriptions)
* [Protobuf support](#protobuf-support)
* [Using with NodeJS](#using-with-nodejs)
* [Custom WebSocket constructor](#custom-websocket-constructor)
* [Using with React Native on Android](#using-with-react-native-on-android)
* [Errors in callbacks](#errors-in-callbacks)
* [Run tests locally](#run-tests-locally)

## Install

SDK can be installed via `npm`:

```bash
npm install @impulsem/centrifuge
```

And then in your project:

```javascript
import { Centrifuge } from '@impulsem/centrifuge';
```

In browser, you can import SDK from [unpkg](https://app.unpkg.com/@impulsem/centrifuge@latest) CDN (replace `1.0.0` with a concrete version number you want to use, see [releases](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.JS/releases)):

```html
<script src="https://unpkg.com/@impulsem/centrifuge@1.0.0/dist/centrifuge.js"></script>
```

The same files are available from jsDelivr: `https://cdn.jsdelivr.net/npm/@impulsem/centrifuge@1.0.0/dist/centrifuge.js`, see [@impulsem/centrifuge on jsdelivr](https://www.jsdelivr.com/package/npm/@impulsem/centrifuge). Note that `@impulsem/centrifuge` browser builds target [ES6](https://caniuse.com/es6).

**By default, library works with JSON only**, if you want to send binary payloads go to [Protobuf support](#protobuf-support) section to see how to import client with Protobuf support.

## Quick start

The basic usage example may look like this:

```javascript
// Use WebSocket transport endpoint.
const centrifuge = new Centrifuge('ws://centrifuge.example.com/connection/websocket');

// Allocate Subscription to a channel.
const sub = centrifuge.newSubscription('news');

// React on `news` channel real-time publications.
sub.on('publication', function(ctx) {
    console.log(ctx.data);
});

// Trigger subscribe process.
sub.subscribe();

// Trigger actual connection establishement.
centrifuge.connect();
```

Note, that we explicitly call `.connect()` method to initiate connection establishement with a server and `.subscribe()` method to move Subscription to `subsribing` state (which should transform into `subscribed` state soon after connection with a server is established). The order of `.connect()` and `.subscribe` calls does not actually matter here.

**`Centrifuge` object and `Subscription` object are both instances of [EventEmitter](https://nodejs.org/api/events.html#events_class_eventemitter).** Below we will describe events that can be exposed in detail.

## Supported real-time transports

This SDK supports several real-time transports.

### Websocket transport

WebSocket is the main protocol used by `centrifuge-js` to communicate with a server.

In a browser environment WebSocket is available globally, but if you want to connect from NodeJS env – then you need to provide WebSocket constructor to `centrifuge-js` explicitly. [See below](#using-with-nodejs) more information about this.

It's the only transport for which you can just use a string endpoint as first argument of `Centrifuge` constructor. If you need to use other transports, or several transports – then you should use `Array<TransportEndpoint>`.

### HTTP-based WebSocket fallbacks

In the quick start example above we used WebSocket endpoint to configure Centrifuge. WebSocket is the main transport – it's bidirectional out of the box.

In some cases though, WebSocket connection may not be established (for example, due to corporate firewalls and proxies). For such situations `centrifuge-js` offers several WebSocket fallback options based on HTTP:

* HTTP-streaming based on [ReadableStream API](https://developer.mozilla.org/en-US/docs/Web/API/ReadableStream)
* [SSE (EventSource)](https://developer.mozilla.org/en-US/docs/Web/API/EventSource).

These two transports use Centrifugo/Centrifuge own bidirectional emulation layer. See [more details in introduction post](https://centrifugal.dev/blog/2022/07/19/centrifugo-v4-released#modern-websocket-emulation-in-javascript). Bidirectional emulation must be first enabled on a server-side. See [Centrifugo docs](https://centrifugal.dev/docs/transports/overview) to find out how.

After enabling HTTP-streaming or SSE endpoints on a server side you can slightly change client initialization and point Javascript SDK to a list of endpoints and transports you want to use:

```javascript
const transports = [
    {
        transport: 'websocket',
        endpoint: 'ws://example.com/connection/websocket'
    },
    {
        transport: 'http_stream',
        endpoint: 'http://example.com/connection/http_stream'
    },
    {
        transport: 'sse',
        endpoint: 'http://example.com/connection/sse'
    }
];
const centrifuge = new Centrifuge(transports);
centrifuge.connect()
```

In this case, client will try transports in order, one by one, during the initial handshake. Until success. Then will only use a successfully chosen transport during reconnects.

Supported transports are:

* `websocket`
* `http_stream`
* `sse`
* `sockjs` - SockJS can also be used as a fallback in Centrifugo < v6, in Centrifugo v6 SockJS was removed and will be removed in `centrifuge-js` v6 too. Also, sticky sessions must be used on the backend in distributed case with it. See more details below
* `webtransport` - this SDK also supports WebTransport in experimental form. See details below

If you want to use sticky sessions on a load balancer level as an optimimization for Centrifugal bidirectional emulation layer keep in mind that we currently use `same-origin` credentials policy for emulation requests in `http_stream` and `sse` transport cases. So cookies will only be passed in same-origin case. Please open an issue in case you need to configure more relaxed credentials. Though in most cases stickyness based on client's IP may be sufficient enough.

### Using SockJS

**SockJS usage is DEPRECATED**. Its support was removed in Centrifugo v6, and it will also be removed from this SDK in v6 release.

If you want to use SockJS you must also import SockJS client before centrifuge.js

```html
<script src="https://cdn.jsdelivr.net/npm/sockjs-client@1/dist/sockjs.min.js" type="text/javascript"></script>
<script src="https://unpkg.com/@impulsem/centrifuge@1.0.0/dist/centrifuge.js" type="text/javascript"></script>
```

Or provide it explicitly as a dependency:

```javascript
import { Centrifuge } from '@impulsem/centrifuge'
import SockJS from 'sockjs-client'

const transports = [{
    transport: "sockjs",
    endpoint: "http://localhost:8000/connection/sockjs"
}];

const centrifuge = new Centrifuge(transports, {
  sockjs: SockJS
})
```

Note, that in SockJS case endpoint starts with `http://`, not with `ws://` as we used above when connecting to a pure WebSocket endpoint.

### WebTransport (experimental)

WebTransport is experimental and is only supported by Centrifugo at the moment (i.e. it's not available in Centrifuge library for Go out of the box).

Server must be additionally configured to work with WebTransport connections – see information in [Centrifugo WebTransport docs](https://centrifugal.dev/docs/transports/webtransport).

## Client API

Let's look at top-level API of `Centrifuge` client.

### Client methods and events

#### connect method

As we already showed above, we must call `connect()` method to make an actual connection
request to Centrifugo server:

```javascript
const centrifuge = new Centrifuge('ws://centrifuge.example.com/connection/websocket');
centrifuge.connect();
```

`connect()` triggers an actual connection request to server.

#### connected event

As soon as connection is established and client successfully authenticated – `connected` event on `Centrifuge` object instance will be called.

It's possible to listen to this event by setting event listener function on `connected` event:

```javascript
centrifuge.on('connected', function(ctx) {
    // now client connected to Centrifugo and authenticated.
});
```

#### auth event

`auth` event is fired on every connect, and when the user or auth class of the connection changes after a refresh. The payload is `{ user, authClass, ttl }`: `ttl` is the number of seconds until the token expires, as the server sent it. An empty `user` and `authClass` mean an anonymous connection, or a server that does not send them.

```javascript
centrifuge.on('auth', function(ctx) {
    console.log('connection identity', ctx.user, ctx.authClass);
});
```

#### connecting event

`connecting` event fired when Centrifuge object goes to connecting state. This may be called during initial connect, or after being `connected` due to temporary connection loss.

```javascript
centrifuge.on('connecting', function(ctx) {
    // do whatever you need in case of connecting to a server
});
```

#### disconnected event

`disconnected` event fired on Centrifuge object every time client disconnects for some reason. This can be terminal disconnect due to advice from a server or disconnect initiated by client-side.

```javascript
centrifuge.on('disconnected', function(ctx) {
    // do whatever you need in case of disconnect from server
});
```

#### state event

`state` event is fired when client state changes. It provides both old and new state.

```javascript
centrifuge.on('state', function(ctx) {
    console.log('state changed from', ctx.oldState, 'to', ctx.newState);
});
```

#### disconnect method

In some cases you may need to disconnect your client from server, use `.disconnect()` method to do this:

```javascript
centrifuge.disconnect();
```

After calling this client will not try to reestablish connection periodically. You must call `.connect()` method manually again.

#### publish method

Sometimes you need to publish into channel without actually being subscribed to it. In this case you can use `publish` method:

```javascript
centrifuge.publish("channel", {"input": "hello"}).then(function(res) {
    console.log('successfully published');
}, function(err) {
    console.log('publish error', err);
});
```

#### send method

This is only valid for Centrifuge server library for Go and does not work for Centrifugo server at the moment. `send` method allows sending asynchronous message from a client to a server.

```javascript
centrifuge.send({"input": "hello"}).then(function(res) {
    console.log('successfully sent');
}, function(err) {
    console.log('send error', err);
});
```

#### rpc method

`rpc` method allows to send rpc request from client to server and wait for data response.

```javascript
centrifuge.rpc("my.method.name", {"input": "hello"}).then(function(res) {
    console.log('rpc result', res);
}, function(err) {
    console.log('rpc error', err);
});
```

#### RPC options, server capabilities and server fields

`rpc` takes an optional third argument with per-call options. Headers are sent with that one call only:

```javascript
centrifuge.rpc("my.method.name", {"input": "hello"}, {headers: {"x-example": "value"}});
```

Headers need a server that supports RPC options. The client advertises this capability in the connect request, and the server confirms it in the connect reply. Check the confirmed capability on the client:

```javascript
if (centrifuge.capabilities.rpcOptions) {
    // per-call headers can be sent
}
```

The capability is per connection and is `false` until a connect reply confirms it. When it is `false`, an `rpc` call with headers rejects locally with error code `errorCodes.unsupported` (15), and the call is not sent. An `rpc` call without options is unchanged.

Errors from the server can carry application fields. They are set on the error object only when the server sent them:

* `appCode` - numeric application error code.
* `appType` - application error type string.
* `metadata` - string map of allow-listed server metadata.

```javascript
centrifuge.rpc("my.method.name", {}).catch(function(err) {
    console.log(err.code, err.appCode, err.appType, err.metadata);
});
```

`centrifuge.serverTime()` returns the current time in milliseconds on the server clock. It is estimated from the server time in the last reply that carried one, and falls back to local time when no reply has carried a server time yet.

`rpc` resolves with `{data, cached}`. `cached` is `true` when the server answered from the record of an earlier attempt of the same call, so the handler did not run again.

##### Retransmit of calls across a lost connection

On a connection that supports RPC options and has an `instance` (see below), every `rpc` call carries a message id. When the transport is lost while a call waits for its reply, the call is kept instead of failing. After the next connect reply confirms the capability, the call is sent again with the same message id, in message id order, and the reply of the server completes the original call. Each call keeps its original deadline across attempts: the `timeout` option, default `5000` ms, counts from the moment of the call. A call whose deadline passes while the connection is down rejects with error code `errorCodes.timeout`.

The calls of a client with an `instance` are bounded by the `maxPendingRpc` option, default `32`. Each `rpc` call takes a slot when it is made, before it waits for the connection or a refresh, and gives the slot back when it settles: by its reply, its error, its timeout, its cancel, or a disconnect for good. A call beyond the bound rejects at once with error code `errorCodes.limitExceeded` (16) and is not sent. A call lost with its transport keeps its slot until it settles, so the bound also caps the retransmit burst of a reconnect. A client without an `instance` has no bound.

A write error of a tracked call on an open transport is handled as a lost transport: the client reconnects, and the call is sent again on that connection with the same message id.

A command the codec cannot encode, for example an rpc method that is not a string, is refused by its write: its call rejects with `errorCodes.transportWriteError` (10), the connection stays, and the other calls written with it are sent.

A server error reply that carries the metadata key `x-centrifugo-retry` says that the server did not run the call, and the client sends the call again. The value `new_msg_id` means that the server refused the message id: the client re-syncs its message ids to the server time of that reply and sends the call again with a new id, at most twice. The value `later` means that the message id is still good: the call is sent again with the same id after a random delay, drawn uniformly from zero up to 250 ms, then up to 500 ms, then up to 1000 ms (full jitter), at most three times. All attempts stay within the call's timeout. When the attempts are used up, or the error carries no such key, the call rejects with the error as the server sent it, unchanged.

A call that settles locally, by its deadline or its signal, keeps its callback for one call timeout, so a late reply is still acknowledged. Then the callback is dropped, and a server that never replies does not grow the client. The cancel command of such a call gets the call timeout too: a cancel the server does not answer in that time is dropped, and it is not sent again. The client keeps at most `maxPendingRpc` cancels waiting for a connection; when there are more, the oldest is dropped.

On a connection without RPC options (legacy mode), calls are sent as plain requests without a message id, are not kept on a lost connection, and reject with `errorCodes.connectionClosed` when the transport is lost, as before. A call that was never written, and waits for a connection without RPC options, goes out as a plain request then; a call with a signal or with headers, which such a connection cannot carry, rejects with `errorCodes.unsupported`. A call tracked on a connection with RPC options, and written before the connection was lost, rejects with `errorCodes.connectionClosed` when the next connection has no RPC options: it may have run on the lost connection, so it is not reported as not sent (`errorCodes.unsupported`).

Without an `instance`, a call is a plain request too: it is not kept on a lost connection and is not sent again, so a call lost on the connection may already have run. Headers are sent with it on a connection with RPC options.

##### Cancel an rpc call

Pass an `AbortSignal` in the options to cancel a call:

```javascript
const controller = new AbortController();
centrifuge.rpc("my.method.name", {}, {signal: controller.signal}).catch(function(err) {
    // err.code === errorCodes.cancelled (17) when the signal was aborted
});
controller.abort();
```

Aborting sends a cancel for the call to the server, and the call rejects locally with error code `errorCodes.cancelled` (17). A signal that is already aborted sends nothing, and the call rejects with `errorCodes.cancelled` in every connection state. A call that waits for a connection or for a refresh rejects with `errorCodes.cancelled` at once when it is aborted, and is not sent. A call that was sent, and lost before its cancel went out, is cancelled with the next connection, in case the server received the call before the connection was lost. A call with a signal made while the client is disconnected waits for the connection within its timeout. If it is aborted meanwhile, it rejects with `errorCodes.cancelled` and nothing is sent. Once connected to a server without RPC options, or without an `instance`, `rpc` with a signal rejects with `errorCodes.unsupported` before it is sent: the server has no scope to cancel the call in. A sent call whose deadline passes rejects with `errorCodes.timeout` and gets the same cancel, so the server stops its work. This is a deviation from the server's token session scope, which the client does not use: a cancel is sent only with an `instance`, even on a connection that could scope it by its token session. A sent call that is still pending when `disconnect()` ends the client rejects with `errorCodes.connectionClosed`. Its cancel is kept for the next `connect()`, which sends it, in case the server runs the call; the kept cancels are at most `maxPendingRpc`, and the acknowledgements of replies are dropped. A call that was never written rejects with `errorCodes.cancelled` at that disconnect, and a call still waiting in `ready` for a connection rejects with `errorCodes.clientDisconnected`, as `ready` does for every waiter. The server may still run that call, so a retry with a new call runs the handler again unless the handler is idempotent.

##### Instance of the client

The `instance` option identifies an installation or a browser tab to the server. It scopes the duplicate detection of retransmitted calls: whoever knows the instance can retransmit and cancel the calls of its scope, so it is a secret. The client never generates it and never stores it. The application generates it and keeps it, per installation or per browser tab. It must be a random value of at least 128 bits, 22 to 64 characters of `A-Z`, `a-z`, `0-9`, `_` and `-`. A value of another form throws a configuration error when the client is created. Do not log it or share it.

A per-tab identifier kept in `sessionStorage` is one way to do it:

```javascript
function tabInstance() {
    let instance = sessionStorage.getItem("app-instance");
    if (!instance) {
        const bytes = crypto.getRandomValues(new Uint8Array(16));
        instance = btoa(String.fromCharCode(...bytes))
            .replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
        sessionStorage.setItem("app-instance", instance);
    }
    return instance;
}

const centrifuge = new Centrifuge(endpoint, {instance: tabInstance()});
```

Without an `instance` the server has no scope for duplicate detection of these calls, so the client sends them as plain requests: they are not retransmitted, and a signal on them rejects with `errorCodes.unsupported`.

#### Client error codes

The client reports these errors itself, for calls it did not send or could not finish. A server never sends them.

| Code | `errorCodes` | Meaning |
|---|---|---|
| 15 | `unsupported` | the call was not sent: the connection cannot carry what it asks for (headers, a signal or a cancel) |
| 16 | `limitExceeded` | the call was not sent: `maxPendingRpc` or the refresh bound |
| 17 | `cancelled` | the call was cancelled by its signal |
| 18 | `redirectFailed` | a redirect of an RPC pool call could not be fetched, see [RPC pool for large payloads](#rpc-pool-for-large-payloads) |

Codes 15 to 19 are reserved for client-local errors of the fork SDKs; a server never sends them.

#### history method

Allows to get history from a server. This is a top-level analogue of `Subscription.history` method. But accepts a channel as first argument.

```javascript
centrifuge.history("channel", {since: {offset: 0, epoch: "xyz"}, limit: 10}).then(function(resp) {
    console.log(resp);
}, function(err) {
    console.log('history error', err);
});
```

#### presence method

Allows to get presence info from a server. This is a top-level analogue of `Subscription.presence` method. But accepts a channel as first argument.

```javascript
centrifuge.presence("channel").then(function(resp) {
    console.log(resp);
}, function(err) {
    console.log('presence error', err);
});
```

#### presenceStats method

Allows to get presence stats from a server. This is a top-level analogue of `Subscription.presenceStats` method. But accepts a channel as first argument.

```javascript
centrifuge.presenceStats("channel").then(function(resp) {
    console.log(resp);
}, function(err) {
    console.log('presence stats error', err);
});
```

#### ready method

Returns a Promise which will be resolved upon connection establishement (i.e. when Client goes to `connected` state).

#### setToken method

`setToken` may be useful to dynamically change the connection token. For example when you need to implement login/logout workflow. See an example in [blog post](https://centrifugal.dev/blog/2023/06/29/centrifugo-v5-released#token-behaviour-adjustments-in-sdks).

#### setData method

`setData` (since v5.5.0) allows setting connection data (some extra payload to deliver to the backend with connection request). This only affects the next connection attempt, not the current one. Note that if `getData` callback is configured, it will override this value during reconnects.

```javascript
centrifuge.setData({ 'name': 'Maria' });
```

#### setHeaders method

`setHeaders` allows setting connection [emulated headers](https://centrifugal.dev/blog/2025/01/16/centrifugo-v6-released#headers-emulation). These headers will be sent with the next connection attempt. **Requires Centrifugo v6**.

```javascript
centrifuge.setHeaders({ 'Authorization': 'XXX' });
```

#### error event

To listen asynchronous error happening internally while Centrifuge client works you can set an `error` handler:

```javascript
const centrifuge = new Centrifuge('ws://centrifuge.example.com/connection/websocket');

centrifuge.on('error', function(ctx) {
    console.log(ctx);
});
```

This can help you to log failed connection attempts, or token refresh errors, etc.

### Client options

Let's look at available configuration parameters when initializing `Centrifuge` object instance.

#### token

Set initial connection token (JWT). See [Connection Token](#connection-token) section for more details.

#### getToken

Set function for getting connection token. This may be used for initial token loading and token refresh mechanism (when initial token is going to expire). See [Connection Token](#connection-token) section for more details.

#### data

Set custom data to send to a server within every connect command.

#### getData

Set function for getting/renewing connection data. This callback is called upon reconnects to get fresh connection data. In many cases you may prefer using `setData` method of Centrifuge Client instead.

```javascript
const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket', {
    getData: async () => {
        // Return fresh data on each reconnect
        return { 'timestamp': Date.now() };
    }
});
```

#### name

Set custom client name. By default, it's set to `js`. This is useful for analytics and semantically must identify an environment from which client establishes a connection.

#### version

Version of your application - useful for analytics.

#### headers

Provide header emulation - these headers are sent with first protocol message. The backend can process those in a customized manner. In case of Centrifugo these headers are then used like real HTTP headers sent from the client. **Requires Centrifugo v6**.

```javascript
const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket', {
    headers: {
        'X-Custom-Header': 'value',
        'Authorization': 'Bearer token'
    }
});
```

#### instance

Identifier of the installation or the browser tab, sent with the connect command for the server's duplicate detection of retransmitted calls. The application supplies it and keeps it; the client never generates or stores it. See [Instance of the client](#instance-of-the-client). Default is empty (no instance).

#### maxPendingRpc

Maximum number of rpc calls of a client with an `instance` that are not settled, the calls waiting for a connection included. Beyond it `rpc` rejects at once with `errorCodes.limitExceeded`. Default is `32`. A client without an `instance` has no bound. See [Retransmit of calls across a lost connection](#retransmit-of-calls-across-a-lost-connection).

#### debug

`debug` is a boolean option which is `false` by default. When enabled lots of various debug
messages will be logged into javascript console. Mostly useful for development or
troubleshooting.

#### minReconnectDelay

When client disconnected from a server it will automatically try to reconnect using a backoff algorithm with jitter. `minReconnectDelay` option sets minimal interval value in milliseconds before first reconnect attempt. Default is `500` milliseconds.

#### maxReconnectDelay

`maxReconnectDelay` sets an upper reconnect delay value. Default is `20000` milliseconds - i.e. clients won't have delays between reconnect attempts which are larger than 20 seconds.

#### maxServerPingDelay

`maxServerPingDelay` sets the maximum delay of server pings after which connection is considered broken and client reconnects. In milliseconds. Default is `10000`.

#### timeout

Timeout for operations in milliseconds. Default is `5000`.

#### websocket

`websocket` option allows to explicitly provide custom WebSocket client to use. By default centrifuge-js will try to use global WebSocket object, so if you are in web browser – it will just use native WebSocket implementation. See notes about using `centrifuge-js` with NodeJS below.

#### fetch

Provide shim for fetch implementation. Useful when working in environments where fetch is not available globally.

#### readableStream

Provide shim for ReadableStream. Useful when working in environments where ReadableStream is not available globally.

#### eventsource

Provide shim for EventSource object. Useful when working in environments where EventSource is not available globally.

#### emulationEndpoint

Which emulation endpoint to use for bidirectional emulation transports. Default is `/emulation`.

#### sockjs

`sockjs` option allows to explicitly provide SockJS client object to Centrifuge client.

#### sockjsOptions

`sockjsOptions` allows modifying options passed to SockJS constructor. For example:

```javascript
const centrifuge = new Centrifuge(transports, {
    sockjs: SockJS,
    sockjsOptions: {
        transports: ['websocket', 'xhr-streaming'],
        timeout: 10000
    }
});
```

#### networkEventTarget

EventTarget for network online/offline events. In browser environment Centrifuge uses global window online/offline events automatically by default. This option allows providing a custom EventTarget for handling network state changes in other environments. Listeners are added when `connect()` is called and removed once the client becomes disconnected (for example, after calling `disconnect()`), so a disconnected client is not kept referenced from the event target. A custom target must implement `removeEventListener` for this – otherwise listeners stay registered.


### Connection Token

Depending on authentication scheme used by a server you may also want to provide connection token:

```javascript
const centrifuge = new Centrifuge('ws://centrifuge.example.com/connection/websocket', {
    token: '<CONNECTION_TOKEN>'
});
```

In case of Centrifugo on a server side this may be a JSON Web Token - see [authentication documentation](https://centrifugal.github.io/centrifugo/server/authentication/) for details on how to generate it on your backend side.

**Connection token must come to the frontend from application backend - i.e. must be generated on the backend side**. The way to deliver token to the application frontend is up to the developer. Usually you can pass it in template rendering context or issue a separate call to request a connection token from the backend.

If the token sets connection expiration then the client SDK will keep the token refreshed. It does this by calling a special callback function. This callback must return a new token. If a new token with updated connection expiration is returned from callback then it's sent to Centrifugo. In case of error returned by your callback SDK will retry the operation after some jittered time. You can throw a special error (`throw new Centrifuge.UnauthorizedError();`) from `getToken` function to move the client into disconnected state (for example, when there is no permission to connect anymore).

An example of possible `getToken` function implementation:

```javascript
import { Centrifuge, UnauthorizedError } from '@impulsem/centrifuge';

async function getToken() {
    if (!loggedIn) {
        return "";
    }
    const res = await fetch('/centrifuge/connection_token');
    if (!res.ok) {
        if (res.status === 403) {
            // Return special error to not proceed with token refreshes, client will be disconnected.
            throw new UnauthorizedError();
        }
        // Any other error thrown will result into token refresh re-attempts.
        throw new Error(`Unexpected status code ${res.status}`);
    }
    const data = await res.json();
    return data.token;
}

const client = new Centrifuge(
    'ws://localhost:8000/connection/websocket',
    {
        token: 'JWT-GENERATED-ON-BACKEND-SIDE',
        getToken: getToken
    }
);
```

> If initial token is not provided, but `getToken` is specified – then SDK assumes that developer wants to use token authentication. In this case SDK attempts to get a connection token before establishing an initial connection.

#### Refreshing the connection token on demand

`refresh(token?)` sends a connection token to the server now, the given one or the one `getToken` returns. Use it after the application obtained a new token, for example after a login or a logout. The promise resolves `{ user, authClass, ttl }` with the identity the server confirmed. The token is kept for the reconnects that follow once the server accepts it. When the server refuses the new token, the previous one is kept.

```javascript
const identity = await centrifuge.refresh('NEW-CONNECTION-TOKEN');
console.log('now connected as', identity.user);
```

While the client is disconnected, the refresh is sent on the next connect, however long the reconnect takes: the call timeout does not apply to the refresh itself. At most 32 refresh calls can be in progress at once; beyond that `refresh` rejects with `errorCodes.limitExceeded`. Calls with the same token, or without one, made while a refresh is pending share its promise. A refresh with another token is sent after the one in flight, and each caller gets the identity of its own token. The explicit refresh and the timer-driven refresh never have two refresh commands in flight, and the reconnects use the token of the last refresh the server accepted. `rpc` calls wait for a refresh in flight, so they go out under the identity it confirms; such a wait ends at the call's own timeout, and rejects with `errorCodes.cancelled` when the call's signal aborts. When the server refuses the token, the promise rejects with the server error, the connection keeps its identity, and the reconnects keep the previous token. A timer-driven refresh that the server refuses is handled by the code of the error. A temporary error, or a client error with a code below 100, emits an `error` event with type `refresh` and is retried with backoff, with a token from `getToken`. A refusal with 109 (token expired) is handled the same way, and the retry uses a fresh token from `getToken`. A refusal with 103 (permission denied) emits the `error` event and keeps the connection with its identity; it is not retried, since the same token would be refused again. The server ends the connection at the expiry of the token, and the reconnect uses `getToken`. Any other error disconnects the client with its code. The retries of 109 and the keeping of the connection on 103 differ from the upstream client, which disconnects on both. Whether the server accepts a change of user is decided by the server configuration.

Deploy the server before the clients that use the identity fields: a server that does not send `user` and `auth_class` in its refresh replies makes each refresh read as an anonymous connection.

A `refresh()` without a token that a `refresh(token)` call superseded resolves with the identity the connection holds, and `ttl` 0: the token it fetched was not sent.

#### Token exchange over the connection

The `tokenExchange` option lets the SDK obtain and renew the connection token over the connection itself, from a refresh token the application persists. Its callbacks, the flow, the refusal run and grace, the events and the server requirements are described in [docs/token_exchange.md](docs/token_exchange.md).

## RPC pool for large payloads

`RpcPool` carries calls with large payloads (chunks of a few hundred KiB to a few MiB) over several connections, so that one transfer does not hold up the others. It is an ordinary client on a server configured for large frames: see the server's large-payload fleet. The pool speaks protobuf only, so it is exported from the protobuf entry (`@impulsem/centrifuge/build/protobuf`) and not from the JSON entry: a pool needs the protobuf entry, and the JSON entry does not load the protobuf codec for an application that creates no pool.

```javascript
const pool = new RpcPool({
    // An endpoint key is whatever the server answered the application, e.g. a cluster or a region.
    endpoints: (endpointKey) => endpointsOf(endpointKey),
    connection: {token: "...", getToken: fetchToken, websocket: WebSocket},
    affinityHeader: "x-affinity",
    maxConnectionsPerEndpoint: 2,
    maxInflightPerConnection: 2,
    reroute: (error) => error.metadata?.["x-relocate"],
    redirect: (reply, error) => redirectOf(reply, error),
});

const result = await pool.rpc("part.write", chunk, {endpointKey: "eu-2", affinity: hint});
```

### Options

| Option | Default | Meaning |
|---|---|---|
| `endpoints(key)` | required | the endpoint or endpoints of a key; `''` is the key of a call without one. May return a promise. Resolved once per key, until `close()`; a failed resolution is resolved again by the next call. Keys are kept for the life of the pool, so use a bounded set (clusters, regions), not one key per object |
| `connection` | none | the options of each connection: `token`, `getToken`, `headers`, `name`, `version`, `timeout`, `websocket`, `minReconnectDelay`, `maxReconnectDelay`, `maxServerPingDelay`, `debug`. Any other option, `instance` in particular, throws `badConfiguration`. `tokenExchange` is refused: see [Tokens](#tokens) |
| `tokenFrom` | none | the `Centrifuge` client which owns the access token of the connections. The pool lends the client's token to each connection and renews it through that client. Exclusive with `connection.token` and `connection.getToken` |
| `affinityHeader` | none | the header that carries a call's `affinity`. Required to pass one, and it must be allow-listed for the method on the server |
| `maxConnectionsPerEndpoint` | `2` | connections opened per endpoint at most |
| `maxInflightPerConnection` | `1` | calls in flight per connection. A window above `1` needs `client.concurrency >= window + 1` on the server |
| `maxQueued` | `64` | calls waiting for a slot, over all endpoints, including those waiting for their endpoint key to resolve and those waiting for a redirect fetch slot. Beyond it `rpc()` rejects with `limitExceeded`. A resend of a call already admitted is queued beyond it, and is bounded by `retries`. A change of a connection places only the calls waiting for its endpoint key, and stops at the first one it cannot place, so the bound does not multiply the work of an event |
| `idleTimeout` | `60000` | a connection without calls closes after this many milliseconds, at most `2147483647` (the longest timer; a larger value throws `badConfiguration`); `0` keeps it open |
| `retries` | `3` | resends of a call lost with its connection, refused with the `later` signal, or moved by `reroute`, counted together |
| `reroute(error)` | none | returns the endpoint key a server error moves the call to, or `undefined` |
| `redirect(reply, error)` | none | returns `{url, headers?, maxBytes?}` when the bytes of the call are served elsewhere, or `undefined` |
| `maxRedirectsInflight` | `4` | redirect fetches in flight over the pool. A fetch which would wait beyond `maxQueued` fetches rejects with `limitExceeded` |
| `fetch` | `globalThis.fetch` | the fetch function of redirects |

`rpc(method, data, {endpointKey, affinity, headers, signal, timeout})` resolves with `{data, cached}` like `Centrifuge.rpc`. `timeout` defaults to the connection's `timeout` and covers the resends and the redirect fetch of the call. It is at most the connection's `timeout`, because a reply is bounded by that timeout: a longer call needs a larger `connection.timeout`. The redirect fetch counts in the same deadline, so a redirected body the link cannot carry in that time rejects with `timeout`: 16 MiB over a 2 Mbit/s link takes about 64 s, and a fleet which serves such bodies raises `connection.timeout` for its pool. A `timeout` that is not a positive number, or is above the connection's, rejects with `badConfiguration` before the call is sent.

### Tokens

With `tokenFrom` the pool takes its token from the client that owns it, which is the one way to give a pool a token that renews. Pass the client itself:

```javascript
const client = new Centrifuge(url, {tokenExchange: {load, store, exchange}});
const pool = new RpcPool({endpoints, tokenFrom: client, connection: {websocket: WebSocket}});
```

The pool never runs an exchange and never calls an application getter for its connections. Each connection is given the client's current token. When a connection's token expires or is refused, the pool asks the client, and the client renews its own token once for it; a renewal the client has already started is shared, so two refused connections cause one renewal. The client's own renewal timer keeps the token ahead of the connections, so in steady state the pool causes no renewal. `tokenFrom` works with every token mode of the client: `tokenExchange`, `getToken` (one call for the client instead of one per connection), and a static `token`, which is not renewed. `connection.token` and `connection.getToken` stay for applications without a client, and `connection.tokenExchange` is refused with `badConfiguration`.

The pool follows the client's session. While the client is connected, a connection waits for a token only as long as the client needs to renew it. A connection of a client which is connecting waits for the connect. When the client's session ends as unauthorized (for example, the application's `load` returns nothing at a renewal), `rpc()` rejects at once with `clientDisconnected` and the message `unauthorized`, and no connection opens until the client connects again. A call is never sent under an identity other than the one of its connection: when the client's user or auth class changes, the connections of the old identity take no new call, complete the calls in flight, and close. A call lost with a connection that ends as unauthorized, or is retired, is not sent again. An application which cancels its transfers when the user changes should do so itself: calls already in flight complete under the old identity.

A client which holds no usable token and is neither connecting nor connected (for example, a pool made before `connect()`) also makes `rpc()` reject at once with `unauthorized`, so no connection opens only to fail. A renewal which gives back the token a connection was refused fails that connection as unauthorized at once: the same token would be refused again, so waiting for the client's next timer refresh would only wait out the call's deadline. A refresh which the server refuses with 103 keeps the client's connection and is not retried by the client, so the connections waiting for it keep waiting for the client's next event, and their calls end at their deadline.

### Plain calls

The calls are plain requests: they carry no message id and no instance, so the server never retransmits them and never runs a second copy of one. A call is sent again only where the contract allows it, and its method must be idempotent:

- after its connection was lost before the reply, on a connection of its key;
- when the server refuses it with the `later` signal, after the delay of the client's retry rule (uniform in `[0, 250 ms * 2^n]` for the `n`-th resend);
- when `reroute` moves it to another key.

A call never goes out again after its timeout or its cancel, and a connection the server closes for message size (close code 1009) settles its calls with `connectionClosed` without a resend: the chunk is over the frame limit of the server, and every attempt would fail the same way. The pool then emits `error` with the disconnect, and the next call opens a new connection.

### Affinity and moves

The `affinity` is a hint the backend issues when a transfer begins. The pool sends it as the `affinityHeader` on every call of the transfer. The server hashes it to a replica of the target; it does not parse it. A header of the call with the same name, or an affinity without `affinityHeader`, rejects with `badConfiguration`.

A move is an error the server answers, with the trailers the method lists in `error_metadata`. The `reroute` hook returns the new endpoint key, and the call is sent there at once. The hook runs on server errors only, and only while the call has resends left. The application updates its own map of keys outside the pool.

### Redirects

A redirect is a reply or an error the application recognises in `redirect`. The pool fetches the bytes within the call's deadline and at most `maxRedirectsInflight` at a time, and the promise resolves with `{data}` the way a socket reply does. The URL must be `https:`, or `http:` on a `ws:` endpoint. The URL is fetched as given, with no check of its host, so the application must validate the host of any URL it builds from request data. A redirect of the URL itself is not followed: a 3xx status rejects. On a secure endpoint (`wss:` or `https:`) the fetch asks the runtime for `redirect: 'error'`: a redirect fails the fetch before any hop is sent, because a browser's `'manual'` gives an opaque response no hop can be checked on. The hook names the final URL, and that URL is the one fetched. On an insecure endpoint the fetch asks for `redirect: 'manual'`, and a hop is refused before its request is sent in a runtime which honours it (browsers and undici do). A runtime whose fetch follows a redirect anyway has sent the hop when the pool sees the final URL, and the pool then refuses the response before its body is read; no check of the client can come earlier there. A non-2xx status, a network failure or a body over `maxBytes` (16 MiB by default) rejects with `redirectFailed` (18). The pool does not fetch a redirect twice, because a presigned URL may be single-use. The hook is called with the outcome the server or the connection gave, not with the pool's own timeout or cancel.

### Lifetime and state

Connections open when a call needs one, and a connection opens only after the previous connect of its endpoint has completed. An idle connection is closed after `idleTimeout`; `idleTimeout: 0` starts no idle timer at all. A call without a redirect starts no fetch, and the redirect gate holds no timer. `stats()` returns `{connections, inflight, queued, redirects}` for the application's own pacing. `close()` disconnects every connection; waiting calls reject with `cancelled`, calls in flight with `connectionClosed`, and calls made after it with `clientClosed`.

`pool.on("error", ({endpoint, error}) => ...)` reports a connection that ended for good with a code other than the one `close()` gives, such as a message size close or an unauthorized token.

## Subscription API

What we usually want from Centrifugo is to receive new messages published into channels. To do this we must create `Subscription` object.

### Subscription methods and events

#### Subscribe to a channel

The simplest usage that allow to subscribe on channel and listen to new messages is:

```javascript
const sub = centrifuge.newSubscription('example');

sub.on('publication', function(ctx) {
    // handle new Publication data coming from channel "news".
    console.log(ctx.data);
});

sub.subscribe();
```

#### Subscription events

Some events which can be listened on Subscription object are:

* `publication` – called when new publication received from a Subscription channel
* `join` – called when someone joined channel
* `leave` – called when someone left channel
* `subscribing` - called when Subscription goes to `subscribing` state (initial subscribe and re-subscribes)
* `subscribed` – called when Subscription goes to `subscribed` state
* `unsubscribed` – called when Subscription goes to `unsubscribed` state
* `error` – called when subscription on channel failed with error. It can be called several times
    during lifetime as browser client automatically resubscribes on channels after successful reconnect 
    (caused by temporary network disconnect for example or Centrifugo server restart)

Don't be frightened by amount of events available. In most cases you only need some of them until you need full control to what happens with your subscriptions.

**`Subscription` objects are instances of [EventEmitter](https://nodejs.org/api/events.html#events_class_eventemitter).**

#### presence method of Subscription

`presence` allows to get information about clients which are subscribed on channel at
this moment. Note that this information is only available if `presence` option enabled
in Centrifugo configuration for all channels or for channel namespace.

```javascript
const sub = centrifuge.newSubscription("news");
sub.subscribe()

sub.presence().then(function(ctx) {
    console.log(ctx.clients);
}, function(err) {
    // presence call failed with error
});
```

`presence` is internally a promise that will be waiting for subscription subscribe success if required.

As you can see presence data is a map where keys are client IDs and values are objects
with client information.

Format of `err` in error callback:

```javascript
{
    "code": 108,
    "message": "not available"
}
```

* `code` - error code (number)
* `message` – error description (string)

*Note, that in order presence to work corresponding options must be enabled in server channel configuration (on top level or for channel namespace)*

#### presenceStats method of subscription

`presenceStats` allows to get two counters from a server: number of total clients currently subscribed and number of unique users currently subscribed. Note that this information is only available if `presence` option enabled in server configuration for a channel.

```javascript
sub.presenceStats().then(function(ctx) {
    console.log(ctx.numClients);
}, function(err) {
    // presence stats call failed with error
});
```

#### history method of subscription

`history` method allows to get last messages published into channel. Note that history for channel must be configured in Centrifugo to be available for `history` calls from client.

```javascript
sub.history({limit: 100}).then(function(ctx) {
    console.log(ctx.publications);
}, function(err) {
    // history call failed with error
});
```

*Note, that in order history to work corresponding options must be enabled in server channel configuration (on top level or for channel namespace)*

Some history options available:

* `limit` (number)
* `since` (StreamPosition)
* `reverse` (boolean)

```javascript
resp = await subscription.history({'since': {'offset': 2, 'epoch': 'xcf4w'}, limit: 100});
```

If server can't fulfill a query for history (due to stream retention - size or expiration, or malformed offset, or stream already has another epoch) then an Unrecoverable Position Error will be returned (code `112`).

To only call for current `offset` and `epoch` use:

```javascript
resp = await subscription.history({limit: 0});
```

I.e. not providing `since` and using zero `limit`.

#### publish method of subscription

`publish` method of Subscription object allows publishing data into channel directly from a client.

**Using client-side publish is not an idiomatic Centrifugo usage in many cases. Centrifugo is standalone server and when publishing from a client you won't get the message on the backend side (except using publish proxy feature of Centrifugo). In most real-life apps you need to send new data to your application backend first (using the convenient way, for example AJAX request in web app) and then publish data to Centrifugo over Centrifugo API.**

*Just like presence and history publish must be allowed in Centrifugo configuration for all channels or for channel namespace.*

```javascript
sub.publish({"input": "hello world"}).then(function() {
        // success ack from Centrifugo received
    }, function(err) {
        // publish call failed with error
    });
});
```

*Note, that in order publish to work in Centrifugo corresponding option must be enabled in server channel configuration or client should have capability to publish*.

#### Publish debouncing

When the server sets a publish debounce interval for the channel (Centrifugo option `client_publish_debounce_interval`, returned to the client in the subscribe reply), the SDK coalesces rapid publishes:

* the first publish goes out at once and opens a window of that interval;
* publishes inside the window replace the pending data, and their promises resolve immediately; when the window closes, the latest data is sent and a new window opens;
* a publish after an idle window goes out at once again;
* on a map subscription each key has its own window;
* pending data is dropped on `unsubscribe()`, on disconnect and on `remove(key)` of that key, and nothing more is sent for it;
* an error of a publish sent when a window closes is not reported, since no caller awaits it.

Debouncing is meant for fire-and-forget data such as cursor positions. Use it only when intermediate values may be dropped: a publish that is replaced inside the window is never delivered.

#### unsubscribe method of subscription

You can call `unsubscribe` method to unsubscribe from a channel:

```javascript
sub.unsubscribe();
```

**Important thing to mention** is that unsubscribing from subscription does not remove event handlers you already set to that Subscription object. This allows to simply subscribe to channel again later calling `.subscribe()` method of subscription (see below). But there are cases when your code structured in a way that you need to remove event handlers after unsubscribe **to prevent them be executed twice** in the future. To do this remove event listeners explicitly after calling `unsubscribe()`:

```javascript
sub.unsubscribe();
sub.removeAllListeners();
```

#### ready method of subscription

Returns a Promise which will be resolved upon subscription success (i.e. when Subscription goes to `subscribed` state).

#### setData method of subscription

`setData` (since v5.5.0) allows setting subscription data (some extra payload to deliver to the backend with subscription request). This only applies on the next subscription attempt. Note that if `getData` callback is configured, it will override this value during resubscriptions.

#### setTagsFilter method of subscription

`setTagsFilter` (since v5.5.0) allows setting tags filter for the subscription. Cannot be used together with `delta` option.

See Centrifugo [Channel publication filtering](https://centrifugal.dev/docs/server/publication_filtering) docs.

```javascript
const tagsFilter = {
    key: "ticker",
    cmp: "eq",
    val: ticker
};
sub.setTagsFilter(tagsFilter);
```

### Subscription token

You may want to provide subscription token:

```javascript
const sub = centrifuge.newSubscription("news", {
    token: '<SUBSCRIPTION_TOKEN>'
});
```

In case of Centrifugo on a server side this may be a JSON Web Token - see [channel token auth documentation](https://centrifugal.github.io/centrifugo/server/channel_token_auth) for details on how to generate it on your backend side.

**Subscription token must come to the frontend from application backend - i.e. must be generated on the backend side**. The way to deliver token to the application frontend is up to the developer. Usually you can pass it in template rendering context or issue a separate call to request a connection token from the backend.

If token sets subscription expiration client SDK will keep token refreshed. It does this by calling special callback function. This callback must return a new token. If new token with updated subscription expiration returned from a calbback then it's sent to Centrifugo. If your callback returns an empty string – this means user has no permission to subscribe to a channel anymore and subscription will be unsubscribed. In case of error returned by your callback SDK will retry operation after some jittered time. 

An example:

```javascript
import { Centrifuge, UnauthorizedError } from '@impulsem/centrifuge';

async function getToken(ctx) {
    // ctx argument has a channel.
    const res = await fetch('/centrifuge/subscription_token', {
        method: 'POST',
        headers: new Headers({ 'Content-Type': 'application/json' }),
        body: JSON.stringify(ctx)
    });
    if (!res.ok) {
        if (res.status === 403) {
            // Return special error to not proceed with token refreshes, subscription will be unsubscribed.
            throw new UnauthorizedError();
        }
        // Any other error thrown will result into token refresh re-attempts.
        throw new Error(`Unexpected status code ${res.status}`);
    }
    const data = await res.json();
    return data.token;
}

const client = new Centrifuge('ws://localhost:8000/connection/websocket', {});

const sub = centrifuge.newSubscription(channel, {
    token: 'JWT-GENERATED-ON-BACKEND-SIDE',
    getToken: getToken,
});
sub.subscribe();
```

> If initial token is not provided, but `getToken` is specified – then SDK assumes that developer wants to use token authorization for a channel subscription. In this case SDK attempts to get a subscription token before initial subscribe.

### Subscription Options

When creating a new subscription using `centrifuge.newSubscription(channel, options)`, you can provide various options to customize subscription behavior:

#### token

Allows setting initial subscription token (JWT). See [Subscription token](#subscription-token) section for more details.

#### getToken

Allows setting function to get/refresh subscription token. This will only be called when new token needed, not on every resubscribe. See [Subscription token](#subscription-token) section for more details.

#### data

Data to send to a server with subscribe command.

#### getData

Allows setting function to get/renew subscription data during resubscriptions. In many cases you may prefer using `setData` method of Subscription instead.

```javascript
const sub = centrifuge.newSubscription("news", {
    getData: async (ctx) => {
        // ctx.channel contains channel name
        return { 'timestamp': Date.now() };
    }
});
```

#### since

Force recovery on first subscribe from a provided `StreamPosition`. This is useful when you want to recover messages from a specific point during initial Subscription initialization.

```javascript
const sub = centrifuge.newSubscription("news", {
    since: { offset: 100, epoch: 'xyz' }
});
```

#### minResubscribeDelay

Min delay between resubscribe attempts in milliseconds. Default is `500`.

#### maxResubscribeDelay

Max delay between resubscribe attempts in milliseconds. Default is `20000`.

#### delta

Delta format to be used for differential updates. Currently only `'fossil'` is supported. Cannot be used together with `tagsFilter`.

Delta also works without positioning (no `since`, no recovery) when the server keeps the latest publication of the channel (`keep_latest_publication`): the first publication after subscribe is full; later publications are deltas when the publisher sets `delta` on publish (or the namespace has `delta_publish`). The base is kept per node, so this is at most once; map subscriptions are not covered.

```javascript
const sub = centrifuge.newSubscription("news", {
    delta: 'fossil'
});
```

#### Channel compaction

When the server allows channel compaction (`allow_channel_compaction`), the SDK negotiates a numeric channel id on subscribe automatically, and the server then sends the id instead of the channel name in publication, join and leave pushes. Nothing needs to be configured on the client. Server-side subscriptions (for example those from the connection token) are not compacted.

#### tagsFilter

Server-side tags filter to apply for publications in channel. Cannot be used together with `delta`.

See Centrifugo [Channel publication filtering](https://centrifugal.dev/docs/server/publication_filtering) docs.

```javascript
const tagsFilter = {
    key: "ticker",
    cmp: "eq",
    val: ticker
};

const sub = centrifuge.newSubscription("tickers", {
    tagsFilter: tagsFilter
});
```

## Subscription management API

According to [client SDK spec](https://centrifugal.dev/docs/transports/client_api#subscription-management) centrifuge-js supports several methods to manage client-side subscriptions in internal registry. The following methods are available on top level of the Centrifuge SDK client instance.

### newSubscription

`newSubscription(channel: string, options?: Partial<SubscriptionOptions>): Subscription`

Creates new `Subscription` to a channel or throws an exception if the Subscription to a channel already exists in the internal registry of the client.

### getSubscription

`getSubscription(channel: string): Subscription | null`

getSubscription returns `Subscription` if it's registered in the internal registry or `null`.

### removeSubscription

`removeSubscription(sub: Subscription | null)`

removeSubscription allows removing Subcription from the internal registry.

### subscriptions

`subscriptions(): Record<string, Subscription>`

 Get a map with all current client-side subscriptions registered in the client.

## Stream subscriptions with `getState`

For positioned/recoverable stream channels, the SDK can call an app-supplied `getState` callback that loads the application's current state from its own source of truth (database, API, etc.) and returns the corresponding stream position. The SDK subscribes with recovery from that position, so any publications committed after the state was read arrive as `publication` events. This is the "app-owned state" pattern — Centrifugo delivers only change events, your database stays the canonical store.

The SDK invokes `getState`:

* on the initial subscribe (no saved position);
* on reconnect **only when server recovery fails** (server returns error 112 — unrecoverable position).

On a successful recovery, `getState` is **not** called — the SDK has a valid position and missed publications arrive as events.

Inside the callback, **read the stream position first, then read your data** so the position is a lower bound. Recovered publications may overlap data already loaded in `getState`; this is safe for idempotent updates, otherwise deduplicate by `offset`. See `SubscriptionOptions.getState` JSDoc for the full contract.

Requires **Centrifugo >= v6.8.0**.

References:

* Blog post — [Transactional publishing for stream subscriptions with PostgreSQL](https://centrifugal.dev/blog/2026/05/15/pg-stream-broker-benefits)
* Example — [pg_stream_broker kitchen orders demo](https://github.com/centrifugal/examples/tree/master/v6/pg_stream_broker)

## Map subscriptions (experimental)

A **map subscription** delivers a real-time key-value collection whose lifecycle is managed by Centrifugo: clients receive a complete snapshot on subscribe, catch up on missed key changes after disconnects, and observe live per-key updates. Typical use cases include cursor positions, lobby members, feature flags, scoreboards, and per-key presence (`map_clients` / `map_users`).

Use `Centrifuge.newMapSubscription(channel, options?)` to create one. The variants `newMapClientsSubscription` and `newMapUsersSubscription` produce presence-style map subscriptions backed by `$clients:*` / `$users:*` channels. The returned `MapSubscription` emits a `sync` event whenever a fresh snapshot is delivered — on the initial subscribe, and again only when the SDK rebuilds state from scratch (e.g. after an unrecoverable position with the default `from_scratch` strategy). Every individual key change (set or remove) — both during live operation and on successful recovery after a disconnect — arrives as an `update` event; on successful recovery `sync` is suppressed because the application's existing snapshot is still valid. Call `publish(key, data)` / `remove(key)` to mutate.

> [!WARNING]
> Map subscriptions are **experimental** and require **Centrifugo >= v6.8.0**. The shape and behavior may change in a backwards-incompatible way in subsequent minor releases as the feature stabilizes.

References:

* Server docs — [Map subscriptions](https://centrifugal.dev/docs/server/map_subscriptions)
* PRO enhancements — [Map subscriptions (PRO)](https://centrifugal.dev/docs/pro/map_subscriptions)
* Blog series — Map subscriptions Part 1 (synchronized key-value state and presence) and Part 2 (PostgreSQL map broker with transactional publishing)

## Shared poll subscriptions (experimental)

A **shared poll subscription** moves polling from clients to Centrifugo: clients register their interest in specific keys, and Centrifugo polls the backend once per configurable interval on behalf of everyone, fanning the changes back out. Backend load scales with the number of unique tracked items rather than the number of connected clients. Typical use cases include vote counts, prices, view counts, live scores, and configuration sync.

Use `Centrifuge.newSharedPollSubscription(channel, options?)`, then call `track(keys)` / `untrack(keys)` on the returned `SharedPollSubscription` to manage the interest set. Provide the `getSignature` callback in options to authorize tracked keys via per-key HMAC signatures (replayed on reconnect and refreshed on signature TTL). The subscription emits an `update` event per item whenever the backend reports a new version, including synthetic `removed: true` events when the server revokes a key.

> [!WARNING]
> Shared poll subscriptions are **experimental** and require **Centrifugo >= v6.8.0**. The shape and behavior may change in a backwards-incompatible way in subsequent minor releases as the feature stabilizes.

References:

* Server docs — [Shared poll subscriptions](https://centrifugal.dev/docs/server/shared_poll)
* PRO enhancements — [Shared poll (PRO)](https://centrifugal.dev/docs/pro/shared_poll)
* Blog post — [Shared poll subscriptions: O(unique items) polling with low-latency updates](https://centrifugal.dev/blog/2026/05/21/shared-poll-subscriptions)

## Message batching

There is also a command batching support. It allows to send several commands to a server in one request - may be especially useful when connection established via one of HTTP-based transports.

You can start collecting commands by calling `startBatching()` method:

```javascript
centrifuge.startBatching();
```

Finally if you don't want batching anymore call `stopBatching()` method:

```javascript
centrifuge.stopBatching();
```

This call will flush all collected commands to a network.

## Server-side subscriptions

We encourage using client-side subscriptions where possible as they provide a better control and isolation from connection. But in some cases you may want to use [server-side subscriptions](https://centrifugal.dev/docs/server/server_subs) (i.e. subscriptions created by server upon connection establishment).

Technically, client SDK keeps server-side subscriptions in internal registry (similar to client-side subscriptions but without possibility to control them).

To listen for server-side subscription events use callbacks as shown in example below:

```javascript
const client = new Centrifuge('ws://localhost:8000/connection/websocket', {});

client.on('subscribed', function(ctx) {
    // Called when subscribed to a server-side channel upon Client moving to
    // connected state or during connection lifetime if server sends Subscribe
    // push message.
    console.log('subscribed to server-side channel', ctx.channel);
});

client.on('subscribing', function(ctx) {
    // Called when existing connection lost (Client reconnects) or Client
    // explicitly disconnected. Client continue keeping server-side subscription
    // registry with stream position information where applicable.
    console.log('subscribing to server-side channel', ctx.channel);
});

client.on('unsubscribed', function(ctx) {
    // Called when server sent unsubscribe push or server-side subscription
    // previously existed in SDK registry disappeared upon Client reconnect.
    console.log('unsubscribed from server-side channel', ctx.channel);
});

client.on('publication', function(ctx) {
    // Called when server sends Publication over server-side subscription.
    console.log('publication receive from server-side channel', ctx.channel, ctx.data);
});

client.connect();
```

Server-side subscription events mostly mimic events of client-side subscriptions. But again – they do not provide control to the client and managed entirely by a server side.

Additionally, Client has several top-level methods to call with server-side subscription related operations:

* `publish(channel, data)`
* `history(channel, options)`
* `presence(channel)`
* `presenceStats(channel)`

## Protobuf support

To import client which uses Protobuf protocol under the hood:

```html
<script src="https://unpkg.com/@impulsem/centrifuge@1.0.0/dist/centrifuge.protobuf.js"></script>
```

Or if you are developing with npm:

```javascript
import { Centrifuge } from '@impulsem/centrifuge/build/protobuf';
```

This client uses [protobuf.js](https://github.com/dcodeIO/ProtoBuf.js/) under the hood.

When running with Protobuf-based client, you can send and receive any binary data as `Uint8Array`. Make sure data is properly encoded when calling methods of Centrifuge Protobuf-based instance. For example, you can not just send JSON-like objects like in JSON protocol case, you need to encode data to `Uint8Array` first:

```javascript
const data = new TextEncoder("utf-8").encode(JSON.stringify({"any": "data"})); 
sub.publish(data);
```

## Using with NodeJS

NodeJS does not have native WebSocket library in std lib. To use `centrifuge-js` on Node you need to explicitly provide WebSocket constructor to the library.

First, install WebSocket dependency:

```
npm install ws
```

At this point you have 2 options. Explicitly pass WebSocket object to Centrifuge.

```javascript
import { Centrifuge } from '@impulsem/centrifuge';
import WebSocket from 'ws';

var centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket', {
    websocket: WebSocket
})
```

Or define it globally:

```javascript
import { Centrifuge } from '@impulsem/centrifuge';
import WebSocket from 'ws';

global.WebSocket = WebSocket;

const centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket');
```

## Custom WebSocket constructor

If you are building a client for a non-browser environment and want to pass custom headers then you can use the following approach to wrap a WebSocket constructor and let custom options to be used on connection initialization:

```javascript
const myWs = function (options) {
  return class wsClass extends WebSocket {
    constructor(...args) {
      if (args.length === 1) {
        super(...[...args, 'centrifuge-json', ...[options]])
      } else {
        super(...[...args, ...[options]])
      }
    }
  }
}
```

It should be now possible to use pass your custom WebSocket constructor to `centrifuge-js` and so custom headers will be used when connecting to a server (only in non-browser environment):

```javascript
var centrifuge = new Centrifuge('ws://localhost:8000/connection/websocket', {
    websocket: myWs({ headers: { Authorization: '<token or key>' } }),
});
```

See a basic example with React Native where this technique is used in the upstream centrifuge-js discussion [#224](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.JS/issues) (upstream issue 224).

## Using with React Native on Android

If you have issues with the connection on Android when using React Native – [check out the upstream centrifuge-js discussion](https://github.com/Impulse-m/ImpulseM.Centrifugo.Client.JS/issues) (upstream issue 242) – you may be using non-secure endpoint schemes and need to explicitly allow it.

## Errors in callbacks

There is currently no built-in error handling in the SDK for exceptions happening in application-level callbacks, which means you must catch any error that could be thrown in a listener. Not doing that may corrupt a state of SDK making it unusable.

## Run tests locally

If you want to run `centrifuge-js` tests locally, start test Centrifugo server:

```
docker compose up
```

Then:

```
yarn test
```
