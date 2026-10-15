import { errorCodes } from './codes';

/** A running call timer. clear() stops it, also while its grace period runs. */
export interface CallTimer {
  clear(): void;
}

// A call or no-ping timer running at least this late means the event loop didn't
// run meanwhile: the process was suspended or blocked (see callTimer).
const lateCallTimer: number = 1000;
// How long such a call still waits for its reply, or the connection for a ping,
// before failing.
const lateCallGrace: number = 1000;

/**
 * Runs onDue at the due time, local time in milliseconds. A timer running late by lateCallTimer or
 * more means the process was suspended or blocked, and the data waiting in the socket may not have
 * been processed yet: onDue then waits lateCallGrace more, so that data is processed first.
 * @internal
 */
export function callTimer(due: number, onDue: () => void): CallTimer {
  let timer: ReturnType<typeof setTimeout> | null = null;
  timer = setTimeout((): void => {
    const late: boolean = Date.now() - due >= lateCallTimer;
    timer = setTimeout((): void => {
      timer = null;
      onDue();
    }, late ? lateCallGrace : 0);
  }, Math.max(0, due - Date.now()));
  return {
    clear: (): void => {
      if (timer !== null) {
        clearTimeout(timer);
        timer = null;
      }
    },
  };
}

/**
 * Registers a waiter of a client under id: it settles with resolve or reject, with a timeout error after the timeout
 * in milliseconds (none when null), or with a cancelled error when the signal aborts. It leaves the waiters once it
 * settled, so its timer and its abort listener go with it.
 * @internal
 */
export function addWaiter(
  waiters: Record<number, any>,
  id: number,
  timeout: number | null,
  signal: AbortSignal | undefined,
  resolve: (value: any) => void,
  reject: (error: any) => void
): void {
  let timer: CallTimer | null = null;
  const leave: () => void = (): void => {
    timer?.clear();
    signal?.removeEventListener('abort', onAbort);
    delete waiters[id];
  };
  const onAbort: () => void = (): void => {
    leave();
    reject({ code: errorCodes.cancelled, message: 'cancelled' });
  };
  timer = timeout === null ? null : callTimer(Date.now() + timeout, (): void => {
    leave();
    reject({ code: errorCodes.timeout, message: 'timeout' });
  });
  signal?.addEventListener('abort', onAbort, { once: true });
  waiters[id] = {
    resolve: (value: any): void => {
      leave();
      resolve(value);
    },
    reject: (error: any): void => {
      leave();
      reject(error);
    },
  };
}

/**
 * Settles like the promise, unless the deadline passes first (the timeout error) or the signal
 * aborts first (the cancelled error). The timer and the abort listener are removed once it settled.
 * @internal
 */
export function raceCall<T>(
  promise: Promise<T>,
  deadline: number,
  signal: AbortSignal | undefined,
  timeoutError: () => any,
  cancelledError: () => any
): Promise<T> {
  return new Promise<T>((resolve: (value: T) => void, reject: (error: any) => void): void => {
    let timer: CallTimer | null = null;
    const finish: () => void = (): void => {
      if (timer !== null) {
        timer.clear();
        timer = null;
      }
      signal?.removeEventListener('abort', onAbort);
    };
    const onAbort: () => void = (): void => {
      finish();
      reject(cancelledError());
    };
    signal?.addEventListener('abort', onAbort, { once: true });
    timer = callTimer(deadline, (): void => {
      finish();
      reject(timeoutError());
    });
    promise.then((value: T): void => {
      finish();
      resolve(value);
    }, (error: any): void => {
      finish();
      reject(error);
    });
  });
}
