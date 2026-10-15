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
 * The one wait with a deadline and a signal, shared by the waiters and the races of the rpc calls. It calls
 * onDue at the due time (local time in milliseconds, none when due is null), or onAbort when the signal aborts,
 * whichever comes first, and only once. Either call removes the timer and the abort listener first. A signal
 * that is aborted already calls onAbort at once and arms nothing. The returned timer clears the wait without a call.
 * @internal
 */
export function armWait(
  due: number | null,
  signal: AbortSignal | undefined,
  onDue: () => void,
  onAbort: () => void
): CallTimer {
  if (signal?.aborted === true) {
    onAbort();
    return { clear: (): void => {} };
  }
  let timer: CallTimer | null = null;
  const disarm: () => void = (): void => {
    if (timer !== null) {
      timer.clear();
      timer = null;
    }
    signal?.removeEventListener('abort', onAbortEvent);
  };
  const onAbortEvent: () => void = (): void => {
    disarm();
    onAbort();
  };
  if (due !== null) {
    timer = callTimer(due, (): void => {
      disarm();
      onDue();
    });
  }
  signal?.addEventListener('abort', onAbortEvent, { once: true });
  return { clear: disarm };
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
  let wait: CallTimer | null = null;
  const leave: () => void = (): void => {
    if (wait !== null) {
      wait.clear();
    }
    delete waiters[id];
  };
  // The waiter is registered before the wait is armed: an aborted signal settles it at once, and leaves it.
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
  wait = armWait(
    timeout === null ? null : Date.now() + timeout,
    signal,
    (): void => {
      leave();
      reject({ code: errorCodes.timeout, message: 'timeout' });
    },
    (): void => {
      leave();
      reject({ code: errorCodes.cancelled, message: 'cancelled' });
    }
  );
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
    const wait: CallTimer = armWait(
      deadline,
      signal,
      (): void => {
        reject(timeoutError());
      },
      (): void => {
        reject(cancelledError());
      }
    );
    promise.then((value: T): void => {
      wait.clear();
      resolve(value);
    }, (error: any): void => {
      wait.clear();
      reject(error);
    });
  });
}
