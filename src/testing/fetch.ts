// A fetch stub for a redirect that must hold its slot: it never answers, and it rejects in the abort event of the signal
// it is given, which is how the pool closes or cancels a fetch. The redirect tests use it to keep a fetch running until
// the pool settles it. The resolve parameter is never called: the fetch only ends by abort.
export function holdFetchUntilAborted(signal: AbortSignal): Promise<never> {
  return new Promise<never>((resolve: (value: never) => void, reject: (error: Error) => void): void => {
    void resolve;
    if (signal.aborted) {
      reject(new Error('aborted'));
      return;
    }
    signal.addEventListener('abort', (): void => reject(new Error('aborted')));
  });
}
