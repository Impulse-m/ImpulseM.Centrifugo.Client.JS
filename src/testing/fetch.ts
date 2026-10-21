// A fetch stub for a redirect that must hold its slot: it never answers, and it rejects in the abort event of the signal
// it is given, which is how the pool closes or cancels a fetch. The redirect tests use it to keep a fetch running until
// the pool settles it. The stub only ends by abort.
export async function holdFetchUntilAborted(signal: AbortSignal): Promise<never> {
  const error: Error = await new Promise<Error>((resolve: (error: Error) => void): void => {
    if (signal.aborted) {
      resolve(new Error('aborted'));
      return;
    }
    signal.addEventListener('abort', (): void => resolve(new Error('aborted')));
  });
  throw error;
}
