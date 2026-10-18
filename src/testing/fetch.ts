// A fetch stub for a redirect that must hold its slot: it never answers, and it fails only when the signal it is given
// aborts, which is how the pool closes or cancels a fetch. The redirect tests use it to keep a fetch running until the
// pool settles it.
export async function holdFetchUntilAborted(signal: AbortSignal): Promise<never> {
  if (!signal.aborted) {
    await new Promise<void>((resolve: () => void): void => {
      signal.addEventListener('abort', resolve);
    });
  }
  throw new Error('aborted');
}
