const terminalStatuses = new Set(['succeeded', 'incomplete', 'failed']);

export function pollExecution({ executionId, getExecution, getFailed, onUpdate, onError, isCurrent = () => true, delay = ms => new Promise(resolve => setTimeout(resolve, ms)) }) {
  let stopped = false;
  const current = () => !stopped && isCurrent();
  const cancel = () => { stopped = true; };
  const run = async () => {
    let wait = 1000;
    while (current()) {
      try {
        const result = await getExecution(executionId);
        if (!current()) return cancel;
        onUpdate(result);
        if (terminalStatuses.has(result.status)) {
          try {
            const details = await getFailed(executionId);
            if (current()) onUpdate(result, Array.isArray(details) ? details : []);
          } catch (error) {
            if (current()) onError(error, true);
          }
          return cancel;
        }
        await delay(1500);
      } catch (error) {
        if (!current()) return cancel;
        onError(error, false);
        if (error?.status === 404) return cancel;
        await delay(wait);
        wait = Math.min(wait * 2, 10000);
      }
    }
    return cancel;
  };
  void run();
  return cancel;
}
