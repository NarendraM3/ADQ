export function humanSourceError(value, fallback = 'Something went wrong') {
  if (typeof value === 'string') {
    const message = value.trim();
    return !message || message === '[object Object]' ? fallback : message;
  }
  if (value instanceof Error) return humanSourceError(value.message, fallback);
  if (value == null) return fallback;
  if (Array.isArray(value)) return value.map(item => humanSourceError(item, '')).filter(Boolean).join('; ') || fallback;
  if (typeof value === 'object') {
    if (typeof value.code === 'string' && typeof value.message === 'string') return `${value.code}: ${value.message}`;
    for (const key of ['detail', 'message', 'error', 'msg']) {
      if (value[key] != null) {
        const message = humanSourceError(value[key], '');
        if (message) return message;
      }
    }
    return fallback;
  }
  return String(value);
}
