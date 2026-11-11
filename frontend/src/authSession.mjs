const AUTH_KEY_PATTERNS = [
  /auth/i,
  /token/i,
  /jwt/i,
  /session/i,
  /^user$/i,
  /user_id/i,
  /access/i,
  /refresh/i,
];

function isAuthKey(key) {
  return AUTH_KEY_PATTERNS.some(pattern => pattern.test(key));
}

function clearMatchingStorage(storage) {
  const keys = [];
  for (let index = 0; index < storage.length; index += 1) {
    const key = storage.key(index);
    if (key && isAuthKey(key)) keys.push(key);
  }
  keys.forEach(key => storage.removeItem(key));
}

export function clearAuthSession() {
  try {
    clearMatchingStorage(localStorage);
  } catch {}

  try {
    clearMatchingStorage(sessionStorage);
  } catch {}

  try {
    document.cookie.split(';').forEach(cookie => {
      const name = cookie.split('=')[0]?.trim();
      if (!name || !isAuthKey(name)) return;
      document.cookie = `${name}=; Max-Age=0; path=/`;
      document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 GMT; path=/`;
    });
  } catch {}
}

export function logoutToLogin() {
  clearAuthSession();
  window.location.assign('/login');
}
