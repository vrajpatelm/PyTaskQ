// Small fetch helpers shared by every view.
// Always same-origin: in development, Vite proxies the API to localhost:8000
// (vite.config.js), so the server never has to accept cookies from another origin.
export const API_URL = '';

export const WS_URL = `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/dashboard`;

// Fired when the server says the session is gone, so the app can show sign-in.
export const UNAUTHORIZED_EVENT = 'pytaskq:unauthorized';

export async function api(path, options = {}) {
  const res = await fetch(`${API_URL}${path}`, {
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
    credentials: 'include',            // send the session cookie (also to the Vite dev server's API)
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
  if (res.status === 401 && !path.startsWith('/auth/')) {
    window.dispatchEvent(new Event(UNAUTHORIZED_EVENT));
  }
  if (!res.ok) {
    const detail = data?.detail;
    const msg = Array.isArray(detail)
      ? detail.map(d => `${(d.loc || []).slice(1).join('.')}: ${d.msg}`).join('; ')
      : detail || data?.error || `Request failed (HTTP ${res.status})`;
    throw new Error(msg);
  }
  return data;
}

// Status vocabulary shown to people (server keeps its own names).
export const STATUS = {
  Scheduled:      { label: 'Scheduled', tone: 'waiting' },
  Queued:         { label: 'Queued', tone: 'waiting' },
  queued:         { label: 'Queued', tone: 'waiting' },
  Processing:     { label: 'Running', tone: 'running' },
  RetryScheduled: { label: 'Retrying', tone: 'warning' },
  Success:        { label: 'Succeeded', tone: 'success' },
  Failed:         { label: 'Failed', tone: 'danger' },
  DeadLetter:     { label: 'Dead letter', tone: 'danger' },
  Cancelled:      { label: 'Cancelled', tone: 'muted' },
};

export const FINAL = new Set(['Success', 'Failed', 'DeadLetter', 'Cancelled']);

// Workflows and their steps (steps that never ran are Pending or Skipped).
export const WF_STATUS = {
  Running:         { label: 'Running', tone: 'running' },
  Succeeded:       { label: 'Succeeded', tone: 'success' },
  PartlySucceeded: { label: 'Partly succeeded', tone: 'warning' },
  Failed:          { label: 'Failed', tone: 'danger' },
  Cancelled:       { label: 'Cancelled', tone: 'muted' },
  Pending:         { label: 'Not started', tone: 'muted' },
  Skipped:         { label: 'Skipped', tone: 'muted' },
  Gone:            { label: 'Expired', tone: 'muted' },
};
export const CANCELLABLE = new Set(['Queued', 'Scheduled', 'RetryScheduled', 'Processing']);

export function timeAgo(ts) {
  if (!ts) return '—';
  const s = Math.max(0, Date.now() / 1000 - ts);
  if (s < 5) return 'just now';
  if (s < 60) return `${Math.floor(s)}s ago`;
  if (s < 3600) return `${Math.floor(s / 60)}m ago`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ago`;
  return `${Math.floor(s / 86400)}d ago`;
}

export function formatDuration(ms) {
  if (ms === undefined || ms === null || ms === '') return '—';
  const n = Number(ms);
  if (n < 1000) return `${Math.round(n)} ms`;
  if (n < 60000) return `${(n / 1000).toFixed(1)} s`;
  return `${Math.floor(n / 60000)}m ${Math.round((n % 60000) / 1000)}s`;
}

export function formatClock(ts) {
  if (!ts) return '—';
  return new Date(ts * 1000).toLocaleString();
}

export function formatArgs(args, kwargs) {
  const parts = [];
  (args || []).forEach(a => parts.push(JSON.stringify(a)));
  Object.entries(kwargs || {}).forEach(([k, v]) => parts.push(`${k}=${JSON.stringify(v)}`));
  const s = parts.join(', ');
  return s.length > 60 ? `${s.slice(0, 57)}…` : s;
}

// Accept JSON when it parses, otherwise treat the text as a plain string.
export function parseValue(text) {
  const t = String(text ?? '').trim();
  if (t === '') return undefined;
  try { return JSON.parse(t); } catch { return t; }
}

export function prettyResult(value) {
  if (value === undefined || value === null || value === '') return '';
  try { return JSON.stringify(JSON.parse(value), null, 2); } catch { return String(value); }
}

export const storage = {
  get(key, fallback) {
    try { const v = localStorage.getItem(key); return v === null ? fallback : v; } catch { return fallback; }
  },
  set(key, value) {
    try { localStorage.setItem(key, value); } catch { /* private mode */ }
  },
};

// Roles, lowest to highest. `can(user, 'operator')` mirrors the server's check.
export const ROLES = ['viewer', 'operator', 'admin'];
export const ROLE_LABEL = { viewer: 'Viewer', operator: 'Operator', admin: 'Admin' };
export const ROLE_HELP = {
  viewer: 'Can see jobs, workers and dead letters',
  operator: 'Can see everything and run, cancel, replay or delete jobs',
  admin: 'Can do everything, including managing people and invites',
};
export function can(user, role) {
  return !!user && !user.disabled && ROLES.indexOf(user.role) >= ROLES.indexOf(role);
}
