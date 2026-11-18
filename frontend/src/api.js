// Small fetch helpers shared by every view.
export const API_URL = import.meta.env.DEV ? 'http://localhost:8000' : '';

export const WS_URL = import.meta.env.DEV
  ? 'ws://localhost:8000/ws/dashboard'
  : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/dashboard`;

export async function api(path, options = {}) {
  const res = await fetch(`${API_URL}${path}`, {
    headers: options.body ? { 'Content-Type': 'application/json' } : undefined,
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });
  let data = null;
  try { data = await res.json(); } catch { /* empty body */ }
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
export const CANCELLABLE = new Set(['Queued', 'Scheduled', 'RetryScheduled']);

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
