// Small building blocks shared by the dashboard, sign-in screens and settings.
import { useState } from 'react'
import { Check, Copy, Moon, Sun } from 'lucide-react'

export function CopyButton({ text, label = 'Copy ID', copiedLabel = 'Copied' }) {
  const [done, setDone] = useState(false);
  const copy = () => {
    navigator.clipboard?.writeText(text).then(() => { setDone(true); setTimeout(() => setDone(false), 1500); }).catch(() => {});
  };
  return (
    <button type="button" className="btn btn-ghost btn-sm" onClick={copy} aria-label={label}>
      {done ? <Check size={14} /> : <Copy size={14} />} {done ? copiedLabel : label}
    </button>
  );
}

export function Brand() {
  return (
    <div className="brand">
      <img src="/static/favicon.svg" alt="" width="26" height="26" />
      <span>PyTaskQ</span>
    </div>
  );
}

export function ThemeButton({ theme, setTheme }) {
  return (
    <button className="btn btn-ghost btn-sm" onClick={() => setTheme(t => (t === 'dark' ? 'light' : 'dark'))}
      aria-label={theme === 'dark' ? 'Switch to light theme' : 'Switch to dark theme'}>
      {theme === 'dark' ? <Sun size={15} /> : <Moon size={15} />}
    </button>
  );
}

export function FormError({ message }) {
  if (!message) return null;
  return <div className="notice notice-error" role="alert">{message}</div>;
}
