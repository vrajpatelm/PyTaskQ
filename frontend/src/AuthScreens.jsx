// The screens someone sees before they're signed in:
//   SetupScreen  – first launch: create the admin with the setup code
//   LoginScreen  – everyday sign-in
//   InviteScreen – a teammate opening an invite link
import { useEffect, useState } from 'react'
import { KeyRound, LogIn, UserPlus } from 'lucide-react'
import { api, ROLE_HELP, ROLE_LABEL } from './api'
import { Brand, FormError, ThemeButton } from './ui'

function AuthLayout({ theme, setTheme, children }) {
  return (
    <div className="auth-page">
      <header className="topbar">
        <Brand />
        <ThemeButton theme={theme} setTheme={setTheme} />
      </header>
      <main className="auth-card">{children}</main>
    </div>
  );
}

function PasswordFields({ password, setPassword, confirm, setConfirm, autoComplete = 'new-password' }) {
  return (
    <>
      <label className="field"><span>Password <small>at least 8 characters</small></span>
        <input type="password" value={password} onChange={e => setPassword(e.target.value)}
          autoComplete={autoComplete} minLength={8} required />
      </label>
      <label className="field"><span>Repeat password</span>
        <input type="password" value={confirm} onChange={e => setConfirm(e.target.value)}
          autoComplete={autoComplete} minLength={8} required />
      </label>
    </>
  );
}

function usernameField(username, setUsername, autoFocus) {
  return (
    <label className="field"><span>Username</span>
      <input value={username} onChange={e => setUsername(e.target.value)} autoComplete="username"
        autoCapitalize="none" spellCheck="false" placeholder="e.g. vraj" required autoFocus={autoFocus} />
    </label>
  );
}

export function SetupScreen({ onDone, theme, setTheme }) {
  const [code, setCode] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async e => {
    e.preventDefault();
    if (password !== confirm) { setError('The two passwords don’t match.'); return; }
    setBusy(true); setError('');
    try {
      const res = await api('/auth/setup', { method: 'POST', body: { setup_code: code, username, password } });
      onDone(res.user);
    } catch (err) { setError(err.message); }
    finally { setBusy(false); }
  };

  return (
    <AuthLayout theme={theme} setTheme={setTheme}>
      <h1>Set up PyTaskQ</h1>
      <p className="muted">Create the admin account. You can invite your team afterwards.</p>
      <form onSubmit={submit} className="auth-form">
        <label className="field">
          <span>Setup code</span>
          <input value={code} onChange={e => setCode(e.target.value)} placeholder="XXXXXX-XXXXXX-XXXXXX"
            autoComplete="off" spellCheck="false" required autoFocus className="code-input" />
          <small className="muted">
            Printed in the server log when PyTaskQ started. Lost it? Run <code>pytaskq users setup-code</code> on the server.
          </small>
        </label>
        {usernameField(username, setUsername, false)}
        <PasswordFields {...{ password, setPassword, confirm, setConfirm }} />
        <FormError message={error} />
        <button className="btn btn-primary" disabled={busy}><KeyRound size={16} /> Create admin account</button>
      </form>
    </AuthLayout>
  );
}

export function LoginScreen({ onDone, theme, setTheme, notice }) {
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async e => {
    e.preventDefault();
    setBusy(true); setError('');
    try {
      const res = await api('/auth/login', { method: 'POST', body: { username, password } });
      onDone(res.user);
    } catch (err) { setError(err.message); setPassword(''); }
    finally { setBusy(false); }
  };

  return (
    <AuthLayout theme={theme} setTheme={setTheme}>
      <h1>Sign in</h1>
      {notice && <p className="notice notice-info">{notice}</p>}
      <form onSubmit={submit} className="auth-form">
        {usernameField(username, setUsername, true)}
        <label className="field"><span>Password</span>
          <input type="password" value={password} onChange={e => setPassword(e.target.value)} autoComplete="current-password" required />
        </label>
        <FormError message={error} />
        <button className="btn btn-primary" disabled={busy}><LogIn size={16} /> Sign in</button>
      </form>
      <p className="muted small auth-foot">Forgot your password? Ask an admin, or run <code>pytaskq users reset-password NAME</code> on the server.</p>
    </AuthLayout>
  );
}

export function InviteScreen({ token, onDone, onCancel, theme, setTheme }) {
  const [invite, setInvite] = useState(null);
  const [loadError, setLoadError] = useState('');
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    api(`/auth/invites/${encodeURIComponent(token)}`).then(setInvite).catch(e => setLoadError(e.message));
  }, [token]);

  const submit = async e => {
    e.preventDefault();
    if (password !== confirm) { setError('The two passwords don’t match.'); return; }
    setBusy(true); setError('');
    try {
      const res = await api(`/auth/invites/${encodeURIComponent(token)}/accept`, { method: 'POST', body: { username, password } });
      onDone(res.user);
    } catch (err) { setError(err.message); }
    finally { setBusy(false); }
  };

  return (
    <AuthLayout theme={theme} setTheme={setTheme}>
      <h1>Join PyTaskQ</h1>
      {loadError ? (
        <>
          <FormError message={loadError} />
          <button className="btn btn-secondary" onClick={onCancel}>Go to sign in</button>
        </>
      ) : !invite ? (
        <p className="muted">Checking your invite…</p>
      ) : (
        <>
          <p className="muted">
            <strong>{invite.created_by}</strong> invited you as <strong>{ROLE_LABEL[invite.role]}</strong>: {ROLE_HELP[invite.role].toLowerCase()}.
          </p>
          <form onSubmit={submit} className="auth-form">
            {usernameField(username, setUsername, true)}
            <PasswordFields {...{ password, setPassword, confirm, setConfirm }} />
            <FormError message={error} />
            <button className="btn btn-primary" disabled={busy}><UserPlus size={16} /> Create my account</button>
          </form>
          <p className="muted small auth-foot">Already have an account? <button className="link" onClick={onCancel}>Sign in</button></p>
        </>
      )}
    </AuthLayout>
  );
}
