// Settings tab: your account, API tokens, and (for admins) the team.
import { useCallback, useEffect, useState } from 'react'
import { KeyRound, Link2, LogOut, Plus, ShieldAlert, Trash2 } from 'lucide-react'
import { api, can, formatClock, ROLE_HELP, ROLE_LABEL, ROLES, timeAgo } from './api'
import { CopyButton, FormError } from './ui'

function Section({ title, description, children, actions }) {
  return (
    <section className="panel settings-section">
      <div className="settings-head">
        <div>
          <h2>{title}</h2>
          {description && <p className="muted small">{description}</p>}
        </div>
        {actions}
      </div>
      {children}
    </section>
  );
}

function inDays(ts) {
  const d = Math.max(0, Math.round((ts - Date.now() / 1000) / 86400));
  return d === 0 ? 'today' : d === 1 ? 'in 1 day' : `in ${d} days`;
}

// ── account ──────────────────────────────────────────────────────────────────

function AccountSection({ me, onSignOut, notify }) {
  const [current, setCurrent] = useState('');
  const [next, setNext] = useState('');
  const [confirm, setConfirm] = useState('');
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const submit = async e => {
    e.preventDefault();
    if (next !== confirm) { setError('The new passwords don’t match.'); return; }
    setBusy(true); setError('');
    try {
      const res = await api('/auth/me/password', { method: 'POST', body: { current_password: current, new_password: next } });
      notify('success', res.message);
      setCurrent(''); setNext(''); setConfirm('');
    } catch (err) { setError(err.message); }
    finally { setBusy(false); }
  };

  return (
    <Section
      title="Your account"
      description={`Signed in as ${me.username} · ${ROLE_LABEL[me.role]}: ${ROLE_HELP[me.role].toLowerCase()}.`}
      actions={<button className="btn btn-ghost btn-sm" onClick={onSignOut}><LogOut size={14} /> Sign out</button>}
    >
      <form className="inline-form" onSubmit={submit}>
        <h3>Change password</h3>
        <div className="grid-3">
          <label className="field"><span>Current password</span>
            <input type="password" value={current} onChange={e => setCurrent(e.target.value)} autoComplete="current-password" required />
          </label>
          <label className="field"><span>New password</span>
            <input type="password" value={next} onChange={e => setNext(e.target.value)} autoComplete="new-password" minLength={8} required />
          </label>
          <label className="field"><span>Repeat new password</span>
            <input type="password" value={confirm} onChange={e => setConfirm(e.target.value)} autoComplete="new-password" minLength={8} required />
          </label>
        </div>
        <FormError message={error} />
        <div><button className="btn btn-secondary btn-sm" disabled={busy}><KeyRound size={14} /> Change password</button></div>
        <p className="muted small">Changing it signs you out on every other device.</p>
      </form>
    </Section>
  );
}

// ── API tokens ───────────────────────────────────────────────────────────────

function TokensSection({ me, notify }) {
  const [tokens, setTokens] = useState([]);
  const [name, setName] = useState('');
  const [fresh, setFresh] = useState(null);

  const load = useCallback(() => {
    api('/auth/me/tokens').then(d => setTokens(d.tokens || [])).catch(() => {});
  }, []);
  useEffect(() => { load(); }, [load]);

  const create = async e => {
    e.preventDefault();
    try {
      const res = await api('/auth/me/tokens', { method: 'POST', body: { name: name || 'API token' } });
      setFresh(res.token); setName(''); load();
    } catch (err) { notify('error', err.message); }
  };
  const revoke = async id => {
    try { await api(`/auth/me/tokens/${id}`, { method: 'DELETE' }); notify('success', 'Token revoked.'); load(); }
    catch (err) { notify('error', err.message); }
  };

  const origin = window.location.origin;
  return (
    <Section
      title="API tokens"
      description={`For scripts and other services calling the PyTaskQ API. A token acts as you, with ${ROLE_LABEL[me.role]} permissions.`}
    >
      {fresh && (
        <div className="token-reveal" role="status">
          <p><strong>Copy your new token now.</strong> For your security it won’t be shown again.</p>
          <div className="token-row"><code>{fresh}</code><CopyButton text={fresh} label="Copy token" /></div>
          <pre className="code">{`curl ${origin}/tasks -H "Authorization: Bearer ${fresh}"`}</pre>
          <button className="btn btn-ghost btn-sm" onClick={() => setFresh(null)}>I’ve saved it</button>
        </div>
      )}
      <form className="inline-row" onSubmit={create}>
        <input value={name} onChange={e => setName(e.target.value)} placeholder="What is it for? e.g. deploy script" maxLength={60} aria-label="Token name" />
        <button className="btn btn-secondary btn-sm"><Plus size={14} /> Create token</button>
      </form>
      {tokens.length === 0 ? (
        <p className="muted small">You have no API tokens.</p>
      ) : (
        <div className="table-wrap">
          <table>
            <thead><tr><th>Name</th><th>Created</th><th>Last used</th><th></th></tr></thead>
            <tbody>
              {tokens.map(t => (
                <tr key={t.id}>
                  <td className="task-name">{t.name}</td>
                  <td title={formatClock(t.created_at)}>{timeAgo(t.created_at)}</td>
                  <td>{t.last_used_at ? timeAgo(t.last_used_at) : <span className="muted">never</span>}</td>
                  <td><div className="row-actions"><button className="btn btn-ghost btn-sm" onClick={() => revoke(t.id)}>Revoke</button></div></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  );
}

// ── team (admins) ────────────────────────────────────────────────────────────

function TeamSection({ me, notify }) {
  const [users, setUsers] = useState([]);
  const [invites, setInvites] = useState([]);
  const [inviteRole, setInviteRole] = useState('operator');
  const [link, setLink] = useState(null);
  const [confirmDelete, setConfirmDelete] = useState(null);

  const load = useCallback(() => {
    api('/auth/users').then(d => setUsers(d.users || [])).catch(() => {});
    api('/auth/invites').then(d => setInvites(d.invites || [])).catch(() => {});
  }, []);
  useEffect(() => { load(); }, [load]);

  const act = async (fn, okMsg) => {
    try { await fn(); if (okMsg) notify('success', okMsg); load(); }
    catch (err) { notify('error', err.message); }
  };

  const invite = () => act(async () => {
    const res = await api('/auth/invites', { method: 'POST', body: { role: inviteRole } });
    setLink({ url: `${window.location.origin}${res.path}`, role: res.invite.role });
  });

  return (
    <Section title="Team" description="Everyone who can sign in to this PyTaskQ server.">
      <div className="table-wrap">
        <table>
          <thead><tr><th>Person</th><th>Role</th><th>Last sign-in</th><th>Status</th><th></th></tr></thead>
          <tbody>
            {users.map(u => {
              const self = u.username === me.username;
              return (
                <tr key={u.username}>
                  <td>
                    <div className="task-name">{u.username}{self && <span className="muted"> (you)</span>}</div>
                    <div className="muted small">{u.created_by && u.created_by !== 'setup' ? `invited by ${u.created_by}` : 'joined'} {timeAgo(u.created_at)}</div>
                  </td>
                  <td>
                    <select value={u.role} aria-label={`Role for ${u.username}`}
                      onChange={e => act(() => api(`/auth/users/${u.username}`, { method: 'PATCH', body: { role: e.target.value } }),
                        `${u.username} is now ${ROLE_LABEL[e.target.value]}.`)}>
                      {ROLES.map(r => <option key={r} value={r}>{ROLE_LABEL[r]}</option>)}
                    </select>
                  </td>
                  <td>{u.last_login_at ? timeAgo(u.last_login_at) : <span className="muted">never</span>}</td>
                  <td>
                    {u.disabled
                      ? <span className="badge tone-muted">Disabled</span>
                      : <span className="badge tone-success">Active</span>}
                  </td>
                  <td>
                    <div className="row-actions">
                      {!self && (
                        <button className="btn btn-ghost btn-sm"
                          onClick={() => act(() => api(`/auth/users/${u.username}`, { method: 'PATCH', body: { disabled: !u.disabled } }),
                            u.disabled ? `${u.username} can sign in again.` : `${u.username} was signed out and can’t sign in.`)}>
                          {u.disabled ? 'Enable' : 'Disable'}
                        </button>
                      )}
                      {!self && (confirmDelete === u.username ? (
                        <>
                          <button className="btn btn-danger btn-sm" onClick={() => { setConfirmDelete(null); act(() => api(`/auth/users/${u.username}`, { method: 'DELETE' }), `${u.username} was removed.`); }}>Remove {u.username}</button>
                          <button className="btn btn-ghost btn-sm" onClick={() => setConfirmDelete(null)}>Keep</button>
                        </>
                      ) : (
                        <button className="btn btn-ghost btn-sm" aria-label={`Remove ${u.username}`} onClick={() => setConfirmDelete(u.username)}><Trash2 size={14} /></button>
                      ))}
                    </div>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <h3>Invite someone</h3>
      <div className="inline-row">
        <select value={inviteRole} onChange={e => setInviteRole(e.target.value)} aria-label="Role for the invite">
          {ROLES.map(r => <option key={r} value={r}>{ROLE_LABEL[r]}</option>)}
        </select>
        <button className="btn btn-secondary btn-sm" onClick={invite}><Link2 size={14} /> Create invite link</button>
        <span className="muted small">{ROLE_HELP[inviteRole]}.</span>
      </div>
      {link && (
        <div className="token-reveal" role="status">
          <p>Send this link to the person you’re inviting as <strong>{ROLE_LABEL[link.role]}</strong>. It works once and expires in 7 days.</p>
          <div className="token-row"><code>{link.url}</code><CopyButton text={link.url} label="Copy link" /></div>
        </div>
      )}
      {invites.length > 0 && (
        <div className="table-wrap">
          <table>
            <thead><tr><th>Pending invite</th><th>Created by</th><th>Expires</th><th></th></tr></thead>
            <tbody>
              {invites.map(i => (
                <tr key={i.id}>
                  <td>{ROLE_LABEL[i.role]}</td>
                  <td>{i.created_by}</td>
                  <td title={formatClock(i.expires_at)}>{inDays(i.expires_at)}</td>
                  <td><div className="row-actions">
                    <button className="btn btn-ghost btn-sm" onClick={() => act(() => api(`/auth/invites/${i.id}`, { method: 'DELETE' }), 'Invite revoked.')}>Revoke</button>
                  </div></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </Section>
  );
}

export default function SettingsView({ me, authEnabled, onSignOut, notify }) {
  if (!authEnabled) {
    return (
      <section className="panel settings-section">
        <div className="callout">
          <ShieldAlert size={18} />
          <div>
            <strong>Sign-in is turned off on this server.</strong>
            <p>Anyone who can open this page has full control. Remove <code>PYTASKQ_AUTH=off</code> from the server’s environment and restart to require accounts.</p>
          </div>
        </div>
      </section>
    );
  }
  return (
    <div className="settings">
      <AccountSection me={me} onSignOut={onSignOut} notify={notify} />
      <TokensSection me={me} notify={notify} />
      {can(me, 'admin') && <TeamSection me={me} notify={notify} />}
    </div>
  );
}
