import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Play, ListChecks, Server, Skull, Search, X,
  RotateCcw, Trash2, Ban, Clock, Cpu, Zap, Waves, Terminal, ChevronRight,
  Settings as SettingsIcon, UserRound,
} from 'lucide-react'
import {
  api, WS_URL, STATUS, FINAL, CANCELLABLE, timeAgo, formatDuration, formatClock,
  formatArgs, parseValue, prettyResult, storage, can, ROLE_LABEL, UNAUTHORIZED_EVENT,
} from './api'
import { Brand, CopyButton, ThemeButton } from './ui'
import SettingsView from './Settings'
import { InviteScreen, LoginScreen, SetupScreen } from './AuthScreens'

const TABS = [
  { id: 'jobs', label: 'Jobs', icon: ListChecks },
  { id: 'run', label: 'Run a task', icon: Play },
  { id: 'workers', label: 'Workers', icon: Server },
  { id: 'dead', label: 'Dead letters', icon: Skull },
  { id: 'settings', label: 'Settings', icon: SettingsIcon },
];

const EMPTY_METRICS = {
  pending: 0, processing: 0, delayed: 0, dlq: 0, completed_total: 0, failed_total: 0,
  queue_high: 0, queue_default: 0, queue_low: 0, queue_total: 0, queue_capacity: 500, worker_count: 0,
};

// ── small pieces ─────────────────────────────────────────────────────────────

function StatusBadge({ status }) {
  const s = STATUS[status] || { label: status || 'Unknown', tone: 'muted' };
  return <span className={`badge tone-${s.tone}`}>{s.label}</span>;
}

function TypeIcon({ type }) {
  if (type === 'cpu') return <Cpu size={14} aria-label="CPU task" />;
  if (type === 'async') return <Waves size={14} aria-label="Async task" />;
  return <Zap size={14} aria-label="I/O task" />;
}

function Toasts({ toasts, dismiss }) {
  return (
    <div className="toasts" role="status" aria-live="polite">
      {toasts.map(t => (
        <div key={t.id} className={`toast toast-${t.type}`}>
          <span>{t.msg}</span>
          <button className="icon-btn" onClick={() => dismiss(t.id)} aria-label="Dismiss"><X size={14} /></button>
        </div>
      ))}
    </div>
  );
}

// ── lifecycle strip: where every job is right now ────────────────────────────

function Lifecycle({ m }) {
  const pct = Math.min(100, (m.queue_total / (m.queue_capacity || 1)) * 100);
  const capTone = pct >= 90 ? 'danger' : pct >= 70 ? 'warning' : 'ok';
  return (
    <section className="lifecycle" aria-label="Job lifecycle">
      <div className="stage">
        <div className="stage-name"><Clock size={15} /> Scheduled</div>
        <div className="stage-count">{m.delayed}</div>
        <div className="stage-note">waiting for their start time or a retry</div>
      </div>
      <ChevronRight className="flow-arrow" aria-hidden="true" />
      <div className="stage">
        <div className="stage-name">Queued</div>
        <div className="stage-count">{m.queue_total}</div>
        <div className={`capacity cap-${capTone}`} title={`${m.queue_total} of ${m.queue_capacity} queue slots used`}>
          <div style={{ width: `${pct}%` }} />
        </div>
        <div className="stage-note split">
          <span>high {m.queue_high}</span><span>default {m.queue_default}</span><span>low {m.queue_low}</span>
        </div>
      </div>
      <ChevronRight className="flow-arrow" aria-hidden="true" />
      <div className="stage stage-running">
        <div className="stage-name"><span className={`pulse ${m.processing ? 'on' : ''}`} /> Running</div>
        <div className="stage-count">{m.processing}</div>
        <div className="stage-note">on {m.worker_count} worker{m.worker_count === 1 ? '' : 's'}</div>
      </div>
      <ChevronRight className="flow-arrow" aria-hidden="true" />
      <div className="stage stage-outcomes">
        <div className="outcome">
          <div className="stage-name">Succeeded</div>
          <div className="stage-count tone-text-success">{m.completed_total}</div>
        </div>
        <div className="outcome">
          <div className="stage-name">Dead letters</div>
          <div className={`stage-count ${m.dlq ? 'tone-text-danger' : ''}`}>{m.dlq}</div>
        </div>
      </div>
    </section>
  );
}

function GettingStarted() {
  return (
    <section className="callout">
      <Terminal size={18} />
      <div>
        <strong>No workers are running, so queued jobs will wait.</strong>
        <p>Start one in a terminal next to your project:</p>
        <pre>pytaskq worker --app your_module:queue</pre>
        <p className="muted">Try the built-in demo tasks with <code>pytaskq worker --app pytaskq.task_registery:queue</code>.</p>
      </div>
    </section>
  );
}

// ── Jobs ─────────────────────────────────────────────────────────────────────

const FILTERS = [
  { id: '', label: 'All' },
  { id: 'Queued', label: 'Queued' },
  { id: 'Scheduled', label: 'Scheduled' },
  { id: 'Processing', label: 'Running' },
  { id: 'RetryScheduled', label: 'Retrying' },
  { id: 'Success', label: 'Succeeded' },
  { id: 'DeadLetter', label: 'Dead letter' },
  { id: 'Cancelled', label: 'Cancelled' },
];

function JobsView({ tasks, filter, setFilter, nameFilter, setNameFilter, taskNames, onOpen, loading }) {
  const [lookup, setLookup] = useState('');
  return (
    <div className="panel">
      <div className="toolbar">
        <div className="chips" role="tablist" aria-label="Filter by status">
          {FILTERS.map(f => (
            <button key={f.id || 'all'} role="tab" aria-selected={filter === f.id}
              className={`chip ${filter === f.id ? 'active' : ''}`} onClick={() => setFilter(f.id)}>
              {f.label}
            </button>
          ))}
        </div>
        <div className="toolbar-right">
          <select value={nameFilter} onChange={e => setNameFilter(e.target.value)} aria-label="Filter by task">
            <option value="">All tasks</option>
            {taskNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
          <form className="lookup" onSubmit={e => { e.preventDefault(); if (lookup.trim()) onOpen(lookup.trim()); }}>
            <Search size={14} />
            <input value={lookup} onChange={e => setLookup(e.target.value)} placeholder="Find a job by ID" aria-label="Find a job by ID" />
          </form>
        </div>
      </div>

      {tasks.length === 0 ? (
        <div className="empty">
          {loading ? 'Loading jobs…' : filter || nameFilter
            ? 'No jobs match these filters.'
            : <>No jobs yet. Open <strong>Run a task</strong> to queue your first one.</>}
        </div>
      ) : (
        <div className="table-wrap">
          <table className="jobs">
            <thead>
              <tr><th>Status</th><th>Task</th><th>Arguments</th><th>Queued</th><th className="num">Duration</th><th>Priority</th></tr>
            </thead>
            <tbody>
              {tasks.map(t => (
                <tr key={t.task_id} onClick={() => onOpen(t.task_id)} tabIndex={0}
                  onKeyDown={e => { if (e.key === 'Enter') onOpen(t.task_id); }}>
                  <td><StatusBadge status={t.status} /></td>
                  <td>
                    <div className="task-name">{t.task_name}</div>
                    <div className="task-id">{t.task_id.slice(0, 8)}</div>
                  </td>
                  <td className="args">{formatArgs(t.args, t.kwargs) || <span className="muted">none</span>}</td>
                  <td title={formatClock(t.created_at)}>{timeAgo(t.created_at)}</td>
                  <td className="num">{formatDuration(t.duration_ms)}</td>
                  <td><span className={`prio prio-${t.priority}`}>{t.priority}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  );
}

function Field({ label, children }) {
  return (<><dt>{label}</dt><dd>{children}</dd></>);
}

function TaskDrawer({ taskId, onClose, notify, refreshKey, canOperate }) {
  const [task, setTask] = useState(null);
  const [error, setError] = useState('');
  const [busy, setBusy] = useState(false);

  const load = useCallback(() => {
    api(`/task/${encodeURIComponent(taskId)}`)
      .then(d => { setTask(d.result); setError(''); })
      .catch(e => { setTask(null); setError(e.message); });
  }, [taskId]);

  useEffect(() => { load(); }, [load, refreshKey]);
  useEffect(() => {
    const onKey = e => { if (e.key === 'Escape') onClose(); };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [onClose]);

  const cancel = async () => {
    setBusy(true);
    try { await api(`/task/${taskId}/cancel`, { method: 'POST' }); notify('success', 'Job cancelled.'); load(); }
    catch (e) { notify('error', e.message); }
    finally { setBusy(false); }
  };

  const result = task ? prettyResult(task.result) : '';
  return (
    <div className="drawer-backdrop" onClick={onClose}>
      <aside className="drawer" onClick={e => e.stopPropagation()} aria-label="Job details" role="dialog">
        <header className="drawer-head">
          <div>
            <h2>{task?.task_name || 'Job details'}</h2>
            <code className="task-id">{taskId}</code>
          </div>
          <button className="icon-btn" onClick={onClose} aria-label="Close"><X size={18} /></button>
        </header>

        {error && <div className="notice notice-error">{error}</div>}
        {task && (
          <>
            <div className="drawer-status">
              <StatusBadge status={task.status} />
              {!FINAL.has(task.status) && task.status !== 'Processing' && (
                <span className="muted">Updates live</span>
              )}
            </div>
            <dl className="facts">
              <Field label="Priority"><span className={`prio prio-${task.priority}`}>{task.priority || 'default'}</span></Field>
              <Field label="Queued">{formatClock(task.created_at)}</Field>
              {task.scheduled_for && <Field label="Runs at">{formatClock(task.scheduled_for)}</Field>}
              {task.started_at && <Field label="Started">{formatClock(task.started_at)}</Field>}
              {task.finished_at && <Field label="Finished">{formatClock(task.finished_at)}</Field>}
              {task.duration_ms !== undefined && <Field label="Duration">{formatDuration(task.duration_ms)}</Field>}
              <Field label="Attempts">{(task.retry_count || 0) + 1}</Field>
              {task.worker_id && <Field label="Worker"><code>{task.worker_id}</code></Field>}
              {task.created_by && <Field label="Queued by">{task.created_by}</Field>}
              {task.cancelled_by && <Field label="Cancelled by">{task.cancelled_by}</Field>}
              {task.note && <Field label="Note">{task.note}</Field>}
            </dl>

            <h3>Arguments</h3>
            <pre className="code">{JSON.stringify({ args: task.args ?? [], kwargs: task.kwargs ?? {} }, null, 2)}</pre>

            {task.error && (<><h3>Error</h3><pre className="code code-error">{task.error}</pre></>)}
            {result && (<><h3>Result</h3><pre className="code">{result}</pre></>)}

            <div className="drawer-actions">
              <CopyButton text={taskId} />
              {canOperate && CANCELLABLE.has(task.status) && (
                <button className="btn btn-danger btn-sm" onClick={cancel} disabled={busy}>
                  <Ban size={14} /> Cancel job
                </button>
              )}
            </div>
          </>
        )}
      </aside>
    </div>
  );
}

// ── Run a task ───────────────────────────────────────────────────────────────

function paramPlaceholder(p) {
  if ('default' in p) return JSON.stringify(p.default);
  if (p.annotation === 'int' || p.annotation === 'float') return '42';
  if (p.annotation === 'str') return 'text';
  if (p.annotation === 'list') return '[1, 2, 3]';
  if (p.annotation === 'dict') return '{"key": "value"}';
  return 'value or JSON';
}

function RunView({ registered, onQueued, notify }) {
  const visible = registered.filter(t => !t.internal);
  const [picked, setName] = useState('');
  const [values, setValues] = useState({});
  const [manualArgs, setManualArgs] = useState('[]');
  const [priority, setPriority] = useState('');
  const [delay, setDelay] = useState('');
  const [webhook, setWebhook] = useState('');
  const [busy, setBusy] = useState(false);

  // Default to the first available task until the person picks one
  const name = picked || visible[0]?.name || '';

  const spec = visible.find(t => t.name === name);
  const params = (spec?.params || []).filter(p => !p.kind);

  const submit = async e => {
    e.preventDefault();
    let args = [];
    const kwargs = {};
    if (spec) {
      for (const p of params) {
        const v = parseValue(values[p.name]);
        if (v === undefined) {
          if (p.required) { notify('error', `“${p.name}” is required.`); return; }
          continue;
        }
        kwargs[p.name] = v;
      }
    } else {
      try { args = JSON.parse(manualArgs || '[]'); } catch { notify('error', 'Arguments must be a JSON list, e.g. [1, "a"].'); return; }
      if (!Array.isArray(args)) { notify('error', 'Arguments must be a JSON list, e.g. [1, "a"].'); return; }
    }
    setBusy(true);
    try {
      const body = { task_name: name, args, kwargs, delay_seconds: Number(delay) || 0, on_success_url: webhook || null };
      if (priority) body.priority = priority;
      const res = await api('/task/enqueue', { method: 'POST', body });
      notify('success', `${name} ${res.status === 'scheduled' ? 'scheduled' : 'queued'}.`, res.task_id);
      onQueued(res.task_id);
    } catch (err) {
      notify('error', err.message);
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="run-grid">
      <div className="panel task-list" aria-label="Available tasks">
        <h2>Tasks</h2>
        {visible.length === 0 ? (
          <p className="muted small">
            No tasks found yet. Workers announce their tasks when they start, or start the dashboard with
            <code> --app your_module:queue</code>. You can still type a task name.
          </p>
        ) : (
          <ul>
            {visible.map(t => (
              <li key={t.name}>
                <button className={`task-pick ${t.name === name ? 'active' : ''}`} onClick={() => { setName(t.name); setValues({}); }}>
                  <span className="task-pick-name"><TypeIcon type={t.type} /> {t.name}</span>
                  {t.doc && <span className="task-pick-doc">{t.doc}</span>}
                  {t.cron && <span className="task-pick-cron"><Clock size={12} /> {t.cron}</span>}
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <form className="panel run-form" onSubmit={submit}>
        {spec ? (
          <>
            <h2>{spec.name}</h2>
            <p className="muted small">
              {spec.type === 'cpu' ? 'Runs in a separate process' : spec.type === 'async' ? 'Runs on the worker event loop' : 'Runs in a worker thread'}
              {' '}· retried up to {spec.max_retries}× · times out after {spec.timeout ? `${spec.timeout}s` : 'never'}
            </p>
            {params.length === 0 && <p className="muted">This task takes no arguments.</p>}
            {params.map(p => (
              <label key={p.name} className="field">
                <span>{p.name}{p.required && <em className="req" aria-label="required">*</em>}{p.annotation && <small> {p.annotation}</small>}</span>
                <input value={values[p.name] ?? ''} placeholder={paramPlaceholder(p)}
                  onChange={e => setValues(v => ({ ...v, [p.name]: e.target.value }))} />
              </label>
            ))}
          </>
        ) : (
          <>
            <h2>Run a task</h2>
            <label className="field"><span>Task name<em className="req">*</em></span>
              <input value={name} onChange={e => setName(e.target.value)} placeholder="send_welcome_email" required />
            </label>
            <label className="field"><span>Arguments <small>JSON list</small></span>
              <input value={manualArgs} onChange={e => setManualArgs(e.target.value)} placeholder='[42, "hello"]' />
            </label>
          </>
        )}

        <div className="field">
          <span>Priority</span>
          <div className="segmented" role="radiogroup" aria-label="Priority">
            {[['', `Task default${spec && spec.priority !== 'default' ? ` (${spec.priority})` : ''}`], ['high', 'High'], ['default', 'Normal'], ['low', 'Low']].map(([v, l]) => (
              <button type="button" key={v || 'auto'} role="radio" aria-checked={priority === v}
                className={priority === v ? 'active' : ''} onClick={() => setPriority(v)}>{l}</button>
            ))}
          </div>
        </div>
        <label className="field"><span>Start after <small>seconds, leave empty to start now</small></span>
          <input type="number" min="0" value={delay} onChange={e => setDelay(e.target.value)} placeholder="0" />
        </label>
        <details className="advanced">
          <summary>Notify a URL when it succeeds</summary>
          <label className="field"><span>Webhook URL</span>
            <input type="url" value={webhook} onChange={e => setWebhook(e.target.value)} placeholder="https://example.com/hooks/job-done" />
          </label>
        </details>
        <button className="btn btn-primary" type="submit" disabled={busy || !name}>
          <Play size={16} /> {Number(delay) > 0 ? 'Schedule job' : 'Queue job'}
        </button>
      </form>
    </div>
  );
}

// ── Workers ──────────────────────────────────────────────────────────────────

function uptime(s) {
  if (s === null || s === undefined) return '—';
  if (s < 60) return `${s}s`;
  if (s < 3600) return `${Math.floor(s / 60)}m`;
  if (s < 86400) return `${Math.floor(s / 3600)}h ${Math.floor((s % 3600) / 60)}m`;
  return `${Math.floor(s / 86400)}d ${Math.floor((s % 86400) / 3600)}h`;
}

function WorkersView({ workers }) {
  if (workers.length === 0) return <div className="panel"><GettingStarted /></div>;
  return (
    <div className="panel">
      <div className="table-wrap">
        <table>
          <thead><tr><th>Worker</th><th>App</th><th className="num">Running</th><th className="num">Done</th><th className="num">Failed</th><th>Pools</th><th>Up</th><th>Health</th></tr></thead>
          <tbody>
            {workers.map(w => (
              <tr key={w.worker_id}>
                <td><div className="task-name">{w.hostname || 'worker'}{w.pid ? ` · pid ${w.pid}` : ''}</div><div className="task-id">{w.worker_id}</div></td>
                <td>{w.app || <span className="muted">—</span>}</td>
                <td className="num">{w.active}</td>
                <td className="num">{w.processed}</td>
                <td className="num">{w.failed}</td>
                <td className="small">{w.cpu_workers ?? '?'} proc · {w.io_workers ?? '?'} threads</td>
                <td>{uptime(w.uptime_seconds)}</td>
                <td>
                  <span className={`badge tone-${w.status === 'healthy' ? 'success' : w.status === 'degraded' ? 'warning' : 'danger'}`}>
                    {w.status === 'healthy' ? 'Healthy' : w.status === 'degraded' ? 'Slow heartbeat' : 'Unresponsive'}
                  </span>
                  <div className="muted small">{w.last_heartbeat_seconds_ago}s ago</div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── Dead letters ─────────────────────────────────────────────────────────────

function DeadLettersView({ items, notify, onOpen, canOperate }) {
  const [confirmPurge, setConfirmPurge] = useState(false);
  const act = async (path, okMsg) => {
    try { const r = await api(path, { method: 'POST' }); notify('success', okMsg(r)); }
    catch (e) { notify('error', e.message); }
  };

  if (items.length === 0) {
    return <div className="panel"><div className="empty">No dead letters. Jobs that fail every retry land here so you can inspect and replay them.</div></div>;
  }
  return (
    <div className="panel">
      <div className="toolbar">
        <p className="muted small">These jobs failed every retry. Fix the cause, then replay them.</p>
        {canOperate && <div className="toolbar-right">
          <button className="btn btn-secondary btn-sm" onClick={() => act('/dlq/retry_all', r => `Replaying ${r.retried} job${r.retried === 1 ? '' : 's'}.`)}>
            <RotateCcw size={14} /> Replay all
          </button>
          {confirmPurge ? (
            <>
              <button className="btn btn-danger btn-sm" onClick={() => { setConfirmPurge(false); act('/dlq/purge_all', () => 'Dead letters deleted.'); }}>
                Delete {items.length}
              </button>
              <button className="btn btn-ghost btn-sm" onClick={() => setConfirmPurge(false)}>Keep them</button>
            </>
          ) : (
            <button className="btn btn-ghost btn-sm" onClick={() => setConfirmPurge(true)}><Trash2 size={14} /> Delete all</button>
          )}
        </div>}
      </div>
      <div className="table-wrap">
        <table>
          <thead><tr><th>Task</th><th>Last error</th><th>Failed</th><th className="num">Attempts</th><th></th></tr></thead>
          <tbody>
            {items.map(t => (
              <tr key={t.task_id}>
                <td>
                  <button className="link" onClick={() => onOpen(t.task_id)}>{t.task_name}</button>
                  <div className="task-id">{t.task_id.slice(0, 8)}</div>
                </td>
                <td className="error-cell">{t.error || <span className="muted">not recorded</span>}</td>
                <td>{timeAgo(t.failed_at)}</td>
                <td className="num">{(t.retry_count || 0) + 1}</td>
                <td>
                  {canOperate && <div className="row-actions">
                    <button className="btn btn-secondary btn-sm" onClick={() => act(`/dlq/replay/${t.task_id}`, () => `${t.task_name} replayed.`)}><RotateCcw size={14} /> Replay</button>
                    <button className="btn btn-ghost btn-sm" onClick={() => act(`/dlq/purge/${t.task_id}`, () => 'Dead letter deleted.')} aria-label={`Delete ${t.task_name}`}><Trash2 size={14} /></button>
                  </div>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

// ── App shell ────────────────────────────────────────────────────────────────

function useHashTab(tabs) {
  // Keep the raw hash; which tabs are allowed depends on the user's role.
  const [hash, setHash] = useState(() => window.location.hash.replace('#', ''));
  useEffect(() => {
    const on = () => setHash(window.location.hash.replace('#', ''));
    window.addEventListener('hashchange', on);
    return () => window.removeEventListener('hashchange', on);
  }, []);
  const go = id => { window.location.hash = id; setHash(id); };
  return [tabs.some(t => t.id === hash) ? hash : 'jobs', go];
}

function Dashboard({ me, authEnabled, onSignOut, theme, setTheme }) {
  const canOperate = can(me, 'operator');
  const tabs = useMemo(() => TABS.filter(t => t.id !== 'run' || canOperate), [canOperate]);
  const [tab, setTab] = useHashTab(tabs);
  const [metrics, setMetrics] = useState(EMPTY_METRICS);
  const [dlq, setDlq] = useState([]);
  const [connected, setConnected] = useState(false);
  const [tick, setTick] = useState(0);          // bumps on every live update
  const [tasks, setTasks] = useState([]);
  const [loadingTasks, setLoadingTasks] = useState(true);
  const [filter, setFilter] = useState('');
  const [nameFilter, setNameFilter] = useState('');
  const [registered, setRegistered] = useState([]);
  const [workers, setWorkers] = useState([]);
  const [openId, setOpenId] = useState(null);
  const [toasts, setToasts] = useState([]);

  const notify = useCallback((type, msg, taskId) => {
    const id = Math.random().toString(36).slice(2);
    setToasts(t => [...t.slice(-3), { id, type, msg, taskId }]);
    setTimeout(() => setToasts(t => t.filter(x => x.id !== id)), type === 'error' ? 7000 : 3500);
  }, []);

  // Live updates over WebSocket (falls back to polling while disconnected)
  useEffect(() => {
    let ws, retry, alive = true;
    const connect = () => {
      ws = new WebSocket(WS_URL);
      ws.onopen = () => setConnected(true);
      ws.onmessage = ev => {
        const data = JSON.parse(ev.data);
        if (data.metrics) setMetrics(data.metrics);
        if (data.dlq) setDlq(data.dlq);
        setTick(t => t + 1);
      };
      ws.onclose = ev => {
        setConnected(false);
        if (ev.code === 4401) { window.dispatchEvent(new Event(UNAUTHORIZED_EVENT)); return; }
        if (alive) retry = setTimeout(connect, 3000);
      };
      ws.onerror = () => ws.close();
    };
    connect();
    return () => { alive = false; clearTimeout(retry); if (ws) ws.close(); };
  }, []);

  useEffect(() => {
    if (connected) return;
    const poll = () => {
      api('/metrics').then(setMetrics).catch(() => {});
      api('/dlq').then(d => setDlq(d.tasks || [])).catch(() => {});
      setTick(t => t + 1);
    };
    const iv = setInterval(poll, 5000);
    return () => clearInterval(iv);
  }, [connected]);

  // Job history follows live updates, throttled to at most ~1 request/second
  const lastFetch = useRef(0);
  const pending = useRef(null);
  useEffect(() => {
    const run = () => {
      lastFetch.current = Date.now();
      const q = new URLSearchParams({ limit: '100' });
      if (filter) q.set('status', filter);
      if (nameFilter) q.set('task_name', nameFilter);
      api(`/tasks?${q}`).then(d => setTasks(d.tasks || [])).catch(() => {}).finally(() => setLoadingTasks(false));
    };
    const wait = Math.max(0, 1000 - (Date.now() - lastFetch.current));
    clearTimeout(pending.current);
    pending.current = setTimeout(run, wait);
    return () => clearTimeout(pending.current);
  }, [tick, filter, nameFilter]);

  useEffect(() => {
    const load = () => {
      api('/tasks/registered').then(d => setRegistered(d.tasks || [])).catch(() => {});
      api('/system/workers').then(d => setWorkers(d.workers || [])).catch(() => {});
    };
    load();
    const iv = setInterval(load, 5000);
    return () => clearInterval(iv);
  }, []);

  const taskNames = useMemo(() => {
    const s = new Set(registered.filter(t => !t.internal).map(t => t.name));
    tasks.forEach(t => s.add(t.task_name));
    return [...s].sort();
  }, [registered, tasks]);

  const workerCount = Math.max(metrics.worker_count || 0, workers.length);

  return (
    <div className="shell">
      <header className="topbar">
        <Brand />
        <div className="topbar-right">
          <span className={`conn ${connected ? 'live' : ''}`}>
            <span className="dot" /> {connected ? 'Live' : 'Reconnecting…'}
          </span>
          <button className="user-chip" onClick={() => setTab('settings')} title="Account settings">
            <UserRound size={14} />
            <span>{authEnabled ? me.username : 'Sign-in off'}</span>
            {authEnabled && <span className="role">{ROLE_LABEL[me.role]}</span>}
          </button>
          <ThemeButton theme={theme} setTheme={setTheme} />
        </div>
      </header>

      <Lifecycle m={{ ...metrics, worker_count: workerCount }} />
      {workerCount === 0 && tab !== 'workers' && <GettingStarted />}

      <nav className="tabs" aria-label="Sections">
        {tabs.map(t => {
          const Icon = t.icon;
          const count = t.id === 'dead' ? dlq.length : t.id === 'workers' ? workerCount : null;
          return (
            <a key={t.id} href={`#${t.id}`} className={`tab ${tab === t.id ? 'active' : ''}`}
              aria-current={tab === t.id ? 'page' : undefined} onClick={e => { e.preventDefault(); setTab(t.id); }}>
              <Icon size={16} /> {t.label}
              {count ? <span className={`count ${t.id === 'dead' ? 'count-danger' : ''}`}>{count}</span> : null}
            </a>
          );
        })}
      </nav>

      <main>
        {tab === 'jobs' && (
          <JobsView tasks={tasks} filter={filter} setFilter={setFilter} nameFilter={nameFilter}
            setNameFilter={setNameFilter} taskNames={taskNames} onOpen={setOpenId} loading={loadingTasks} />
        )}
        {tab === 'run' && (
          <RunView registered={registered} notify={notify} onQueued={() => {}} />
        )}
        {tab === 'workers' && <WorkersView workers={workers} />}
        {tab === 'dead' && <DeadLettersView items={dlq} notify={notify} onOpen={setOpenId} canOperate={canOperate} />}
        {tab === 'settings' && <SettingsView me={me} authEnabled={authEnabled} onSignOut={onSignOut} notify={notify} />}
      </main>

      {openId && <TaskDrawer taskId={openId} onClose={() => setOpenId(null)} notify={notify} refreshKey={tick} canOperate={canOperate} />}
      <Toasts
        toasts={toasts.map(t => ({
          ...t,
          msg: t.taskId
            ? <>{t.msg} <button className="link" onClick={() => { setOpenId(t.taskId); setTab('jobs'); }}>View job</button></>
            : t.msg,
        }))}
        dismiss={id => setToasts(t => t.filter(x => x.id !== id))}
      />
    </div>
  );
}

// ── Root: decides between setup, sign-in, invite and the dashboard ────────────

function readInviteToken() {
  try { return new URLSearchParams(window.location.search).get('invite'); } catch { return null; }
}

function clearInviteFromUrl() {
  const url = new URL(window.location.href);
  url.searchParams.delete('invite');
  window.history.replaceState(null, '', url.pathname + url.search + url.hash);
}

export default function App() {
  const [theme, setTheme] = useState(() => storage.get('theme', 'dark'));
  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    storage.set('theme', theme);
  }, [theme]);

  // status: null while loading, then the /auth/status response
  const [status, setStatus] = useState(null);
  const [error, setError] = useState('');
  const [invite, setInvite] = useState(readInviteToken);
  const [notice, setNotice] = useState('');

  const refresh = useCallback(() => {
    api('/auth/status').then(s => { setStatus(s); setError(''); })
      .catch(e => setError(`Can’t reach the PyTaskQ server: ${e.message}`));
  }, []);
  useEffect(() => { refresh(); }, [refresh]);

  // Any 401 from the API (expired or revoked session) sends you back to sign-in
  useEffect(() => {
    const onUnauthorized = () => {
      setNotice('Your session ended. Please sign in again.');
      setStatus(s => (s && s.auth_enabled ? { ...s, user: null } : s));
    };
    window.addEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
    return () => window.removeEventListener(UNAUTHORIZED_EVENT, onUnauthorized);
  }, []);

  const signedIn = user => {
    setNotice('');
    if (invite) { clearInviteFromUrl(); setInvite(null); }
    setStatus(s => ({ ...s, setup_required: false, user }));
  };
  const signOut = async () => {
    try { await api('/auth/logout', { method: 'POST' }); } catch { /* already gone */ }
    setNotice('');
    setStatus(s => ({ ...s, user: null }));
  };
  const screen = { theme, setTheme };

  if (error) {
    return (
      <div className="auth-page"><main className="auth-card">
        <h1>PyTaskQ</h1>
        <p className="notice notice-error">{error}</p>
        <button className="btn btn-secondary" onClick={refresh}>Try again</button>
      </main></div>
    );
  }
  if (!status) return <div className="auth-page"><p className="muted center">Loading…</p></div>;
  if (status.setup_required) return <SetupScreen onDone={signedIn} {...screen} />;
  if (!status.user && invite) {
    return <InviteScreen token={invite} onDone={signedIn} {...screen}
      onCancel={() => { clearInviteFromUrl(); setInvite(null); }} />;
  }
  if (!status.user) return <LoginScreen onDone={signedIn} notice={notice} {...screen} />;
  return <Dashboard me={status.user} authEnabled={status.auth_enabled} onSignOut={signOut} {...screen} />;
}
