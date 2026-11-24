import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import {
  Play, ListChecks, Server, Skull, Search, X,
  RotateCcw, Trash2, Ban, Clock, Cpu, Zap, Waves, Terminal, ChevronRight,
  Settings as SettingsIcon, UserRound, Pause, ArrowUp, AlertTriangle, Layers, History as HistoryIcon, Workflow,
} from 'lucide-react'
import {
  api, WS_URL, STATUS, WF_STATUS, FINAL, CANCELLABLE, timeAgo, formatDuration, formatClock,
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
  { id: 'workflows', label: 'Workflows', icon: Workflow },
  { id: 'history', label: 'History', icon: HistoryIcon },
  { id: 'settings', label: 'Settings', icon: SettingsIcon },
];

const EMPTY_METRICS = {
  pending: 0, processing: 0, delayed: 0, dlq: 0, completed_total: 0, failed_total: 0,
  queue_high: 0, queue_default: 0, queue_low: 0, queue_total: 0, queue_capacity: 500, worker_count: 0,
  queues: [], fullest_queue: { name: 'default', total: 0 },
};

// Queues that matter right now: they have jobs waiting or a worker serving them.
const queuesInUse = m => (m.queues || []).filter(q => q.total > 0 || q.workers > 0 || q.paused);
const showsQueues = m => {
  const used = queuesInUse(m);
  return used.length > 1 || (used.length === 1 && used[0].name !== 'default');
};

// ── small pieces ─────────────────────────────────────────────────────────────

function StatusBadge({ status }) {
  const s = STATUS[status] || { label: status || 'Unknown', tone: 'muted' };
  return <span className={`badge tone-${s.tone}`}>{s.label}</span>;
}

// A running job's own report of how far it got (pytaskq.progress()).
function JobProgress({ t, wide }) {
  if (t.status !== 'Processing' || t.progress === undefined || t.progress === null || t.progress === '') return null;
  const pct = Math.max(0, Math.min(100, Number(t.progress) || 0));
  return (
    <div className={`job-progress ${wide ? 'wide' : ''}`} title={t.progress_message || `${pct}%`}>
      <div className="capacity" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
        <div style={{ width: `${pct}%` }} />
      </div>
      <span className="small muted">{pct}%{wide && t.progress_message ? ` · ${t.progress_message}` : ''}</span>
    </div>
  );
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

function Lifecycle({ m, onAdvice }) {
  // Each queue has its own capacity: the bar shows the fullest one.
  const fullest = m.fullest_queue || { name: 'default', total: m.queue_total };
  const pct = Math.min(100, (fullest.total / (m.queue_capacity || 1)) * 100);
  const full = m.queue_capacity > 0 && fullest.total >= m.queue_capacity;
  const nearly = !full && pct >= 90;
  const which = showsQueues(m) ? `Queue '${fullest.name}'` : 'Queue';
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
        <div className={`capacity cap-${capTone}`}
          title={`Fullest queue '${fullest.name}': ${fullest.total} of ${m.queue_capacity} slots used`}>
          <div style={{ width: `${pct}%` }} />
        </div>
        {full || nearly ? (
          <div className="stage-note queue-full" role="status">
            {full ? `${which} full: new jobs are refused.` : `${which} nearly full.`}{' '}
            <button className="link" onClick={onAdvice}>What to change</button>
          </div>
        ) : (
          <div className="stage-note split">
            <span>high {m.queue_high}</span><span>default {m.queue_default}</span><span>low {m.queue_low}</span>
          </div>
        )}
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

function QueueStrip({ m, active, onPick, canOperate, notify }) {
  const queues = queuesInUse(m);
  const [busy, setBusy] = useState('');
  const toggle = async q => {
    setBusy(q.name);
    try {
      await api(`/queues/${encodeURIComponent(q.name)}/${q.paused ? 'resume' : 'pause'}`, { method: 'POST' });
      notify('success', q.paused ? `Queue '${q.name}' resumed.`
        : `Queue '${q.name}' paused. Running jobs finish; new jobs wait until you resume it.`);
    } catch (e) { notify('error', e.message); }
    finally { setBusy(''); }
  };
  return (
    <section className="queue-strip" aria-label="Queues">
      <span className="queue-strip-label">Queues</span>
      {queues.map(q => {
        const stranded = q.total > 0 && q.workers === 0 && !q.paused;
        return (
          <div key={q.name} className={`queue-chip-wrap ${q.paused ? 'paused' : ''}`}>
            <button className={`queue-chip ${stranded ? 'stranded' : ''} ${q.paused ? 'paused' : ''} ${active === q.name ? 'active' : ''}`}
              aria-pressed={active === q.name} onClick={() => onPick(active === q.name ? '' : q.name)}
              title={q.paused ? `Paused: workers don't start new jobs from '${q.name}'`
                : stranded ? 'Jobs are waiting but no running worker serves this queue' : `Show jobs on queue '${q.name}'`}>
              <span className="queue-chip-name">{q.name}</span>
              <span className="queue-chip-count">{q.total} waiting</span>
              <span className="queue-chip-workers">
                {q.paused ? <><Pause size={12} /> paused</>
                  : stranded ? <><AlertTriangle size={12} /> no worker</> : `${q.workers} worker${q.workers === 1 ? '' : 's'}`}
              </span>
            </button>
            {canOperate && (
              <button className="icon-btn queue-pause" onClick={() => toggle(q)} disabled={busy === q.name}
                aria-label={q.paused ? `Resume queue ${q.name}` : `Pause queue ${q.name}`}
                title={q.paused ? 'Resume: workers take jobs from this queue again' : 'Pause: stop starting new jobs from this queue'}>
                {q.paused ? <Play size={14} /> : <Pause size={14} />}
              </button>
            )}
          </div>
        );
      })}
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

// Consecutive jobs with the same task and status fold into one row, so a burst
// of identical requests can't push everything else off the page.
const GROUP_MIN = 3;

function groupRuns(tasks) {
  const out = [];
  for (const t of tasks) {
    const last = out[out.length - 1];
    if (last && last.task_name === t.task_name && last.status === t.status) last.items.push(t);
    else out.push({ task_name: t.task_name, status: t.status, items: [t] });
  }
  // Key by the oldest job: new arrivals join at the top without closing an open group
  for (const g of out) g.key = g.items[g.items.length - 1].task_id;
  return out;
}

function JobRow({ t, onOpen, nested }) {
  return (
    <tr className={nested ? 'nested' : undefined} onClick={() => onOpen(t.task_id)} tabIndex={0}
      onKeyDown={e => { if (e.key === 'Enter') onOpen(t.task_id); }}>
      <td><StatusBadge status={t.status} /></td>
      <td>
        <div className="task-name">{t.task_name}</div>
        <div className="task-id">{t.task_id.slice(0, 8)}</div>
      </td>
      <td className="args">{formatArgs(t.args, t.kwargs) || <span className="muted">none</span>}</td>
      <td title={formatClock(t.created_at)}>{timeAgo(t.created_at)}</td>
      <td className="num">{t.status === 'Processing' && t.progress !== undefined ? <JobProgress t={t} /> : formatDuration(t.duration_ms)}</td>
      <td><span className={`prio prio-${t.priority}`}>{t.priority}</span></td>
    </tr>
  );
}

function GroupRow({ g, open, toggle }) {
  const newest = g.items[0], oldest = g.items[g.items.length - 1];
  const timed = g.items.filter(t => t.duration_ms !== undefined && t.duration_ms !== null && t.duration_ms !== '');
  const avg = timed.length ? timed.reduce((a, t) => a + Number(t.duration_ms), 0) / timed.length : null;
  return (
    <tr className="group-row" onClick={toggle} tabIndex={0} aria-expanded={open}
      onKeyDown={e => { if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); toggle(); } }}>
      <td><StatusBadge status={g.status} /></td>
      <td>
        <div className="task-name group-name">
          <ChevronRight size={14} className={`caret ${open ? 'open' : ''}`} aria-hidden="true" />
          {g.task_name} <span className="times">×{g.items.length}</span>
        </div>
        <div className="task-id">{open ? 'hide these jobs' : 'show each job'}</div>
      </td>
      <td className="args">
        {formatArgs(newest.args, newest.kwargs) || <span className="muted">none</span>}
        <div className="task-id">and {g.items.length - 1} more like it</div>
      </td>
      <td title={`${formatClock(oldest.created_at)} – ${formatClock(newest.created_at)}`}>
        {timeAgo(newest.created_at)}
        <div className="task-id">oldest {timeAgo(oldest.created_at)}</div>
      </td>
      <td className="num">{avg === null ? '—' : <>{formatDuration(avg)}<div className="task-id">average</div></>}</td>
      <td><span className={`prio prio-${newest.priority}`}>{newest.priority}</span></td>
    </tr>
  );
}

function JobsView({ tasks, filter, setFilter, nameFilter, setNameFilter, queueFilter, setQueueFilter, queueNames,
  taskNames, onOpen, loading }) {
  const [lookup, setLookup] = useState('');
  const [grouped, setGrouped] = useState(() => storage.get('groupRepeats', true));
  const [paused, setPaused] = useState(false);
  const [hovering, setHovering] = useState(false);
  const [scrolled, setScrolled] = useState(false);
  const [frozen, setFrozen] = useState(null);       // the rows on screen while the list is held
  const [openGroups, setOpenGroups] = useState(() => new Set());
  const tableRef = useRef(null);

  useEffect(() => { storage.set('groupRepeats', grouped); }, [grouped]);

  // Hold the list still while you're reading it: pointer over it, scrolled
  // down into it, or paused. Held rows still update their status in place.
  const hold = () => setFrozen(f => f ?? tasks);
  const release = (still) => { if (!still) setFrozen(null); };
  useEffect(() => {
    const onScroll = () => {
      const down = (tableRef.current?.getBoundingClientRect().top ?? 0) < 0;
      setScrolled(down);
      if (down) setFrozen(f => f ?? tasks);
      else if (!paused && !hovering) setFrozen(null);
    };
    window.addEventListener('scroll', onScroll, { passive: true });
    return () => window.removeEventListener('scroll', onScroll);
  }, [tasks, paused, hovering]);
  const held = frozen !== null;

  const shown = useMemo(() => {
    if (!frozen) return tasks;
    const latest = new Map(tasks.map(t => [t.task_id, t]));
    return frozen.map(t => latest.get(t.task_id) || t);
  }, [frozen, tasks]);

  // A new filter is a new list: show it right away.
  const changeFilter = fn => v => { setFrozen(null); setPaused(false); setOpenGroups(new Set()); fn(v); };

  const newest = shown[0]?.created_at ?? 0;
  const shownIds = useMemo(() => new Set(shown.map(t => t.task_id)), [shown]);
  const waiting = held ? tasks.filter(t => !shownIds.has(t.task_id) && t.created_at >= newest).length : 0;
  const showNew = () => {
    setPaused(false); setFrozen(null);
    tableRef.current?.scrollIntoView({ block: 'start', behavior: 'smooth' });
  };

  const groups = useMemo(() => groupRuns(shown), [shown]);
  const toggleGroup = key => setOpenGroups(s => {
    const n = new Set(s);
    if (n.has(key)) n.delete(key); else n.add(key);
    return n;
  });

  return (
    <div className="panel">
      <div className="toolbar">
        <div className="chips" role="tablist" aria-label="Filter by status">
          {FILTERS.map(f => (
            <button key={f.id || 'all'} role="tab" aria-selected={filter === f.id}
              className={`chip ${filter === f.id ? 'active' : ''}`} onClick={() => changeFilter(setFilter)(f.id)}>
              {f.label}
            </button>
          ))}
        </div>
        <div className="toolbar-right">
          {(queueNames.length > 1 || queueFilter) && (
            <select value={queueFilter} onChange={e => changeFilter(setQueueFilter)(e.target.value)} aria-label="Filter by queue">
              <option value="">All queues</option>
              {queueNames.map(n => <option key={n} value={n}>{n}</option>)}
            </select>
          )}
          <select value={nameFilter} onChange={e => changeFilter(setNameFilter)(e.target.value)} aria-label="Filter by task">
            <option value="">All tasks</option>
            {taskNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
          <form className="lookup" onSubmit={e => { e.preventDefault(); if (lookup.trim()) onOpen(lookup.trim()); }}>
            <Search size={14} />
            <input value={lookup} onChange={e => setLookup(e.target.value)} placeholder="Find a job by ID" aria-label="Find a job by ID" />
          </form>
        </div>
      </div>

      <div className="list-bar">
        <label className="switch">
          <input type="checkbox" checked={grouped} onChange={e => setGrouped(e.target.checked)} />
          Group repeated jobs
        </label>
        <div className="list-bar-right">
          {waiting > 0 && (
            <button className="new-jobs" onClick={showNew}>
              <ArrowUp size={14} /> {waiting}{waiting >= 100 ? '+' : ''} new job{waiting === 1 ? '' : 's'}
            </button>
          )}
          <button className="btn btn-ghost btn-sm" onClick={() => (paused ? showNew() : (setPaused(true), hold()))}
            aria-pressed={paused} title={paused ? 'Show new jobs as they arrive' : 'Keep this list still'}>
            {paused ? <><Play size={13} /> Resume</> : <><Pause size={13} /> Pause</>}
          </button>
          <span className={`hold-state ${held ? 'held' : ''}`} aria-live="polite">
            {held ? (paused ? 'Paused' : 'Held while you read') : 'Live'}
          </span>
        </div>
      </div>

      {shown.length === 0 ? (
        <div className="empty">
          {loading ? 'Loading jobs…' : filter || nameFilter || queueFilter
            ? 'No jobs match these filters.'
            : <>No jobs yet. Open <strong>Run a task</strong> to queue your first one.</>}
        </div>
      ) : (
        <div className="table-wrap" ref={tableRef}
          onMouseEnter={() => { setHovering(true); hold(); }}
          onMouseLeave={() => { setHovering(false); release(paused || scrolled); }}
          onFocus={() => { setHovering(true); hold(); }}
          onBlur={e => { if (!e.currentTarget.contains(e.relatedTarget)) { setHovering(false); release(paused || scrolled); } }}>
          <table className="jobs">
            <thead>
              <tr><th>Status</th><th>Task</th><th>Arguments</th><th>Queued</th><th className="num">Duration</th><th>Priority</th></tr>
            </thead>
            <tbody>
              {grouped
                ? groups.map(g => (g.items.length < GROUP_MIN
                  ? g.items.map(t => <JobRow key={t.task_id} t={t} onOpen={onOpen} />)
                  : [
                    <GroupRow key={`g-${g.key}`} g={g} open={openGroups.has(g.key)} toggle={() => toggleGroup(g.key)} />,
                    ...(openGroups.has(g.key) ? g.items.map(t => <JobRow key={t.task_id} t={t} onOpen={onOpen} nested />) : []),
                  ]))
                : shown.map(t => <JobRow key={t.task_id} t={t} onOpen={onOpen} />)}
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
    try {
      const res = await api(`/task/${taskId}/cancel`, { method: 'POST' });
      notify('success', res?.stopping ? 'Stopping the job. Its worker stops it within a second or two.' : 'Job cancelled.');
      load();
    }
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
            <JobProgress t={task} wide />
            <dl className="facts">
              <Field label="Priority"><span className={`prio prio-${task.priority}`}>{task.priority || 'default'}</span></Field>
              <Field label="Queue">{task.queue || 'default'}</Field>
              <Field label="Queued">{formatClock(task.created_at)}</Field>
              {task.scheduled_for && <Field label="Runs at">{formatClock(task.scheduled_for)}</Field>}
              {task.started_at && <Field label="Started">{formatClock(task.started_at)}</Field>}
              {task.finished_at && <Field label="Finished">{formatClock(task.finished_at)}</Field>}
              {task.duration_ms !== undefined && <Field label="Duration">{formatDuration(task.duration_ms)}</Field>}
              <Field label="Attempts">{(task.retry_count || 0) + 1}</Field>
              {task.worker_id && <Field label="Worker"><code>{task.worker_id}</code></Field>}
              {task.created_by && <Field label="Queued by">{task.created_by}</Field>}
              {task.cancelled_by && <Field label="Cancelled by">{task.cancelled_by}</Field>}
              {task.waiting_for && !FINAL.has(task.status) && task.status !== 'Processing' && (
                <Field label="Waiting">{task.waiting_for}</Field>
              )}
              {task.note && <Field label="Note">{task.note}</Field>}
            </dl>

            <h3>Arguments</h3>
            <pre className="code">{JSON.stringify({ args: task.args ?? [], kwargs: task.kwargs ?? {} }, null, 2)}</pre>

            {task.error && (<><h3>Error</h3><pre className="code code-error">{task.error}</pre></>)}
            {result && (<><h3>Result</h3><pre className="code">{result}</pre></>)}
            {task.result_dropped && (<><h3>Result</h3><p className="callout-inline"><AlertTriangle size={14} /> {task.result_dropped}</p></>)}

            <div className="drawer-actions">
              <CopyButton text={taskId} />
              {canOperate && CANCELLABLE.has(task.status) && (
                <button className="btn btn-danger btn-sm" onClick={cancel} disabled={busy || !!task.cancel_requested_at}>
                  <Ban size={14} /> {task.status === 'Processing' ? (task.cancel_requested_at ? 'Stopping…' : 'Stop job') : 'Cancel job'}
                </button>
              )}
            </div>
          </>
        )}
      </aside>
    </div>
  );
}

// ── Workflows: chains and groups of jobs ─────────────────────────────────────

function WfBadge({ status }) {
  const s = WF_STATUS[status] || STATUS[status] || { label: status || 'Unknown', tone: 'muted' };
  return <span className={`badge tone-${s.tone}`}>{s.label}</span>;
}

function WorkflowsView({ onOpen, notify, canOperate, refreshKey }) {
  const [items, setItems] = useState(null);
  const [openWf, setOpenWf] = useState(null);
  useEffect(() => {
    let live = true;
    api('/workflows?limit=100').then(d => { if (live) setItems(d.workflows || []); }).catch(() => live && setItems([]));
    return () => { live = false; };
  }, [refreshKey]);

  if (items === null) return <div className="panel"><div className="empty">Loading workflows…</div></div>;
  if (!items.length) {
    return (
      <div className="panel">
        <div className="empty wf-empty">
          <p>No workflows yet. A workflow runs jobs one after another, or several at once, and passes results on:</p>
          <pre>{`from pytaskq import chain, group

await app.workflow(chain(
    download.s(url),
    group(resize.s(size=64), resize.s(size=512)),
    publish.s(),
), name="Thumbnails")`}</pre>
        </div>
      </div>
    );
  }
  return (
    <div className="panel">
      <div className="table-wrap">
        <table className="jobs">
          <thead><tr><th>Status</th><th>Workflow</th><th>Steps</th><th>Started</th><th className="num">Took</th></tr></thead>
          <tbody>
            {items.map(w => {
              const pct = w.total_steps ? Math.round(100 * (w.done_steps || 0) / w.total_steps) : 0;
              const took = w.finished_at && w.created_at ? (w.finished_at - w.created_at) * 1000 : null;
              return (
                <tr key={w.workflow_id} onClick={() => setOpenWf(w.workflow_id)} tabIndex={0}
                  onKeyDown={e => { if (e.key === 'Enter') setOpenWf(w.workflow_id); }}>
                  <td><WfBadge status={w.status} /></td>
                  <td>
                    <div className="task-name">{w.name}</div>
                    <div className="task-id">{w.workflow_id}</div>
                  </td>
                  <td>
                    <div className="job-progress wide wf-steps">
                      <div className="capacity" role="progressbar" aria-valuenow={pct} aria-valuemin={0} aria-valuemax={100}>
                        <div style={{ width: `${pct}%` }} />
                      </div>
                      <span className="small muted">
                        {w.done_steps || 0} of {w.total_steps} done
                        {w.failed_steps ? ` · ${w.failed_steps} failed` : ''}
                        {w.status === 'Running' ? ` · stage ${(w.stage || 0) + 1} of ${w.stages}` : ''}
                      </span>
                    </div>
                  </td>
                  <td title={formatClock(w.created_at)}>{formatClock(w.created_at)}</td>
                  <td className="num">{took === null ? '' : formatDuration(took)}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {openWf && (
        <WorkflowDetail id={openWf} onClose={() => setOpenWf(null)} onOpenJob={onOpen} notify={notify}
          canOperate={canOperate} refreshKey={refreshKey} />
      )}
    </div>
  );
}

function WorkflowDetail({ id, onClose, onOpenJob, notify, canOperate, refreshKey }) {
  const [wf, setWf] = useState(null);
  const [busy, setBusy] = useState(false);
  const load = useCallback(() => api(`/workflows/${encodeURIComponent(id)}`).then(setWf).catch(e => notify('error', e.message)),
    [id, notify]);
  useEffect(() => { load(); }, [load, refreshKey]);
  const act = async (path, done) => {
    setBusy(true);
    try { const res = await api(path, { method: 'POST' }); notify('success', done(res)); load(); }
    catch (e) { notify('error', e.message); }
    finally { setBusy(false); }
  };
  if (!wf) return null;
  const hasFailed = wf.stages.some(st => st.steps.some(s => s.status === 'DeadLetter' || s.status === 'Failed'));
  // A Failed workflow waits for its failed steps (retry) or can be cancelled; with on_failure="continue"
  // it went on without them, so there is nothing to retry.
  return (
    <section className="wf-detail" aria-label={`Workflow ${wf.name}`}>
      <header className="wf-head">
        <div>
          <h3>{wf.name} <WfBadge status={wf.status} /></h3>
          <p className="small muted">{wf.workflow_id} · {wf.done_steps} of {wf.total_steps} steps done
            {wf.on_failure === 'continue' ? ' · carries on past failed steps' : ''}</p>
          {wf.reason && <p className="wf-reason">{wf.reason}</p>}
        </div>
        <div className="wf-actions">
          {canOperate && (wf.status === 'Running' || wf.status === 'Failed') && (
            <button className="btn btn-danger btn-sm" disabled={busy}
              onClick={() => act(`/workflows/${encodeURIComponent(id)}/cancel`, () => 'Workflow cancelled.')}>
              <Ban size={14} /> Cancel workflow
            </button>
          )}
          {canOperate && hasFailed && wf.status === 'Failed' && (
            <button className="btn btn-secondary btn-sm" disabled={busy}
              onClick={() => act(`/workflows/${encodeURIComponent(id)}/retry`,
                r => `Retrying ${r.retried} failed step${r.retried === 1 ? '' : 's'}. The workflow carries on when ${r.retried === 1 ? 'it succeeds' : 'they succeed'}.`)}>
              <RotateCcw size={14} /> Retry failed steps
            </button>
          )}
          <button className="btn btn-ghost btn-sm" onClick={onClose}>Close</button>
        </div>
      </header>
      <ol className="wf-stages">
        {wf.stages.map(st => (
          <li key={st.stage} className={`wf-stage ${st.stage === wf.stage && wf.status === 'Running' ? 'current' : ''}`}>
            <span className="wf-stage-label">{st.group ? `Stage ${st.stage + 1} · ${st.steps.length} at once` : `Stage ${st.stage + 1}`}</span>
            {st.steps.map(step => {
              const ran = !['Pending', 'Skipped', 'Gone'].includes(step.status);
              return (
                <button key={step.step_id} className="wf-step" disabled={!ran}
                  onClick={() => ran && onOpenJob(step.step_id)}
                  title={step.error || (ran ? 'Open this job' : 'Hasn\'t run')}>
                  <span className="wf-step-name">{step.task}</span>
                  <WfBadge status={step.status} />
                </button>
              );
            })}
          </li>
        ))}
      </ol>
    </section>
  );
}

// ── History: search every kept finished job ──────────────────────────────────

const HISTORY_STATUSES = [
  { id: '', label: 'Any outcome' },
  { id: 'Success', label: 'Succeeded' },
  { id: 'DeadLetter', label: 'Dead letter' },
  { id: 'Cancelled', label: 'Cancelled' },
];

const toUnix = local => (local ? new Date(local).getTime() / 1000 : null);

function HistoryView({ taskNames, queueNames, onOpen }) {
  const [form, setForm] = useState({ q: '', status: '', task_name: '', queue: '', since: '', until: '' });
  const [rows, setRows] = useState([]);
  const [page, setPage] = useState(null);      // { next, scanned, kept } of the last request
  const [scanned, setScanned] = useState(0);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const [searched, setSearched] = useState(false);
  const set = key => e => setForm(f => ({ ...f, [key]: e.target.value }));

  const run = async (cursor = null) => {
    setBusy(true); setError('');
    const params = new URLSearchParams({ limit: '50' });
    for (const k of ['q', 'status', 'task_name', 'queue']) if (form[k]) params.set(k, form[k]);
    if (form.since) params.set('since', String(toUnix(form.since)));
    if (form.until) params.set('until', String(toUnix(form.until)));
    if (cursor) params.set('cursor', cursor);
    try {
      const d = await api(`/tasks/history?${params}`);
      setRows(r => (cursor ? [...r, ...d.tasks] : d.tasks));
      setScanned(s => (cursor ? s + d.scanned : d.scanned));
      setPage(d);
      setSearched(true);
    } catch (e) { setError(e.message); }
    finally { setBusy(false); }
  };

  return (
    <div className="panel">
      <form className="history-form" onSubmit={e => { e.preventDefault(); run(); }}>
        <label className="lookup history-q">
          <Search size={14} />
          <input value={form.q} onChange={set('q')} placeholder="Text in the ID, arguments, result or error"
            aria-label="Search text" />
        </label>
        <select value={form.status} onChange={set('status')} aria-label="Outcome">
          {HISTORY_STATUSES.map(s => <option key={s.id} value={s.id}>{s.label}</option>)}
        </select>
        <select value={form.task_name} onChange={set('task_name')} aria-label="Task">
          <option value="">All tasks</option>
          {taskNames.map(n => <option key={n} value={n}>{n}</option>)}
        </select>
        {queueNames.length > 1 && (
          <select value={form.queue} onChange={set('queue')} aria-label="Queue">
            <option value="">All queues</option>
            {queueNames.map(n => <option key={n} value={n}>{n}</option>)}
          </select>
        )}
        <label className="history-time"><span>Finished after</span>
          <input type="datetime-local" value={form.since} onChange={set('since')} /></label>
        <label className="history-time"><span>and before</span>
          <input type="datetime-local" value={form.until} onChange={set('until')} /></label>
        <button className="btn btn-primary btn-sm" type="submit" disabled={busy}>{busy ? 'Searching…' : 'Search'}</button>
      </form>

      {error && <div className="notice notice-error">{error}</div>}
      {!searched ? (
        <div className="empty">
          Search the finished jobs PyTaskQ keeps (the newest FINISHED_JOBS_KEPT, 50,000 by default),
          not only the recent ones on the Jobs tab.
        </div>
      ) : rows.length === 0 && !page?.next ? (
        <div className="empty">No finished jobs match. Try fewer filters or a wider time range.</div>
      ) : (
        <div className="table-wrap">
          <table className="jobs">
            <thead>
              <tr><th>Status</th><th>Task</th><th>Arguments</th><th>Finished</th><th className="num">Duration</th></tr>
            </thead>
            <tbody>
              {rows.map(t => (
                <tr key={t.task_id} onClick={() => onOpen(t.task_id)} tabIndex={0}
                  onKeyDown={e => { if (e.key === 'Enter') onOpen(t.task_id); }}>
                  <td><StatusBadge status={t.status} /></td>
                  <td>
                    <div className="task-name">{t.task_name}</div>
                    <div className="task-id">{t.task_id.slice(0, 8)}</div>
                  </td>
                  <td className="args">{formatArgs(t.args, t.kwargs) || <span className="muted">none</span>}</td>
                  <td title={formatClock(t.finished_at)}>{formatClock(t.finished_at)}</td>
                  <td className="num">{formatDuration(t.duration_ms)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {searched && page && (
        <div className="history-foot">
          <span className="muted small">
            {rows.length} found · looked through {scanned.toLocaleString()} of {Number(page.kept || 0).toLocaleString()} kept jobs
          </span>
          {page.next && (
            <button className="btn btn-ghost btn-sm" onClick={() => run(page.next)} disabled={busy}>
              {busy ? 'Searching…' : 'Look further back'}
            </button>
          )}
        </div>
      )}
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
                  {(t.cron || (t.queue && t.queue !== 'default')) && (
                    <span className="task-pick-meta">
                      {t.queue && t.queue !== 'default' && <span className="task-pick-cron"><Layers size={12} /> {t.queue}</span>}
                      {t.cron && <span className="task-pick-cron"><Clock size={12} /> {t.cron}{t.tz && t.tz !== 'UTC' ? ` (${t.tz})` : ' UTC'}</span>}
                    </span>
                  )}
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
              {spec.queue && spec.queue !== 'default' && <> · queue <strong>{spec.queue}</strong></>}
              {spec.max_concurrency && <> · at most <strong>{spec.max_concurrency}</strong> running at once</>}
              {spec.rate_limit && <> · starts limited to <strong>{spec.rate_limit}</strong></>}
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
          <thead><tr><th>Worker</th><th>App</th><th>Queues</th><th className="num">Running</th><th className="num">Done</th><th className="num">Failed</th><th>Pools</th><th>Up</th><th>Health</th></tr></thead>
          <tbody>
            {workers.map(w => (
              <tr key={w.worker_id}>
                <td><div className="task-name">{w.hostname || 'worker'}{w.pid ? ` · pid ${w.pid}` : ''}</div><div className="task-id">{w.worker_id}</div><div className="muted small">{w.version ? `PyTaskQ ${w.version}` : 'PyTaskQ older than 0.5'}</div></td>
                <td>{w.app || <span className="muted">—</span>}</td>
                <td className="small">{(w.queues || ['default']).join(', ')}</td>
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
                  {w.loop_blocked_seconds > 0 && <div className="small warn-text" title="An async task is running blocking code; see Settings → Performance">Blocked by an async task for {w.loop_blocked_seconds}s</div>}
                  {w.stuck_io > 0 && <div className="small warn-text" title="IO tasks that timed out but whose code is still running">{w.stuck_io} IO thread{w.stuck_io === 1 ? '' : 's'} stuck</div>}
                  {w.cpu_restarts > 0 && <div className="muted small" title="CPU processes replaced after a timeout or a crash">{w.cpu_restarts} CPU process{w.cpu_restarts === 1 ? '' : 'es'} replaced</div>}
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
        <p className="muted small">These jobs failed every retry. Fix the cause, then replay them. Entries marked “(invalid job)” weren't valid jobs at all; they're kept so you can see what was sent.</p>
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
                  <div className="task-id">{t.invalid ? t.task_id : t.task_id.slice(0, 8)}</div>
                </td>
                <td className="error-cell">
                  {t.error || <span className="muted">not recorded</span>}
                  {t.invalid && <div className="muted small raw-preview" title={t.raw}>Sent: <code>{String(t.raw ?? '').slice(0, 120)}{String(t.raw ?? '').length > 120 ? '…' : ''}</code></div>}
                </td>
                <td>{timeAgo(t.failed_at)}</td>
                <td className="num">{(t.retry_count || 0) + 1}</td>
                <td>
                  {canOperate && <div className="row-actions">
                    {!t.invalid && <button className="btn btn-secondary btn-sm" onClick={() => act(`/dlq/replay/${t.task_id}`, () => `${t.task_name} replayed.`)}><RotateCcw size={14} /> Replay</button>}
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
  const [redisProblem, setRedisProblem] = useState(null);   // the server can't read Redis right now
  const [tick, setTick] = useState(0);          // bumps on every live update
  const [tasks, setTasks] = useState([]);
  const [loadingTasks, setLoadingTasks] = useState(true);
  const [filter, setFilter] = useState('');
  const [nameFilter, setNameFilter] = useState('');
  const [queueFilter, setQueueFilter] = useState('');
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
      ws.onopen = () => { setConnected(true); setRedisProblem(null); };
      ws.onmessage = ev => {
        const data = JSON.parse(ev.data);
        setRedisProblem(data.degraded ? (data.detail || "Can't reach Redis right now.") : null);
        if (data.degraded) return;
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
      if (queueFilter) q.set('queue', queueFilter);
      api(`/tasks?${q}`).then(d => setTasks(d.tasks || [])).catch(() => {}).finally(() => setLoadingTasks(false));
    };
    const wait = Math.max(0, 1000 - (Date.now() - lastFetch.current));
    clearTimeout(pending.current);
    pending.current = setTimeout(run, wait);
    return () => clearTimeout(pending.current);
  }, [tick, filter, nameFilter, queueFilter]);

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
  const queueNames = useMemo(() => {
    const s = new Set((metrics.queues || []).map(q => q.name));
    registered.forEach(t => t.queue && s.add(t.queue));
    return [...s].sort((a, b) => (a === 'default' ? -1 : b === 'default' ? 1 : a.localeCompare(b)));
  }, [metrics.queues, registered]);

  return (
    <div className="shell">
      <header className="topbar">
        <Brand />
        <div className="topbar-right">
          <span className={`conn ${connected && !redisProblem ? 'live' : ''}`} title={redisProblem || undefined}>
            <span className="dot" /> {!connected ? 'Reconnecting…' : redisProblem ? (redisProblem.startsWith('Redis is full') ? 'Redis is full' : 'Redis unreachable') : 'Live'}
          </span>
          <button className="user-chip" onClick={() => setTab('settings')} title="Account settings">
            <UserRound size={14} />
            <span>{authEnabled ? me.username : 'Sign-in off'}</span>
            {authEnabled && <span className="role">{ROLE_LABEL[me.role]}</span>}
          </button>
          <ThemeButton theme={theme} setTheme={setTheme} />
        </div>
      </header>

      <Lifecycle m={{ ...metrics, worker_count: workerCount }} onAdvice={() => setTab('settings')} />
      {(showsQueues(metrics) || canOperate || queuesInUse(metrics).some(q => q.paused)) && queuesInUse(metrics).length > 0 && (
        <QueueStrip m={metrics} active={queueFilter} onPick={q => { setQueueFilter(q); setTab('jobs'); }}
          canOperate={canOperate} notify={notify} />
      )}
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
            setNameFilter={setNameFilter} queueFilter={queueFilter} setQueueFilter={setQueueFilter}
            queueNames={queueNames} taskNames={taskNames} onOpen={setOpenId} loading={loadingTasks} />
        )}
        {tab === 'run' && (
          <RunView registered={registered} notify={notify} onQueued={() => {}} />
        )}
        {tab === 'workers' && <WorkersView workers={workers} />}
        {tab === 'dead' && <DeadLettersView items={dlq} notify={notify} onOpen={setOpenId} canOperate={canOperate} />}
        {tab === 'workflows' && <WorkflowsView onOpen={setOpenId} notify={notify} canOperate={canOperate} refreshKey={tick} />}
        {tab === 'history' && <HistoryView taskNames={taskNames} queueNames={queueNames} onOpen={setOpenId} />}
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
