import { useState, useEffect, useRef } from 'react'
import { Send, Search, AlertOctagon, Inbox, Activity, Clock, AlertTriangle, CheckCircle, XCircle, RotateCcw, Trash2, Zap, BarChart2, Copy, Moon, Sun } from 'lucide-react'

const API_URL = import.meta.env.DEV ? 'http://localhost:8000' : '';

const STATUS_COLORS = {
  Success:        { bg: 'var(--status-success-bg)', color: 'var(--status-success-text)', border: 'var(--status-success-border)' },
  Failed:         { bg: 'var(--status-failed-bg)', color: 'var(--status-failed-text)', border: 'var(--status-failed-border)' },
  DeadLetter:     { bg: 'var(--status-failed-bg)', color: 'var(--status-failed-text)', border: 'var(--status-failed-border)' },
  RetryScheduled: { bg: 'var(--status-warning-bg)', color: 'var(--status-warning-text)', border: 'var(--status-warning-border)' },
  Processing:     { bg: 'var(--status-processing-bg)', color: 'var(--status-processing-text)', border: 'var(--status-processing-border)' },
  Pending:        { bg: 'var(--status-pending-bg)', color: 'var(--status-pending-text)', border: 'var(--status-pending-border)' },
  queued:         { bg: 'var(--status-pending-bg)', color: 'var(--status-pending-text)', border: 'var(--status-pending-border)' },
};

function StatusBadge({ status }) {
  const s = STATUS_COLORS[status] || { bg: 'var(--status-default-bg)', color: 'var(--status-default-text)', border: 'var(--status-default-border)' };
  return (
    <span style={{
      padding: '0.2rem 0.6rem', borderRadius: '999px', fontSize: '0.75rem',
      fontWeight: 700, background: s.bg, color: s.color,
      border: `1px solid ${s.border}`, whiteSpace: 'nowrap'
    }}>{status}</span>
  );
}

function CopyButton({ text }) {
  const [copied, setCopied] = useState(false);
  return (
    <button className="btn btn-secondary" style={{ padding: '0.3rem 0.7rem', fontSize: '0.75rem' }}
      onClick={() => { navigator.clipboard.writeText(text); setCopied(true); setTimeout(() => setCopied(false), 2000); }}>
      <Copy size={12} style={{ marginRight: '0.3rem' }} />{copied ? 'Copied!' : 'Copy'}
    </button>
  );
}

// ── Animated Theme Toggle ────────────────────────────────────────────────────
function ThemeToggle({ theme, onToggle }) {
  const isDark = theme === 'dark';
  return (
    <button
      onClick={onToggle}
      title={isDark ? 'Switch to Pastel Light' : 'Switch to Modern Dark'}
      style={{
        display: 'flex',
        alignItems: 'center',
        gap: '0.5rem',
        padding: '0.45rem 0.9rem',
        borderRadius: '999px',
        border: '1px solid var(--border-color)',
        background: 'var(--surface-color)',
        color: 'var(--text-secondary)',
        cursor: 'pointer',
        fontSize: '0.8rem',
        fontWeight: 600,
        fontFamily: 'inherit',
        transition: 'all 0.2s ease',
        boxShadow: 'var(--card-shadow)',
      }}
    >
      {isDark ? <Sun size={14} color="#fbbf24" /> : <Moon size={14} color="#6366f1" />}
      {isDark ? 'Pastel Light' : 'Modern Dark'}
    </button>
  );
}

function App() {
  const [theme, setTheme] = useState(localStorage.getItem('theme') || 'dark');

  useEffect(() => {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('theme', theme);
  }, [theme]);

  const toggleTheme = () => setTheme(t => t === 'dark' ? 'light' : 'dark');

  const [metrics, setMetrics] = useState({
    pending: 0, processing: 0, delayed: 0, dlq: 0,
    completed_total: 0, failed_total: 0,
    queue_high: 0, queue_default: 0, queue_low: 0,
    queue_total: 0, queue_capacity: 500,
    worker_count: 0,
  });

  const [taskName, setTaskName] = useState('matrix_multiply');
  const [taskArgs, setTaskArgs] = useState('50');
  const [delaySeconds, setDelaySeconds] = useState('0');
  const [submitStatus, setSubmitStatus] = useState(null);

  const [recentTasks, setRecentTasks] = useState([]);
  const recentTasksRef = useRef(recentTasks);
  recentTasksRef.current = recentTasks;

  const [lookupId, setLookupId] = useState('');
  const [lookupResult, setLookupResult] = useState(null);
  const [dlqTasks, setDlqTasks] = useState([]);
  const [workers, setWorkers] = useState([]);

  // ── WebSocket: real-time dashboard ──────────────────────────────────────
  useEffect(() => {
    const WS_URL = import.meta.env.DEV
      ? `ws://localhost:8000/ws/dashboard`
      : `${window.location.protocol === 'https:' ? 'wss:' : 'ws:'}//${window.location.host}/ws/dashboard`;

    let ws;
    let reconnectTimer;
    let alive = true;

    const connect = () => {
      ws = new WebSocket(WS_URL);

      ws.onopen = () => {
        const ids = recentTasksRef.current
          .filter(t => !['Success', 'Failed', 'DeadLetter'].includes(t.status))
          .map(t => t.task_id);
        if (ids.length > 0) ws.send(JSON.stringify({ track: ids }));
      };

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);
        if (data.metrics) setMetrics(data.metrics);
        if (data.dlq) setDlqTasks(data.dlq);
        if (data.task_statuses && Object.keys(data.task_statuses).length > 0) {
          setRecentTasks(prev => prev.map(t => ({
            ...t,
            status: data.task_statuses[t.task_id] || t.status,
          })));
        }
        const pending = recentTasksRef.current
          .filter(t => !['Success', 'Failed', 'DeadLetter'].includes(t.status))
          .map(t => t.task_id);
        if (ws.readyState === WebSocket.OPEN && pending.length > 0) {
          ws.send(JSON.stringify({ track: pending }));
        }
      };

      ws.onclose = () => {
        if (alive) reconnectTimer = setTimeout(connect, 3000);
      };
    };

    connect();
    return () => {
      alive = false;
      clearTimeout(reconnectTimer);
      if (ws) ws.close();
    };
  }, []);

  useEffect(() => {
    const fetchWorkers = () => {
      fetch(`${API_URL}/system/workers`)
        .then(r => r.json()).then(data => setWorkers(data.workers || [])).catch(() => {});
    };
    fetchWorkers();
    const iv = setInterval(fetchWorkers, 5000);
    return () => clearInterval(iv);
  }, []);

  const handleEnqueue = async (e) => {
    e.preventDefault();
    setSubmitStatus({ type: 'info', msg: 'Dispatching task...' });
    let parsedArgs = [];
    if (taskName === 'matrix_multiply') parsedArgs = [Number(taskArgs) || 10];
    else if (taskName === 'generate_csv_report') parsedArgs = [Number(taskArgs) || 100];
    else parsedArgs = taskArgs.split(',').map(a => { const t = a.trim(); return isNaN(t) ? t : Number(t); }).filter(a => a !== '');
    
    if (taskName === 'matrix_multiply' && parsedArgs[0] > 1000) {
      setSubmitStatus({ type: 'error', msg: 'Matrix size cannot exceed 1000.' }); return;
    }
    const delay = parseInt(delaySeconds, 10);
    const endpoint = delay > 0 ? `${API_URL}/task/schedule?delay_seconds=${delay}` : `${API_URL}/task/enqueue`;
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ task_name: taskName, args: parsedArgs })
      });
      const data = await response.json();
      if (response.ok) {
        setSubmitStatus({ type: 'success', msg: `✓ Enqueued! ID: ${data.task_id}` });
        setRecentTasks(prev => [
          { task_id: data.task_id, task_name: taskName, status: 'queued', dispatched_at: new Date().toLocaleTimeString() },
          ...prev
        ].slice(0, 8));
      } else {
        setSubmitStatus({ type: 'error', msg: `Error: ${data.detail}` });
      }
    } catch (err) { setSubmitStatus({ type: 'error', msg: `Network error: ${err.message}` }); }
  };

  const handleLookup = async (e) => {
    e.preventDefault();
    if (!lookupId) return;
    try {
      const res = await fetch(`${API_URL}/task/${lookupId}`);
      setLookupResult((await res.json()).result);
    } catch (err) { setLookupResult({ error: err.message }); }
  };

  const handleReplay = async (taskId) => {
    try { await fetch(`${API_URL}/dlq/replay/${taskId}`, { method: 'POST' }); }
    catch (err) { console.error(err); }
  };

  const handlePurge = async (taskId) => {
    try { await fetch(`${API_URL}/dlq/purge/${taskId}`, { method: 'POST' }); }
    catch (err) { console.error(err); }
  };

  const handleRetryAll = async () => {
    try { await fetch(`${API_URL}/dlq/retry_all`, { method: 'POST' }); }
    catch (err) { console.error(err); }
  };

  const queuePct = Math.min((metrics.queue_total / (metrics.queue_capacity || 500)) * 100, 100);
  const queueColor = queuePct >= 90 ? 'var(--danger-color)' : queuePct >= 70 ? 'var(--warning-color)' : 'var(--accent-color)';

  return (
    <div>
      {/* ── Header ────────────────────────────────────────────────────────────── */}
      <header className="header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h1>⚡ PyTaskQ</h1>
            <p>Real-time distributed worker queue dashboard.</p>
          </div>
          <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
            <ThemeToggle theme={theme} onToggle={toggleTheme} />
          </div>
        </div>
      </header>

      {/* ── Queue Health Bar ──────────────────────────────────────────────────── */}
      <div style={{ background: 'var(--surface-color)', border: '1px solid var(--border-color)', borderRadius: '8px', padding: '0.75rem 1.25rem', marginBottom: '1.5rem', display: 'flex', alignItems: 'center', gap: '1rem' }}>
        <BarChart2 size={16} color="var(--text-secondary)" />
        <span style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Queue Health</span>
        <div style={{ flex: 1, height: '8px', background: 'var(--border-color)', borderRadius: '999px', overflow: 'hidden' }}>
          <div style={{ height: '100%', width: `${queuePct}%`, background: queueColor, borderRadius: '999px', transition: 'width 0.5s ease' }} />
        </div>
        <span style={{ fontSize: '0.8rem', fontWeight: 700, color: queueColor, whiteSpace: 'nowrap' }}>{metrics.queue_total} / {metrics.queue_capacity} tasks</span>
        <div style={{ display: 'flex', gap: '0.5rem', fontSize: '0.75rem' }}>
          <span style={{ background: 'var(--status-failed-bg)', color: 'var(--status-failed-text)', padding: '0.15rem 0.5rem', borderRadius: '999px', fontWeight: 600 }}>H:{metrics.queue_high}</span>
          <span style={{ background: 'var(--status-processing-bg)', color: 'var(--status-processing-text)', padding: '0.15rem 0.5rem', borderRadius: '999px', fontWeight: 600 }}>D:{metrics.queue_default}</span>
          <span style={{ background: 'var(--status-success-bg)', color: 'var(--status-success-text)', padding: '0.15rem 0.5rem', borderRadius: '999px', fontWeight: 600 }}>L:{metrics.queue_low}</span>
        </div>
      </div>

      {/* ── Metrics Grid ─────────────────────────────────────────────────────── */}
      <div className="metrics-grid">
        <div className="metric-box pending"><div className="metric-title"><Inbox size={16} /> Pending</div><div className="metric-value" style={{ color: 'var(--accent-color)' }}>{metrics.pending}</div></div>
        <div className="metric-box processing"><div className="metric-title"><Activity size={16} /> Active</div><div className="metric-value" style={{ color: 'var(--success-color)' }}>{metrics.processing}</div></div>
        <div className="metric-box delayed"><div className="metric-title"><Clock size={16} /> Delayed</div><div className="metric-value" style={{ color: 'var(--warning-color)' }}>{metrics.delayed}</div></div>
        <div className="metric-box dlq"><div className="metric-title"><AlertTriangle size={16} /> Dead Letters</div><div className="metric-value" style={{ color: 'var(--danger-color)' }}>{metrics.dlq}</div></div>
        <div className="metric-box"><div className="metric-title"><CheckCircle size={16} /> Completed</div><div className="metric-value" style={{ color: 'var(--success-color)' }}>{metrics.completed_total}</div></div>
        <div className="metric-box"><div className="metric-title"><Activity size={16} /> Workers</div><div className="metric-value" style={{ color: 'var(--accent-color)' }}>{metrics.worker_count || workers.length}</div></div>
      </div>

      <div className="dashboard-grid">
        {/* ── Left Column ─────────────────────────────────────────────────────── */}
        <div>
          {/* Dispatch Form */}
          <div className="card" style={{ marginBottom: '2rem' }}>
            <h2><Send size={20} style={{ color: 'var(--accent-color)' }} /> Dispatch New Task</h2>
            <form onSubmit={handleEnqueue}>
              <div className="form-group">
                <label>Select Task Type</label>
                <select value={taskName} onChange={(e) => setTaskName(e.target.value)} style={{ padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)', fontSize: '1rem', backgroundColor: 'var(--surface-color)', color: 'var(--text-primary)' }}>
                  <option value="matrix_multiply">Matrix Multiplication (CPU)</option>
                  <option value="url_health_check">URL Health Check (I/O)</option>
                  <option value="generate_csv_report">Generate CSV Report (CPU)</option>
                  <option value="resize_image">Resize Image (I/O)</option>
                </select>
              </div>
              {taskName === 'matrix_multiply' && (<div className="form-group"><label>Matrix Size (N × N) — max 1000</label><input type="number" min="1" max="1000" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required /></div>)}
              {taskName === 'url_health_check' && (<div className="form-group"><label>URL to check</label><input type="url" placeholder="https://example.com" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required /></div>)}
              {taskName === 'generate_csv_report' && (<div className="form-group"><label>Number of rows</label><input type="number" min="1" max="100000" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required /></div>)}
              {taskName === 'resize_image' && (<div className="form-group"><label>Args: image_url, width, height (comma separated)</label><input type="text" placeholder="https://example.com/img.jpg, 800, 600" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required /></div>)}
              <div className="form-group">
                <label>Delay (seconds) — 0 = immediate</label>
                <input type="number" min="0" value={delaySeconds} onChange={(e) => setDelaySeconds(e.target.value)} />
              </div>
              <button type="submit" className="btn btn-primary" style={{ width: '100%', marginTop: '0.5rem' }}>
                <Zap size={16} style={{ marginRight: '0.5rem' }} />Dispatch Task
              </button>
            </form>
            {submitStatus && <div className={`status-box ${submitStatus.type}`}>{submitStatus.msg}</div>}
          </div>

          {/* Live Task Feed */}
          {recentTasks.length > 0 && (
            <div className="card" style={{ marginBottom: '2rem' }}>
              <h2><Activity size={20} style={{ color: 'var(--success-color)' }} /> Live Task Feed</h2>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {recentTasks.map((t) => (
                  <div key={t.task_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.75rem', background: 'var(--task-bg)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
                    <div>
                      <div style={{ fontWeight: 600, fontSize: '0.875rem' }}>{t.task_name}</div>
                      <div style={{ fontFamily: 'monospace', fontSize: '0.7rem', color: 'var(--text-secondary)', marginTop: '0.2rem' }}>{t.task_id}</div>
                    </div>
                    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '0.25rem' }}>
                      <StatusBadge status={t.status} />
                      <span style={{ fontSize: '0.7rem', color: 'var(--text-secondary)' }}>{t.dispatched_at}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Task Lookup */}
          <div className="card">
            <h2><Search size={20} style={{ color: 'var(--accent-color)' }} /> Check Task Status</h2>
            <form onSubmit={handleLookup} style={{ display: 'flex', gap: '1rem' }}>
              <input type="text" placeholder="Paste Task ID here..." value={lookupId} onChange={(e) => setLookupId(e.target.value)} style={{ flex: 1, padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)', background: 'var(--surface-color)', color: 'var(--text-primary)' }} />
              <button type="submit" className="btn btn-secondary">Search</button>
            </form>
            {lookupResult && (
              <div style={{ marginTop: '1rem', padding: '1rem', background: 'var(--code-bg)', color: 'var(--code-text)', borderRadius: '6px', fontFamily: 'monospace', fontSize: '0.85rem', overflowX: 'auto', whiteSpace: 'pre-wrap' }}>
                {Object.keys(lookupResult).length === 0 ? 'Task not found or expired.' : JSON.stringify(lookupResult, null, 2)}
              </div>
            )}
          </div>
        </div>

        {/* ── Right Column ─────────────────────────────────────────────────────── */}
        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          {/* Worker Health */}
          <div className="card">
            <h2><Activity size={20} style={{ color: 'var(--accent-color)' }} /> Worker Fleet Health</h2>
            {workers.length === 0 ? (
              <p style={{ color: 'var(--text-secondary)' }}>No active workers detected. Is the worker process running?</p>
            ) : (
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {workers.map(w => (
                  <div key={w.worker_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.75rem', background: 'var(--task-bg)', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
                    <div style={{ fontFamily: 'monospace', fontSize: '0.8rem', fontWeight: 600 }}>{w.worker_id}</div>
                    <div style={{ display: 'flex', alignItems: 'center', gap: '1rem' }}>
                      <span style={{ fontSize: '0.75rem', color: 'var(--text-secondary)' }}>Heartbeat: {w.last_heartbeat_seconds_ago}s ago</span>
                      <StatusBadge status={w.status === 'healthy' ? 'Success' : 'Failed'} />
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Dead Letter Queue */}
          <div className="card" style={{ flex: 1 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
              <h2 style={{ color: 'var(--danger-color)', margin: 0, display: 'flex', alignItems: 'center', gap: '0.5rem' }}><AlertOctagon size={20} /> Dead Letter Queue</h2>
              {dlqTasks.length > 0 && (
                <div style={{ display: 'flex', gap: '0.5rem' }}>
                  <button onClick={handleRetryAll} className="btn btn-secondary" style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}>Retry All</button>
                  <button onClick={() => fetch(`${API_URL}/dlq/purge_all`, { method: 'POST' })} className="btn btn-danger" style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}>Purge All</button>
                </div>
              )}
            </div>
            {dlqTasks.length === 0 ? (
              <p style={{ color: 'var(--text-secondary)' }}>✓ Queue is healthy. No failed tasks!</p>
            ) : (
              <div className="table-container">
                <table>
                  <thead><tr><th>Task ID</th><th>Name</th><th>Retries</th><th>Actions</th></tr></thead>
                  <tbody>
                    {dlqTasks.map((t, idx) => (
                      <tr key={idx}>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.8rem' }}>{t.task_id.substring(0, 10)}...</td>
                        <td style={{ fontWeight: 600 }}>{t.task_name}</td>
                        <td><span style={{ fontWeight: 700, color: 'var(--danger-color)' }}>{t.retry_count}</span></td>
                        <td style={{ display: 'flex', gap: '0.5rem' }}>
                          <button onClick={() => handleReplay(t.task_id)} className="btn btn-secondary" style={{ padding: '0.25rem 0.5rem', fontSize: '0.75rem', display: 'flex', alignItems: 'center', gap: '0.25rem' }}><RotateCcw size={12} /> Replay</button>
                          <button onClick={() => handlePurge(t.task_id)} className="btn btn-danger" style={{ padding: '0.25rem 0.5rem', fontSize: '0.75rem', display: 'flex', alignItems: 'center', gap: '0.25rem' }}><Trash2 size={12} /> Delete</button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}

export default App
