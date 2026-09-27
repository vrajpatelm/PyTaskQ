import { useState, useEffect, useRef } from 'react'
import { Send, Search, AlertOctagon, Inbox, Activity, Clock, AlertTriangle, CheckCircle, XCircle, RotateCcw, Trash2, Zap, BarChart2, Copy, Eye, EyeOff } from 'lucide-react'

const API_URL = import.meta.env.DEV ? 'http://localhost:8000' : '';

const STATUS_COLORS = {
  Success:        { bg: '#ecfdf5', color: '#065f46', border: '#34d399' },
  Failed:         { bg: '#fef2f2', color: '#991b1b', border: '#f87171' },
  DeadLetter:     { bg: '#fef2f2', color: '#991b1b', border: '#f87171' },
  RetryScheduled: { bg: '#fffbeb', color: '#92400e', border: '#fbbf24' },
  Processing:     { bg: '#eff6ff', color: '#1e40af', border: '#60a5fa' },
  Pending:        { bg: '#f5f3ff', color: '#4c1d95', border: '#a78bfa' },
  queued:         { bg: '#f5f3ff', color: '#4c1d95', border: '#a78bfa' },
};

function StatusBadge({ status }) {
  const s = STATUS_COLORS[status] || { bg: '#f8fafc', color: '#475569', border: '#e2e8f0' };
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

function App() {
  const [apiKey, setApiKey] = useState(sessionStorage.getItem('apiKey') || '');
  const [isAuthed, setIsAuthed] = useState(false);
  const [showKey, setShowKey] = useState(false);
  const [authError, setAuthError] = useState(null);

  const [metrics, setMetrics] = useState({
    pending: 0, processing: 0, delayed: 0, dlq: 0,
    completed_total: 0, failed_total: 0,
    queue_high: 0, queue_default: 0, queue_low: 0,
    queue_total: 0, queue_capacity: 500,
    rate_limit_used: 0, rate_limit_max: 100,
  });

  const [taskName, setTaskName] = useState('matrix_multiply');
  const [taskArgs, setTaskArgs] = useState('50');
  const [emailTo, setEmailTo] = useState('user@example.com');
  const [emailTitle, setEmailTitle] = useState('Welcome!');
  const [emailBody, setEmailBody] = useState('Hello from PyTaskQ background worker.');
  const [delaySeconds, setDelaySeconds] = useState('0');
  const [submitStatus, setSubmitStatus] = useState(null);

  const [recentTasks, setRecentTasks] = useState([]);
  const recentTasksRef = useRef(recentTasks);
  recentTasksRef.current = recentTasks;

  const [webhookUrl, setWebhookUrl] = useState('');
  const [webhookInfo, setWebhookInfo] = useState(null);
  const [webhookStatus, setWebhookStatus] = useState(null);
  const [showSecret, setShowSecret] = useState(false);

  const [lookupId, setLookupId] = useState('');
  const [lookupResult, setLookupResult] = useState(null);
  const [dlqTasks, setDlqTasks] = useState([]);

  // ── WebSocket: real-time dashboard ──────────────────────────────────────
  // Instead of polling every 1s (3 HTTP requests/tick), we open ONE persistent
  // WebSocket connection. The server pushes metrics + DLQ + task statuses every
  // second. The client sends back the list of task IDs it wants tracked.
  useEffect(() => {
    if (!apiKey) { setIsAuthed(false); setAuthError(null); return; }

    const WS_URL = import.meta.env.DEV
      ? `ws://localhost:8000/ws/dashboard?key=${apiKey}`
      : `wss://${window.location.host}/ws/dashboard?key=${apiKey}`;

    let ws;
    let reconnectTimer;
    let alive = true; // set to false on cleanup to stop reconnecting

    const connect = () => {
      ws = new WebSocket(WS_URL);

      ws.onopen = () => {
        setIsAuthed(true);
        setAuthError(null);
        // Tell the server which task IDs we want status updates for
        const ids = recentTasksRef.current
          .filter(t => !['Success', 'Failed', 'DeadLetter'].includes(t.status))
          .map(t => t.task_id);
        if (ids.length > 0) ws.send(JSON.stringify({ track: ids }));
      };

      ws.onmessage = (event) => {
        const data = JSON.parse(event.data);

        // Update metrics
        if (data.metrics) setMetrics(data.metrics);

        // Update DLQ list
        if (data.dlq) setDlqTasks(data.dlq);

        // Update recent task statuses from server-pushed map
        if (data.task_statuses && Object.keys(data.task_statuses).length > 0) {
          setRecentTasks(prev => prev.map(t => ({
            ...t,
            status: data.task_statuses[t.task_id] || t.status,
          })));
        }

        // Keep server informed of which tasks to track
        const pending = recentTasksRef.current
          .filter(t => !['Success', 'Failed', 'DeadLetter'].includes(t.status))
          .map(t => t.task_id);
        if (ws.readyState === WebSocket.OPEN && pending.length > 0) {
          ws.send(JSON.stringify({ track: pending }));
        }
      };

      ws.onerror = () => {
        setIsAuthed(false);
        setAuthError('WebSocket error — check your API key or server connection.');
      };

      ws.onclose = (e) => {
        if (e.code === 4001) {
          // Server rejected the key — don't reconnect
          setIsAuthed(false);
          setAuthError('Unauthorized: The API Key is invalid or has been revoked.');
          return;
        }
        // Any other close (network drop, server restart) → reconnect after 3s
        if (alive) reconnectTimer = setTimeout(connect, 3000);
      };
    };

    connect();

    return () => {
      alive = false;
      clearTimeout(reconnectTimer);
      if (ws) ws.close();
    };
  }, [apiKey]);


  useEffect(() => {
    if (!isAuthed) return;
    fetch(`${API_URL}/webhooks/info`, { headers: { 'Authorization': `Bearer ${apiKey}` } })
      .then(r => r.json()).then(setWebhookInfo).catch(() => {});
  }, [isAuthed]);

  const handleEnqueue = async (e) => {
    e.preventDefault();
    setSubmitStatus({ type: 'info', msg: 'Submitting...' });
    let parsedArgs = [];
    if (taskName === 'send_email') parsedArgs = [emailTo, emailTitle, emailBody];
    else if (taskName === 'matrix_multiply') parsedArgs = [Number(taskArgs) || 10];
    else parsedArgs = taskArgs.split(',').map(a => { const t = a.trim(); return isNaN(t) ? t : Number(t); }).filter(a => a !== '');
    const delay = parseInt(delaySeconds, 10);
    if (taskName === 'matrix_multiply' && parsedArgs[0] > 1000) {
      setSubmitStatus({ type: 'error', msg: 'Size cannot Exceed 1000' }); return;
    }
    const endpoint = delay > 0 ? `${API_URL}/task/schedule?delay_seconds=${delay}` : `${API_URL}/task/enqueue`;
    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
        body: JSON.stringify({ task_name: taskName, args: parsedArgs })
      });
      const data = await response.json();
      if (response.ok) {
        setSubmitStatus({ type: 'success', msg: `Enqueued! ID: ${data.task_id}` });
        setRecentTasks(prev => [
          { task_id: data.task_id, task_name: taskName, status: 'queued', dispatched_at: new Date().toLocaleTimeString() },
          ...prev
        ].slice(0, 5));
      } else {
        setSubmitStatus({ type: 'error', msg: `Error: ${data.detail}` });
      }
    } catch (err) { setSubmitStatus({ type: 'error', msg: `Network error: ${err.message}` }); }
  };

  const handleRegisterWebhook = async () => {
    if (!webhookUrl) return;
    setWebhookStatus({ type: 'info', msg: 'Registering...' });
    try {
      const res = await fetch(`${API_URL}/webhooks/register`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${apiKey}` },
        body: JSON.stringify({ url: webhookUrl })
      });
      const data = await res.json();
      if (res.ok) {
        setWebhookInfo({ registered: true, url: data.url, secret: data.secret });
        setWebhookStatus({ type: 'success', msg: 'Webhook registered! Copy your secret below.' });
        setWebhookUrl('');
      } else { setWebhookStatus({ type: 'error', msg: `Error: ${data.detail}` }); }
    } catch (err) { setWebhookStatus({ type: 'error', msg: `Network error: ${err.message}` }); }
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
    try { await fetch(`${API_URL}/dlq/replay/${taskId}`, { method: 'POST', headers: { 'Authorization': `Bearer ${apiKey}` } }); }
    catch (err) { console.error(err); }
  };

  const handlePurge = async (taskId) => {
    try { await fetch(`${API_URL}/dlq/purge/${taskId}`, { method: 'POST', headers: { 'Authorization': `Bearer ${apiKey}` } }); }
    catch (err) { console.error(err); }
  };

  if (!isAuthed) {
    return (
      <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', justifyContent: 'center', minHeight: '100vh', padding: '2rem' }}>
        <div className="card" style={{ maxWidth: '480px', width: '100%', textAlign: 'center' }}>
          <div style={{ fontSize: '3rem', marginBottom: '0.5rem' }}>⚡</div>
          <h2 style={{ margin: '0 0 0.5rem' }}>PyTaskQ Dashboard</h2>
          <p style={{ color: 'var(--text-secondary)', marginBottom: '1.5rem' }}>Enter your API Key to access your tenant dashboard.</p>
          {authError && (
            <div style={{ background: '#fef2f2', border: '1px solid #f87171', color: '#991b1b', padding: '0.75rem', borderRadius: '6px', marginBottom: '1rem', fontSize: '0.875rem', display: 'flex', alignItems: 'center', gap: '0.5rem' }}>
              <AlertTriangle size={16} />{authError}
            </div>
          )}
          <div style={{ display: 'flex', gap: '0.5rem' }}>
            <input type={showKey ? 'text' : 'password'} value={apiKey}
              onChange={(e) => { const v = e.target.value.trim(); setApiKey(v); sessionStorage.setItem('apiKey', v); if (v) setAuthError(null); }}
              placeholder="Paste sk_... key here"
              style={{ flex: 1, padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)', fontSize: '1rem' }} />
            <button className="btn btn-secondary" style={{ padding: '0 1rem' }} onClick={() => setShowKey(!showKey)}>
              {showKey ? <EyeOff size={16} /> : <Eye size={16} />}
            </button>
          </div>
        </div>
      </div>
    );
  }

  const rateUsed = metrics.rate_limit_used;
  const ratePct = Math.min((rateUsed / metrics.rate_limit_max) * 100, 100);
  const rateColor = ratePct >= 95 ? 'var(--danger-color)' : ratePct >= 75 ? 'var(--warning-color)' : 'var(--success-color)';
  const queuePct = Math.min((metrics.queue_total / metrics.queue_capacity) * 100, 100);
  const queueColor = queuePct >= 90 ? 'var(--danger-color)' : queuePct >= 70 ? 'var(--warning-color)' : 'var(--accent-color)';

  return (
    <div>
      <header className="header">
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <h1>PyTaskQ Dashboard</h1>
            <p>Monitor your distributed worker queues and API health.</p>
          </div>
          <div style={{ display: 'flex', gap: '0.75rem', alignItems: 'center' }}>
            <div style={{ textAlign: 'right', minWidth: '170px' }}>
              <div style={{ fontSize: '0.75rem', color: 'var(--text-secondary)', fontWeight: 600, marginBottom: '0.3rem' }}>
                Rate: <span style={{ color: rateColor, fontWeight: 700 }}>{rateUsed}</span> / {metrics.rate_limit_max} req/min
              </div>
              <div style={{ height: '6px', background: '#e2e8f0', borderRadius: '999px', overflow: 'hidden' }}>
                <div style={{ height: '100%', width: `${ratePct}%`, background: rateColor, borderRadius: '999px', transition: 'width 0.4s ease' }} />
              </div>
            </div>
            <button onClick={() => { setApiKey(''); sessionStorage.removeItem('apiKey'); setIsAuthed(false); }} className="btn btn-secondary" style={{ padding: '0.5rem 1rem' }}>Logout</button>
          </div>
        </div>
      </header>

      <div style={{ background: 'white', border: '1px solid var(--border-color)', borderRadius: '8px', padding: '0.75rem 1.25rem', marginBottom: '1.5rem', display: 'flex', alignItems: 'center', gap: '1rem' }}>
        <BarChart2 size={16} color="var(--text-secondary)" />
        <span style={{ fontSize: '0.8rem', fontWeight: 600, color: 'var(--text-secondary)', whiteSpace: 'nowrap' }}>Queue Health</span>
        <div style={{ flex: 1, height: '8px', background: '#e2e8f0', borderRadius: '999px', overflow: 'hidden' }}>
          <div style={{ height: '100%', width: `${queuePct}%`, background: queueColor, borderRadius: '999px', transition: 'width 0.5s ease' }} />
        </div>
        <span style={{ fontSize: '0.8rem', fontWeight: 700, color: queueColor, whiteSpace: 'nowrap' }}>{metrics.queue_total} / {metrics.queue_capacity} tasks</span>
        <div style={{ display: 'flex', gap: '0.5rem', fontSize: '0.75rem' }}>
          <span style={{ background: '#fef2f2', color: '#991b1b', padding: '0.15rem 0.5rem', borderRadius: '999px', fontWeight: 600 }}>H:{metrics.queue_high}</span>
          <span style={{ background: '#eff6ff', color: '#1e40af', padding: '0.15rem 0.5rem', borderRadius: '999px', fontWeight: 600 }}>D:{metrics.queue_default}</span>
          <span style={{ background: '#f0fdf4', color: '#065f46', padding: '0.15rem 0.5rem', borderRadius: '999px', fontWeight: 600 }}>L:{metrics.queue_low}</span>
        </div>
      </div>

      <div className="metrics-grid">
        <div className="metric-box pending"><div className="metric-title"><Inbox size={16} /> Pending</div><div className="metric-value" style={{ color: 'var(--accent-color)' }}>{metrics.pending}</div></div>
        <div className="metric-box processing"><div className="metric-title"><Activity size={16} /> Active Now</div><div className="metric-value" style={{ color: 'var(--success-color)' }}>{metrics.processing}</div></div>
        <div className="metric-box delayed"><div className="metric-title"><Clock size={16} /> Delayed</div><div className="metric-value" style={{ color: 'var(--warning-color)' }}>{metrics.delayed}</div></div>
        <div className="metric-box dlq"><div className="metric-title"><AlertTriangle size={16} /> Dead Letters</div><div className="metric-value" style={{ color: 'var(--danger-color)' }}>{metrics.dlq}</div></div>
        <div className="metric-box"><div className="metric-title"><CheckCircle size={16} /> Completed</div><div className="metric-value" style={{ color: 'var(--success-color)' }}>{metrics.completed_total}</div></div>
        <div className="metric-box"><div className="metric-title"><XCircle size={16} /> Failed</div><div className="metric-value" style={{ color: 'var(--danger-color)' }}>{metrics.failed_total}</div></div>
      </div>

      <div className="dashboard-grid">
        <div>
          <div className="card" style={{ marginBottom: '2rem' }}>
            <h2><Send size={20} style={{ color: 'var(--accent-color)' }} /> Dispatch New Task</h2>
            <form onSubmit={handleEnqueue}>
              <div className="form-group">
                <label>Select Task Type</label>
                <select value={taskName} onChange={(e) => setTaskName(e.target.value)} style={{ padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)', fontSize: '1rem', backgroundColor: 'white' }}>
                  <option value="matrix_multiply">Matrix Multiplication (CPU)</option>
                  <option value="url_health_check">URL Health Check (I/O)</option>
                  <option value="generate_csv_report">Generate CSV Report (CPU)</option>
                  <option value="custom">Custom Task...</option>
                </select>
              </div>
              {taskName === 'matrix_multiply' && (<div className="form-group"><label>Matrix Size (N x N) — max 1000</label><input type="number" min="1" max="1000" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required /></div>)}
              {taskName === 'url_health_check' && (<div className="form-group"><label>URL to check</label><input type="url" placeholder="https://example.com" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required /></div>)}
              {taskName === 'generate_csv_report' && (<div className="form-group"><label>Number of rows</label><input type="number" min="1" max="100000" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required /></div>)}
              {taskName === 'custom' && (<><div className="form-group"><label>Task Name</label><input type="text" placeholder="my_task" onChange={(e) => setTaskName(e.target.value)} required /></div><div className="form-group"><label>Arguments (comma separated)</label><input type="text" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} placeholder="arg1, arg2" /></div></>)}
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

          {recentTasks.length > 0 && (
            <div className="card" style={{ marginBottom: '2rem' }}>
              <h2><Activity size={20} style={{ color: 'var(--success-color)' }} /> Live Task Feed</h2>
              <div style={{ display: 'flex', flexDirection: 'column', gap: '0.75rem' }}>
                {recentTasks.map((t) => (
                  <div key={t.task_id} style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', padding: '0.75rem', background: '#f8fafc', borderRadius: '8px', border: '1px solid var(--border-color)' }}>
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

          <div className="card">
            <h2><Search size={20} style={{ color: 'var(--accent-color)' }} /> Check Task Status</h2>
            <form onSubmit={handleLookup} style={{ display: 'flex', gap: '1rem' }}>
              <input type="text" placeholder="Paste Task ID here..." value={lookupId} onChange={(e) => setLookupId(e.target.value)} style={{ flex: 1, padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)' }} />
              <button type="submit" className="btn btn-secondary">Search</button>
            </form>
            {lookupResult && (
              <div style={{ marginTop: '1rem', padding: '1rem', background: '#1e293b', color: '#f8fafc', borderRadius: '6px', fontFamily: 'monospace', fontSize: '0.85rem', overflowX: 'auto', whiteSpace: 'pre-wrap' }}>
                {Object.keys(lookupResult).length === 0 ? 'Task not found or expired.' : JSON.stringify(lookupResult, null, 2)}
              </div>
            )}
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '2rem' }}>
          <div className="card">
            <h2 style={{ color: '#7c3aed' }}>🔗 Webhook Manager</h2>
            {webhookInfo?.registered ? (
              <div>
                <div style={{ background: '#f0fdf4', border: '1px solid #86efac', borderRadius: '8px', padding: '1rem', marginBottom: '1rem' }}>
                  <div style={{ fontSize: '0.75rem', fontWeight: 700, color: '#166534', marginBottom: '0.5rem' }}>✓ WEBHOOK REGISTERED</div>
                  <div style={{ fontSize: '0.875rem', color: '#166534', wordBreak: 'break-all' }}>{webhookInfo.url}</div>
                </div>
                <div style={{ background: '#1e293b', borderRadius: '8px', padding: '1rem' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '0.5rem' }}>
                    <span style={{ fontSize: '0.75rem', fontWeight: 700, color: '#94a3b8' }}>HMAC SECRET — keep private!</span>
                    <div style={{ display: 'flex', gap: '0.5rem' }}>
                      <button className="btn btn-secondary" style={{ padding: '0.2rem 0.6rem', fontSize: '0.7rem' }} onClick={() => setShowSecret(!showSecret)}>{showSecret ? <EyeOff size={12} /> : <Eye size={12} />}</button>
                      <CopyButton text={webhookInfo.secret} />
                    </div>
                  </div>
                  <code style={{ fontSize: '0.75rem', color: '#e2e8f0', wordBreak: 'break-all', display: 'block' }}>
                    {showSecret ? webhookInfo.secret : '•'.repeat(64)}
                  </code>
                </div>
                <div style={{ marginTop: '1rem' }}>
                  <label style={{ fontSize: '0.8rem', fontWeight: 600, display: 'block', marginBottom: '0.4rem' }}>Update URL</label>
                  <div style={{ display: 'flex', gap: '0.5rem' }}>
                    <input type="url" value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} placeholder="https://new-url.com/webhook" style={{ flex: 1, padding: '0.6rem', borderRadius: '6px', border: '1px solid var(--border-color)', fontSize: '0.875rem' }} />
                    <button className="btn btn-primary" style={{ padding: '0.6rem 1rem', fontSize: '0.875rem' }} onClick={handleRegisterWebhook}>Update</button>
                  </div>
                </div>
              </div>
            ) : (
              <div>
                <p style={{ color: 'var(--text-secondary)', fontSize: '0.875rem', marginTop: 0 }}>Register a URL to receive signed webhook callbacks when your tasks complete.</p>
                <div className="form-group">
                  <label>Your Webhook URL</label>
                  <input type="url" value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} placeholder="https://your-server.com/webhook" />
                </div>
                <button className="btn btn-primary" style={{ width: '100%' }} onClick={handleRegisterWebhook}>🔗 Register Webhook</button>
              </div>
            )}
            {webhookStatus && <div className={`status-box ${webhookStatus.type}`} style={{ marginTop: '1rem' }}>{webhookStatus.msg}</div>}
          </div>

          <div className="card" style={{ flex: 1 }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
              <h2 style={{ color: 'var(--danger-color)', margin: 0, display: 'flex', alignItems: 'center', gap: '0.5rem' }}><AlertOctagon size={20} /> Dead Letter Queue</h2>
              {dlqTasks.length > 0 && (<button onClick={() => fetch(`${API_URL}/dlq/purge_all`, { method: 'POST', headers: { 'Authorization': `Bearer ${apiKey}` } })} className="btn btn-danger" style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}>Purge All</button>)}
            </div>
            {dlqTasks.length === 0 ? (<p style={{ color: 'var(--text-secondary)' }}>✓ The queue is healthy. No failed tasks!</p>) : (
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
