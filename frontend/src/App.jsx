import { useState, useEffect } from 'react'

// Detect if we are running locally via Vite or on the Cloud via FastAPI
const API_URL = import.meta.env.DEV ? 'http://localhost:8000' : '';

function App() {
  const [metrics, setMetrics] = useState({
    pending: 0, processing: 0, delayed: 0, dlq: 0
  });

  // Task Submission State
  const [taskName, setTaskName] = useState('send_email'); // Default to send_email
  const [taskArgs, setTaskArgs] = useState('50'); // For matrix or custom
  
  // Specific state for the Email Form
  const [emailTo, setEmailTo] = useState('user@example.com');
  const [emailTitle, setEmailTitle] = useState('Welcome!');
  const [emailBody, setEmailBody] = useState('Hello from PyTaskQ background worker.');
  
  const [delaySeconds, setDelaySeconds] = useState('0');
  const [webhookUrl, setWebhookUrl] = useState('');
  const [submitStatus, setSubmitStatus] = useState(null); // { type: 'success' | 'error', msg: '' }

  // Task Lookup State
  const [lookupId, setLookupId] = useState('');
  const [lookupResult, setLookupResult] = useState(null);

  // DLQ State
  const [dlqTasks, setDlqTasks] = useState([]);

  useEffect(() => {
    const fetchMetrics = async () => {
      try {
        const resMetrics = await fetch(`${API_URL}/metrics`);
        setMetrics(await resMetrics.json());
        
        const resDlq = await fetch(`${API_URL}/dlq`);
        const dlqData = await resDlq.json();
        setDlqTasks(dlqData.tasks || []);
      } catch (error) {
        console.error("Error fetching data:", error);
      }
    };

    fetchMetrics();
    const interval = setInterval(fetchMetrics, 2000);
    return () => clearInterval(interval);
  }, []);

  const handleEnqueue = async (e) => {
    e.preventDefault();
    setSubmitStatus({ type: 'info', msg: 'Submitting...' });
    
    let parsedArgs = [];
    
    // Dynamically build the arguments array based on which task is selected
    if (taskName === 'send_email') {
      parsedArgs = [emailTo, emailTitle, emailBody];
    } else if (taskName === 'matrix_multiply') {
      parsedArgs = [Number(taskArgs) || 10];
    } else {
      // Custom task logic (comma separated)
      parsedArgs = taskArgs.split(',').map(arg => {
        const trimmed = arg.trim();
        return isNaN(trimmed) ? trimmed : Number(trimmed);
      }).filter(arg => arg !== '');
    }

    const delay = parseInt(delaySeconds, 10);
    const endpoint = delay > 0 ? `${API_URL}/task/schedule?delay_seconds=${delay}` : `${API_URL}/task/enqueue`;

    try {
      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          task_name: taskName,
          args: parsedArgs,
          webhook_url: webhookUrl || null
        })
      });
      
      const data = await response.json();
      if (response.ok) {
        setSubmitStatus({ 
          type: 'success', 
          msg: `Success! ${delay > 0 ? 'Scheduled' : 'Enqueued'} with ID: ${data.task_id}` 
        });
      } else {
        setSubmitStatus({ type: 'error', msg: `Error: ${data.detail}` });
      }
    } catch (error) {
      setSubmitStatus({ type: 'error', msg: `Network error: ${error.message}` });
    }
  };

  const handleLookup = async (e) => {
    e.preventDefault();
    if (!lookupId) return;
    try {
      const response = await fetch(`${API_URL}/task/${lookupId}`);
      const data = await response.json();
      setLookupResult(data.result);
    } catch (error) {
      setLookupResult({ error: error.message });
    }
  };

  const handleReplay = async (taskId) => {
    try {
      await fetch(`${API_URL}/dlq/replay/${taskId}`, { method: 'POST' });
    } catch (err) { console.error(err); }
  };

  const handlePurge = async (taskId) => {
    try {
      await fetch(`${API_URL}/dlq/purge/${taskId}`, { method: 'POST' });
    } catch (err) { console.error(err); }
  };

  return (
    <div>
      <header className="header">
        <h1>PyTaskQ Dashboard</h1>
        <p>Monitor your distributed worker queues and API health.</p>
      </header>

      {/* METRICS ROW */}
      <div className="metrics-grid">
        <div className="metric-box pending">
          <div className="metric-title">Pending</div>
          <div className="metric-value" style={{color: 'var(--accent-color)'}}>{metrics.pending}</div>
        </div>
        <div className="metric-box processing">
          <div className="metric-title">Processing</div>
          <div className="metric-value" style={{color: 'var(--success-color)'}}>{metrics.processing}</div>
        </div>
        <div className="metric-box delayed">
          <div className="metric-title">Delayed</div>
          <div className="metric-value" style={{color: 'var(--warning-color)'}}>{metrics.delayed}</div>
        </div>
        <div className="metric-box dlq">
          <div className="metric-title">Dead Letters</div>
          <div className="metric-value" style={{color: 'var(--danger-color)'}}>{metrics.dlq}</div>
        </div>
      </div>

      <div className="dashboard-grid">
        {/* LEFT COLUMN: Controls */}
        <div>
          {/* TASK SUBMISSION CARD */}
          <div className="card" style={{ marginBottom: '2rem' }}>
            <h2>🚀 Dispatch New Task</h2>
            <form onSubmit={handleEnqueue}>
              
              {/* Task Selector Dropdown */}
              <div className="form-group">
                <label>Select Task Type</label>
                <select 
                  value={taskName} 
                  onChange={(e) => setTaskName(e.target.value)} 
                  style={{ padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)', fontSize: '1rem', backgroundColor: 'white' }}
                >
                  <option value="send_email">📧 Send Email</option>
                  <option value="matrix_multiply">🧮 Matrix Multiplication</option>
                  <option value="custom">⚙️ Custom Task...</option>
                </select>
              </div>

              {/* Dynamic Forms based on selection */}
              {taskName === 'send_email' ? (
                <div style={{ backgroundColor: '#f8fafc', padding: '1rem', borderRadius: '8px', border: '1px solid var(--border-color)', marginBottom: '1.25rem' }}>
                  <div className="form-group">
                    <label>Recipient Email</label>
                    <input type="email" value={emailTo} onChange={(e) => setEmailTo(e.target.value)} required />
                  </div>
                  <div className="form-group">
                    <label>Subject Title</label>
                    <input type="text" value={emailTitle} onChange={(e) => setEmailTitle(e.target.value)} required />
                  </div>
                  <div className="form-group" style={{ marginBottom: 0 }}>
                    <label>Email Body</label>
                    <textarea 
                      value={emailBody} 
                      onChange={(e) => setEmailBody(e.target.value)} 
                      style={{ padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)', fontFamily: 'inherit', resize: 'vertical' }} 
                      rows="3" 
                      required
                    />
                  </div>
                </div>
              ) : taskName === 'matrix_multiply' ? (
                <div className="form-group">
                  <label>Matrix Size (N x N)</label>
                  <input type="number" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} required />
                </div>
              ) : (
                <>
                  <div className="form-group">
                    <label>Custom Task Name</label>
                    <input type="text" value={taskName === 'custom' ? '' : taskName} onChange={(e) => setTaskName(e.target.value)} placeholder="e.g. scrape_website" required />
                  </div>
                  <div className="form-group">
                    <label>Arguments (comma separated)</label>
                    <input type="text" value={taskArgs} onChange={(e) => setTaskArgs(e.target.value)} placeholder="arg1, arg2, arg3" />
                  </div>
                </>
              )}
              
              {/* Advanced Feature Inputs */}
              <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '1rem', marginTop: '1rem', paddingTop: '1rem', borderTop: '1px solid var(--border-color)' }}>
                <div className="form-group">
                  <label>Delay (Seconds)</label>
                  <input type="number" min="0" value={delaySeconds} onChange={(e) => setDelaySeconds(e.target.value)} />
                </div>
                <div className="form-group">
                  <label>Webhook URL (Optional)</label>
                  <input type="url" placeholder="https://..." value={webhookUrl} onChange={(e) => setWebhookUrl(e.target.value)} />
                </div>
              </div>

              <button type="submit" className="btn btn-primary" style={{ width: '100%', marginTop: '1rem' }}>Dispatch Task</button>
            </form>

            {submitStatus && (
              <div className={`status-box ${submitStatus.type}`}>
                {submitStatus.msg}
              </div>
            )}
          </div>

          {/* TASK LOOKUP CARD */}
          <div className="card">
            <h2>🔍 Check Task Status</h2>
            <form onSubmit={handleLookup} style={{ display: 'flex', gap: '1rem' }}>
              <input 
                type="text" 
                placeholder="Paste Task ID here..." 
                value={lookupId} 
                onChange={(e) => setLookupId(e.target.value)} 
                style={{ flex: 1, padding: '0.75rem', borderRadius: '6px', border: '1px solid var(--border-color)' }}
              />
              <button type="submit" className="btn btn-secondary">Search</button>
            </form>

            {lookupResult && (
              <div style={{ marginTop: '1rem', padding: '1rem', backgroundColor: '#1e293b', color: '#f8fafc', borderRadius: '6px', fontFamily: 'monospace', overflowX: 'auto', whiteSpace: 'pre-wrap' }}>
                {Object.keys(lookupResult).length === 0 ? "Task not found or expired." : JSON.stringify(lookupResult, null, 2)}
              </div>
            )}
          </div>
        </div>

        {/* RIGHT COLUMN: DLQ */}
        <div>
          <div className="card" style={{ height: '100%' }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '1.5rem' }}>
              <h2 style={{ color: 'var(--danger-color)', margin: 0 }}>☠️ Dead Letter Queue</h2>
              {dlqTasks.length > 0 && (
                <button onClick={() => fetch(`${API_URL}/dlq/purge_all`, { method: 'POST' })} className="btn btn-danger" style={{ padding: '0.5rem 1rem', fontSize: '0.875rem' }}>
                  Purge All
                </button>
              )}
            </div>

            {dlqTasks.length === 0 ? (
              <p style={{ color: 'var(--text-secondary)' }}>The queue is healthy. No failed tasks!</p>
            ) : (
              <div className="table-container">
                <table>
                  <thead>
                    <tr>
                      <th>Task ID</th>
                      <th>Name</th>
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {dlqTasks.map((t, idx) => (
                      <tr key={idx}>
                        <td style={{ fontFamily: 'monospace', fontSize: '0.875rem' }}>{t.task_id.substring(0, 10)}...</td>
                        <td style={{ fontWeight: 600 }}>{t.task_name}</td>
                        <td style={{ display: 'flex', gap: '0.5rem' }}>
                          <button onClick={() => handleReplay(t.task_id)} className="btn btn-secondary" style={{ padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}>Replay</button>
                          <button onClick={() => handlePurge(t.task_id)} className="btn btn-danger" style={{ padding: '0.25rem 0.5rem', fontSize: '0.75rem' }}>Delete</button>
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
  )
}

export default App