import { useState, useEffect, useRef } from 'react'
import { api } from '../api'

const STATUS_META = {
  complete: { color: 'var(--accent)',  label: 'COMPLETE', icon: '✓' },
  running:  { color: '#f59e0b',        label: 'RUNNING',  icon: '◉' },
  failed:   { color: '#ef4444',        label: 'FAILED',   icon: '✕' },
  pending:  { color: '#6b7280',        label: 'PENDING',  icon: '○' },
}

function StatusBadge({ status }) {
  const m = STATUS_META[status] || STATUS_META.pending
  return (
    <span style={{
      color: m.color,
      border: `1px solid ${m.color}33`,
      background: `${m.color}11`,
      borderRadius: 4,
      padding: '2px 10px',
      fontFamily: 'var(--mono)',
      fontSize: 11,
      letterSpacing: 1,
      display: 'inline-flex',
      alignItems: 'center',
      gap: 5,
    }}>
      <span style={{ fontSize: 9 }}>{m.icon}</span>
      {m.label}
    </span>
  )
}

function VolumeChart({ data }) {
  // data = array of { run_date, rows_loaded, run_count, had_failure, had_anomaly }
  const canvasRef = useRef(null)

  useEffect(() => {
    if (!canvasRef.current || !data.length) return
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    const W = canvas.width
    const H = canvas.height
    const chartH = H - 44
    const padL = 42

    const maxRows = Math.max(...data.map(d => d.rows_loaded || 0), 1)
    const barW = (W - padL) / data.length

    ctx.clearRect(0, 0, W, H)

    // Grid
    ctx.strokeStyle = '#ffffff08'
    ctx.lineWidth = 1
    for (let i = 0; i <= 4; i++) {
      const y = 8 + (chartH / 4) * i
      ctx.beginPath(); ctx.moveTo(padL, y); ctx.lineTo(W, y); ctx.stroke()
      ctx.fillStyle = '#ffffff33'
      ctx.font = '9px Space Mono, monospace'
      ctx.fillText(Math.round(maxRows * (1 - i / 4)), 0, y + 4)
    }

    data.forEach((d, i) => {
      const x = padL + i * barW
      const h = ((d.rows_loaded || 0) / maxRows) * chartH

      if (h > 0) {
        const grad = ctx.createLinearGradient(0, chartH + 8 - h, 0, chartH + 8)
        if (d.had_anomaly) {
          grad.addColorStop(0, '#f59e0b')
          grad.addColorStop(1, '#f59e0b44')
        } else if (d.had_failure) {
          grad.addColorStop(0, '#ef4444')
          grad.addColorStop(1, '#ef444444')
        } else {
          grad.addColorStop(0, '#00d4aa')
          grad.addColorStop(1, '#00d4aa33')
        }
        ctx.fillStyle = grad
        ctx.fillRect(x + 1, chartH + 8 - h, barW - 3, h)
      }

      // Label every ~5 bars
      if (i % Math.ceil(data.length / 6) === 0) {
        ctx.fillStyle = '#ffffff33'
        ctx.font = '9px Space Mono, monospace'
        const label = d.run_date ? String(d.run_date).slice(5) : ''
        ctx.fillText(label, x, H - 4)
      }
    })
  }, [data])

  return (
    <div>
      <canvas ref={canvasRef} width={860} height={150}
        style={{ width: '100%', height: 150, display: 'block' }} />
      <div style={{ display: 'flex', gap: 20, marginTop: 10 }}>
        {[
          { color: 'var(--accent)', label: 'Normal' },
          { color: '#f59e0b',       label: 'Volume Anomaly' },
          { color: '#ef4444',       label: 'Had Failure' },
        ].map(({ color, label }) => (
          <span key={label} style={{ fontFamily: 'var(--mono)', fontSize: 11, color, display: 'flex', alignItems: 'center', gap: 6 }}>
            <span style={{ width: 10, height: 10, background: color, borderRadius: 2, display: 'inline-block', opacity: 0.8 }} />
            {label}
          </span>
        ))}
      </div>
    </div>
  )
}

function RunDetailModal({ run, onClose }) {
  if (!run) return null
  return (
    <div style={{
      position: 'fixed', inset: 0, background: '#000000cc',
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      zIndex: 1000, backdropFilter: 'blur(4px)',
    }} onClick={onClose}>
      <div style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 12, width: 640, maxHeight: '80vh',
        overflow: 'hidden', display: 'flex', flexDirection: 'column',
        boxShadow: '0 0 60px #00d4aa22',
      }} onClick={e => e.stopPropagation()}>
        <div style={{
          padding: '20px 24px', borderBottom: '1px solid var(--border)',
          display: 'flex', justifyContent: 'space-between', alignItems: 'center',
        }}>
          <div>
            <div style={{ fontFamily: 'var(--mono)', color: 'var(--accent)', fontSize: 11, letterSpacing: 2, marginBottom: 4 }}>
              RUN DETAIL
            </div>
            <div style={{ fontSize: 18, fontWeight: 600 }}>Run #{run.id}</div>
          </div>
          <button onClick={onClose} style={{
            background: 'transparent', border: '1px solid var(--border)',
            color: 'var(--text-muted)', cursor: 'pointer', borderRadius: 6,
            width: 32, height: 32, fontSize: 16,
          }}>✕</button>
        </div>
        <div style={{ padding: '20px 24px', overflowY: 'auto' }}>
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 20 }}>
            {[
              ['Run ID',        `#${run.id}`],
              ['Status',        <StatusBadge status={run.status} />],
              ['Pipeline',      run._pipelineName || `Pipeline #${run.pipeline_id}`],
              ['Phase',         `${run.phase_completed || 0} / 3`],
              ['Started',       run.started_at ? new Date(run.started_at).toLocaleString() : '—'],
              ['Ended',         run.ended_at   ? new Date(run.ended_at).toLocaleString()   : '—'],
              ['Rows Loaded',   (run.rows_loaded ?? 0).toLocaleString()],
              ['Volume Anomaly', run.volume_anomaly_flag ? '⚠ Yes' : 'No'],
            ].map(([label, val]) => (
              <div key={label} style={{
                background: 'var(--bg)', borderRadius: 8,
                padding: '12px 14px', border: '1px solid var(--border)',
              }}>
                <div style={{ fontFamily: 'var(--mono)', fontSize: 10, color: 'var(--text-muted)', marginBottom: 4, letterSpacing: 1 }}>
                  {label.toUpperCase()}
                </div>
                <div style={{ fontSize: 14 }}>{val}</div>
              </div>
            ))}
          </div>
          {run.error_message && (
            <div>
              <div style={{ fontFamily: 'var(--mono)', fontSize: 10, color: '#ef4444', letterSpacing: 2, marginBottom: 8 }}>ERROR</div>
              <pre style={{
                background: 'var(--bg)', border: '1px solid #ef444433',
                borderRadius: 8, padding: 14, fontFamily: 'var(--mono)',
                fontSize: 12, color: '#ef4444', whiteSpace: 'pre-wrap', wordBreak: 'break-word',
              }}>{run.error_message}</pre>
            </div>
          )}
        </div>
      </div>
    </div>
  )
}

export default function Monitor() {
  const [pipelines, setPipelines]       = useState([])
  const [allRuns, setAllRuns]           = useState([])
  const [volumeData, setVolumeData]     = useState([])
  const [selectedPipeline, setSelectedPipeline] = useState('all')
  const [filterStatus, setFilterStatus] = useState('all')
  const [selectedRun, setSelectedRun]   = useState(null)
  const [loading, setLoading]           = useState(true)
  const [page, setPage]                 = useState(0)
  const PAGE_SIZE = 10

  useEffect(() => {
    api.getPipelines()
      .then(async (pipes) => {
        setPipelines(pipes)

        // Fetch runs for all pipelines in parallel
        const runArrays = await Promise.all(
          pipes.map(p =>
            api.getRuns(p.id, 100)
              .then(runs => runs.map(r => ({ ...r, _pipelineName: p.name })))
              .catch(() => [])
          )
        )
        const merged = runArrays.flat().sort(
          (a, b) => new Date(b.started_at) - new Date(a.started_at)
        )
        setAllRuns(merged)

        // Volume history for first pipeline or selected
        if (pipes.length > 0) {
          api.getVolumeHistory(pipes[0].id)
            .then(setVolumeData)
            .catch(() => {})
        }
      })
      .catch(() => {
        // Mock data fallback
        const mockPipes = [
          { id: 1, name: 'Load Customers' },
          { id: 2, name: 'Load Orders' },
          { id: 3, name: 'Load Products' },
        ]
        setPipelines(mockPipes)
        const statuses = ['complete', 'complete', 'complete', 'failed', 'running']
        const mockRuns = Array.from({ length: 35 }, (_, i) => {
          const d = new Date(); d.setHours(d.getHours() - i * 6)
          const pip = mockPipes[i % 3]
          const status = statuses[i % statuses.length]
          const ended = new Date(d.getTime() + (120 + i * 30) * 1000)
          return {
            id: i + 1,
            pipeline_id: pip.id,
            _pipelineName: pip.name,
            status,
            phase_completed: status === 'complete' ? 3 : status === 'failed' ? 1 : 2,
            started_at: d.toISOString(),
            ended_at: status !== 'running' ? ended.toISOString() : null,
            rows_loaded: status === 'complete' ? 800 + i * 47 : 0,
            volume_anomaly_flag: i === 5,
            error_message: status === 'failed' ? 'Connection timeout to source database' : null,
          }
        })
        setAllRuns(mockRuns)

        // Mock volume data
        setVolumeData(Array.from({ length: 30 }, (_, i) => {
          const d = new Date(); d.setDate(d.getDate() - (29 - i))
          return {
            run_date: d.toISOString().slice(0, 10),
            rows_loaded: 800 + Math.floor(Math.random() * 400),
            run_count: 3,
            had_failure: i === 12 ? 1 : 0,
            had_anomaly: i === 20 ? 1 : 0,
          }
        }))
      })
      .finally(() => setLoading(false))
  }, [])

  // Reload volume when pipeline selection changes
  useEffect(() => {
    if (selectedPipeline === 'all' || !pipelines.length) return
    api.getVolumeHistory(Number(selectedPipeline))
      .then(setVolumeData)
      .catch(() => {})
  }, [selectedPipeline])

  const filtered = allRuns.filter(r => {
    if (selectedPipeline !== 'all' && String(r.pipeline_id) !== selectedPipeline) return false
    if (filterStatus !== 'all' && r.status !== filterStatus) return false
    return true
  })
  const paged      = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE)
  const totalPages = Math.ceil(filtered.length / PAGE_SIZE)

  const anomalies = allRuns.filter(r => r.volume_anomaly_flag || r.status === 'failed').slice(0, 6)

  const stats = {
    total:    allRuns.length,
    complete: allRuns.filter(r => r.status === 'complete').length,
    failed:   allRuns.filter(r => r.status === 'failed').length,
    running:  allRuns.filter(r => r.status === 'running').length,
    anomalies: allRuns.filter(r => r.volume_anomaly_flag).length,
  }

  if (loading) return (
    <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', height: '60vh' }}>
      <div style={{ textAlign: 'center' }}>
        <div style={{ fontFamily: 'var(--mono)', color: 'var(--accent)', letterSpacing: 4, marginBottom: 12 }}>LOADING MONITOR</div>
        <div style={{ width: 200, height: 2, background: 'var(--border)', borderRadius: 1, overflow: 'hidden' }}>
          <div style={{ width: '60%', height: '100%', background: 'var(--accent)', borderRadius: 1,
            animation: 'pulse 1.2s ease-in-out infinite' }} />
        </div>
      </div>
    </div>
  )

  return (
    <div style={{ padding: '32px 40px', maxWidth: 1100, margin: '0 auto' }}>
      {/* Header */}
      <div style={{ marginBottom: 32 }}>
        <div style={{ fontFamily: 'var(--mono)', color: 'var(--accent)', fontSize: 11, letterSpacing: 3, marginBottom: 8 }}>
          OBSERVABILITY
        </div>
        <h1 style={{ margin: 0, fontSize: 28, fontWeight: 700, letterSpacing: -0.5 }}>Pipeline Monitor</h1>
        <p style={{ color: 'var(--text-muted)', marginTop: 6, fontSize: 14 }}>
          Job run history, volume trends, and anomaly detection across all pipelines
        </p>
      </div>

      {/* Stats */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(5, 1fr)', gap: 12, marginBottom: 28 }}>
        {[
          { label: 'TOTAL RUNS',  value: stats.total,    color: 'var(--text)' },
          { label: 'COMPLETE',    value: stats.complete,  color: 'var(--accent)' },
          { label: 'FAILED',      value: stats.failed,    color: '#ef4444' },
          { label: 'RUNNING',     value: stats.running,   color: '#f59e0b' },
          { label: 'ANOMALIES',   value: stats.anomalies, color: '#f59e0b' },
        ].map(({ label, value, color }) => (
          <div key={label} style={{
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 10, padding: '16px 18px',
          }}>
            <div style={{ fontFamily: 'var(--mono)', fontSize: 10, color: 'var(--text-muted)', letterSpacing: 2, marginBottom: 8 }}>
              {label}
            </div>
            <div style={{ fontSize: 26, fontWeight: 700, color, fontFamily: 'var(--mono)' }}>{value}</div>
          </div>
        ))}
      </div>

      {/* Volume Chart */}
      <div style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 12, padding: '24px 28px', marginBottom: 28,
      }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 16 }}>
          <div style={{ fontFamily: 'var(--mono)', fontSize: 11, color: 'var(--text-muted)', letterSpacing: 2 }}>
            ROWS LOADED — LAST 30 DAYS
          </div>
          <select
            value={selectedPipeline}
            onChange={e => { setSelectedPipeline(e.target.value); setPage(0) }}
            style={{
              background: 'var(--bg)', border: '1px solid var(--border)',
              color: 'var(--text)', borderRadius: 6, padding: '5px 10px',
              fontFamily: 'var(--mono)', fontSize: 11, cursor: 'pointer',
            }}
          >
            <option value="all">All Pipelines</option>
            {pipelines.map(p => <option key={p.id} value={String(p.id)}>{p.name}</option>)}
          </select>
        </div>
        {volumeData.length > 0
          ? <VolumeChart data={volumeData} />
          : <div style={{ color: 'var(--text-muted)', fontFamily: 'var(--mono)', fontSize: 13, textAlign: 'center', padding: '32px 0' }}>
              NO VOLUME DATA YET — TRIGGER A RUN TO BEGIN
            </div>
        }
      </div>

      {/* Anomaly panel */}
      {anomalies.length > 0 && (
        <div style={{ marginBottom: 28 }}>
          <div style={{ fontFamily: 'var(--mono)', fontSize: 11, color: '#ef4444', letterSpacing: 2, marginBottom: 12 }}>
            ⚠ ANOMALIES &amp; FAILURES ({anomalies.length})
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {anomalies.map(r => (
              <div key={r.id} style={{
                background: '#ef444409', border: '1px solid #ef444433',
                borderRadius: 8, padding: '12px 16px',
                display: 'flex', alignItems: 'center', gap: 12, cursor: 'pointer',
              }} onClick={() => setSelectedRun(r)}>
                <span style={{ color: '#ef4444', fontSize: 18 }}>
                  {r.volume_anomaly_flag ? '◈' : '✕'}
                </span>
                <div style={{ flex: 1 }}>
                  <span style={{ color: '#ef4444', fontFamily: 'var(--mono)', fontSize: 11, letterSpacing: 1 }}>
                    {r.volume_anomaly_flag ? 'VOLUME ANOMALY' : 'RUN FAILURE'} — RUN #{r.id}
                  </span>
                  <span style={{ color: 'var(--text-muted)', fontSize: 13, marginLeft: 12 }}>
                    {r._pipelineName}
                  </span>
                  {r.error_message && (
                    <span style={{ color: 'var(--text-muted)', fontSize: 12, marginLeft: 12 }}>
                      {r.error_message.slice(0, 80)}
                    </span>
                  )}
                </div>
                <StatusBadge status={r.status} />
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Filters + table */}
      <div style={{ display: 'flex', gap: 12, marginBottom: 14, alignItems: 'center' }}>
        <div style={{ fontFamily: 'var(--mono)', fontSize: 11, color: 'var(--text-muted)', letterSpacing: 2 }}>FILTER</div>
        <select
          value={filterStatus}
          onChange={e => { setFilterStatus(e.target.value); setPage(0) }}
          style={{
            background: 'var(--surface)', border: '1px solid var(--border)',
            color: 'var(--text)', borderRadius: 6, padding: '6px 12px',
            fontFamily: 'var(--mono)', fontSize: 12, cursor: 'pointer',
          }}
        >
          <option value="all">All Statuses</option>
          <option value="complete">Complete</option>
          <option value="failed">Failed</option>
          <option value="running">Running</option>
        </select>
        <span style={{ marginLeft: 'auto', fontFamily: 'var(--mono)', fontSize: 12, color: 'var(--text-muted)' }}>
          {filtered.length} runs
        </span>
      </div>

      <div style={{
        background: 'var(--surface)', border: '1px solid var(--border)',
        borderRadius: 12, overflow: 'hidden', marginBottom: 20,
      }}>
        <table style={{ width: '100%', borderCollapse: 'collapse' }}>
          <thead>
            <tr style={{ borderBottom: '1px solid var(--border)' }}>
              {['Run ID', 'Pipeline', 'Status', 'Phase', 'Started', 'Duration', 'Rows Loaded', 'Anomaly'].map(h => (
                <th key={h} style={{
                  padding: '12px 14px', textAlign: 'left',
                  fontFamily: 'var(--mono)', fontSize: 10, color: 'var(--text-muted)',
                  letterSpacing: 2, fontWeight: 600,
                }}>
                  {h.toUpperCase()}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {paged.length === 0 ? (
              <tr><td colSpan={8} style={{ textAlign: 'center', padding: '48px 16px', color: 'var(--text-muted)', fontFamily: 'var(--mono)', fontSize: 13 }}>
                NO RUNS MATCH CURRENT FILTERS
              </td></tr>
            ) : paged.map((run, idx) => {
              const durMs = run.started_at && run.ended_at
                ? new Date(run.ended_at) - new Date(run.started_at)
                : null
              const dur = durMs ? `${Math.round(durMs / 1000)}s` : run.status === 'running' ? 'running…' : '—'

              return (
                <tr key={run.id}
                  onClick={() => setSelectedRun(run)}
                  style={{
                    borderBottom: idx < paged.length - 1 ? '1px solid var(--border)' : 'none',
                    cursor: 'pointer', transition: 'background 0.12s',
                  }}
                  onMouseEnter={e => e.currentTarget.style.background = 'var(--bg)'}
                  onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                >
                  <td style={{ padding: '13px 14px', fontFamily: 'var(--mono)', fontSize: 13, color: 'var(--accent)' }}>#{run.id}</td>
                  <td style={{ padding: '13px 14px', fontSize: 13, maxWidth: 160, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                    {run._pipelineName || `Pipeline #${run.pipeline_id}`}
                  </td>
                  <td style={{ padding: '13px 14px' }}><StatusBadge status={run.status} /></td>
                  <td style={{ padding: '13px 14px', fontFamily: 'var(--mono)', fontSize: 12, color: 'var(--text-muted)' }}>
                    {run.phase_completed || 0} / 3
                  </td>
                  <td style={{ padding: '13px 14px', fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--mono)' }}>
                    {run.started_at ? new Date(run.started_at).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' }) : '—'}
                  </td>
                  <td style={{ padding: '13px 14px', fontSize: 12, fontFamily: 'var(--mono)' }}>{dur}</td>
                  <td style={{ padding: '13px 14px', fontSize: 12, fontFamily: 'var(--mono)' }}>
                    {(run.rows_loaded ?? 0).toLocaleString()}
                  </td>
                  <td style={{ padding: '13px 14px', fontSize: 13 }}>
                    {run.volume_anomaly_flag
                      ? <span style={{ color: '#f59e0b' }}>⚠ Yes</span>
                      : <span style={{ color: 'var(--text-muted)' }}>—</span>}
                  </td>
                </tr>
              )
            })}
          </tbody>
        </table>
      </div>

      {totalPages > 1 && (
        <div style={{ display: 'flex', gap: 8, justifyContent: 'center', alignItems: 'center' }}>
          <button onClick={() => setPage(p => Math.max(0, p - 1))} disabled={page === 0}
            style={{
              background: 'var(--surface)', border: '1px solid var(--border)',
              color: page === 0 ? 'var(--text-muted)' : 'var(--text)',
              borderRadius: 6, padding: '6px 14px', cursor: page === 0 ? 'default' : 'pointer',
              fontFamily: 'var(--mono)', fontSize: 12,
            }}>← PREV</button>
          <span style={{ fontFamily: 'var(--mono)', fontSize: 12, color: 'var(--text-muted)', padding: '0 8px' }}>
            {page + 1} / {totalPages}
          </span>
          <button onClick={() => setPage(p => Math.min(totalPages - 1, p + 1))} disabled={page === totalPages - 1}
            style={{
              background: 'var(--surface)', border: '1px solid var(--border)',
              color: page === totalPages - 1 ? 'var(--text-muted)' : 'var(--text)',
              borderRadius: 6, padding: '6px 14px', cursor: page === totalPages - 1 ? 'default' : 'pointer',
              fontFamily: 'var(--mono)', fontSize: 12,
            }}>NEXT →</button>
        </div>
      )}

      {selectedRun && <RunDetailModal run={selectedRun} onClose={() => setSelectedRun(null)} />}
    </div>
  )
}
