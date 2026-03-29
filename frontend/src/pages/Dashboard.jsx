import { useState, useEffect } from 'react'
import { api } from '../api'
import { useNavigate } from 'react-router-dom'

export default function Dashboard() {
  const [stats, setStats] = useState(null)
  const [pipelines, setPipelines] = useState([])
  const [deps, setDeps] = useState([])
  const [loading, setLoading] = useState(true)
  const navigate = useNavigate()

  useEffect(() => {
    Promise.all([api.getStats(), api.getPipelines(), api.getDependencies()])
      .then(([s, p, d]) => { setStats(s); setPipelines(p); setDeps(d) })
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  const statCards = [
    { label: 'Pipelines', value: stats?.total_pipelines ?? 0, type: 'accent' },
    { label: 'Connections', value: stats?.total_connections ?? 0, type: 'neutral' },
    { label: 'Runs Today', value: stats?.runs_today ?? 0, type: 'success' },
    { label: 'Failures Today', value: stats?.failed_today ?? 0, type: stats?.failed_today > 0 ? 'error' : 'neutral' },
    { label: 'Anomalies (7d)', value: stats?.anomalies_7d ?? 0, type: stats?.anomalies_7d > 0 ? 'warn' : 'neutral' },
  ]

  return (
    <>
      <div className="page-header">
        <div>
          <div className="page-title">Dashboard</div>
          <div className="page-subtitle">Platform overview and pipeline health</div>
        </div>
        <button className="btn btn-primary" onClick={() => navigate('/pipelines')}>
          + New Pipeline
        </button>
      </div>
      <div className="page-body">
        {/* Stats */}
        <div className="stats-grid">
          {statCards.map(s => (
            <div key={s.label} className={`stat-card ${s.type}`}>
              <div className="stat-label">{s.label}</div>
              <div className="stat-value">{s.value}</div>
            </div>
          ))}
        </div>

        <div className="grid-2">
          {/* Recent Pipelines */}
          <div className="card">
            <div className="card-header">
              <div className="card-title">Recent Pipelines</div>
              <button className="btn btn-secondary btn-sm" onClick={() => navigate('/pipelines')}>
                View All
              </button>
            </div>
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Pipeline</th>
                    <th>Last Run</th>
                    <th>Rows</th>
                    <th>Status</th>
                  </tr>
                </thead>
                <tbody>
                  {pipelines.slice(0, 6).map(p => (
                    <tr key={p.id} style={{ cursor: 'pointer' }} onClick={() => navigate('/monitor')}>
                      <td>
                        <div style={{ color: 'var(--text)', fontWeight: 500 }}>{p.name}</div>
                        <div style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--mono)' }}>
                          {p.source_table}
                        </div>
                      </td>
                      <td>
                        {p.last_run_at
                          ? new Date(p.last_run_at).toLocaleDateString()
                          : <span className="text-muted">—</span>}
                      </td>
                      <td>
                        <span className="mono" style={{ fontSize: 13 }}>
                          {p.last_rows_loaded?.toLocaleString() ?? '—'}
                        </span>
                      </td>
                      <td><StatusBadge status={p.last_status} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Dependency Graph */}
          <div className="card">
            <div className="card-header">
              <div className="card-title">Pipeline Dependencies</div>
            </div>
            {deps.length === 0 ? (
              <div className="empty-state" style={{ padding: '30px 20px' }}>
                <div className="empty-title">No dependencies</div>
                <div className="empty-desc">Set pipeline dependencies when creating pipelines</div>
              </div>
            ) : (
              <DependencyGraph deps={deps} pipelines={pipelines} />
            )}
          </div>
        </div>
      </div>
    </>
  )
}

function StatusBadge({ status }) {
  if (!status) return <span className="badge badge-neutral">—</span>
  const map = {
    complete: ['badge-success', '✓ complete'],
    failed: ['badge-error', '✗ failed'],
    running: ['badge-running', '● running'],
    pending: ['badge-neutral', '○ pending'],
  }
  const [cls, label] = map[status] || ['badge-neutral', status]
  return <span className={`badge ${cls}`}>{label}</span>
}

function DependencyGraph({ deps, pipelines }) {
  // Build adjacency: find roots (nothing depends on them)
  const allIds = new Set(pipelines.map(p => p.id))
  const hasDependents = new Set(deps.map(d => d.depends_on_id))
  const isDependency = new Set(deps.map(d => d.pipeline_id))

  // Build chains
  const chains = []
  const visited = new Set()

  deps.forEach(dep => {
    if (!visited.has(dep.depends_on_id)) {
      const chain = [dep.depends_on_name, dep.pipeline_name]
      visited.add(dep.depends_on_id)
      visited.add(dep.pipeline_id)
      chains.push(chain)
    }
  })

  return (
    <div className="dep-graph">
      {chains.map((chain, i) => (
        <div key={i} className="dep-row">
          {chain.map((name, j) => (
            <>
              <div key={name} className={`dep-node ${j === 0 ? 'root' : ''}`}>{name}</div>
              {j < chain.length - 1 && <div className="dep-arrow">→</div>}
            </>
          ))}
        </div>
      ))}
      {pipelines
        .filter(p => !isDependency.has(p.id) && !hasDependents.has(p.id))
        .map(p => (
          <div key={p.id} className="dep-row">
            <div className="dep-node">{p.name}</div>
            <div style={{ fontSize: 11, color: 'var(--text-muted)' }}>— no dependencies</div>
          </div>
        ))}
    </div>
  )
}
