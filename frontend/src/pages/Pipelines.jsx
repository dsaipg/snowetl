import { useState, useEffect } from 'react'
import { api } from '../api'

export default function Pipelines() {
  const [pipelines, setPipelines] = useState([])
  const [showCreate, setShowCreate] = useState(false)
  const [loading, setLoading] = useState(true)
  const [triggering, setTriggering] = useState({})
  const [runResults, setRunResults] = useState({})

  const load = () => api.getPipelines().then(setPipelines).finally(() => setLoading(false))
  useEffect(() => { load() }, [])

  const trigger = async (pipeline) => {
    setTriggering(t => ({ ...t, [pipeline.id]: true }))
    setRunResults(r => ({ ...r, [pipeline.id]: null }))
    try {
      const result = await api.triggerPipeline(pipeline.id)
      setRunResults(r => ({ ...r, [pipeline.id]: { ok: true, ...result } }))
      load()
    } catch (e) {
      setRunResults(r => ({ ...r, [pipeline.id]: { ok: false, error: e.message } }))
    } finally {
      setTriggering(t => ({ ...t, [pipeline.id]: false }))
    }
  }

  const deletePipeline = async (id) => {
    if (!confirm('Delete this pipeline?')) return
    await api.deletePipeline(id)
    load()
  }

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  return (
    <>
      <div className="page-header">
        <div>
          <div className="page-title">Pipelines</div>
          <div className="page-subtitle">Configure and manage ELT pipelines</div>
        </div>
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
          + New Pipeline
        </button>
      </div>

      <div className="page-body">
        {pipelines.length === 0 ? (
          <div className="card">
            <div className="empty-state">
              <div className="empty-icon">⟶</div>
              <div className="empty-title">No pipelines yet</div>
              <div className="empty-desc">Create your first pipeline to start loading data</div>
              <button className="btn btn-primary" style={{ marginTop: 20 }} onClick={() => setShowCreate(true)}>
                + Create Pipeline
              </button>
            </div>
          </div>
        ) : (
          <div className="card">
            <div className="table-wrap">
              <table>
                <thead>
                  <tr>
                    <th>Pipeline</th>
                    <th>Source → Target</th>
                    <th>Destination</th>
                    <th>Schedule</th>
                    <th>Last Run</th>
                    <th>Last Rows</th>
                    <th>Status</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {pipelines.map(p => (
                    <>
                      <tr key={p.id}>
                        <td>
                          <div style={{ color: 'var(--text)', fontWeight: 500 }}>{p.name}</div>
                          {p.depends_on?.length > 0 && (
                            <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 3 }}>
                              depends on {p.depends_on.length} pipeline(s)
                            </div>
                          )}
                        </td>
                        <td>
                          <div style={{ fontFamily: 'var(--mono)', fontSize: 12 }}>
                            <span style={{ color: 'var(--text-dim)' }}>{p.source_table}</span>
                            <span style={{ color: 'var(--text-muted)', margin: '0 6px' }}>→</span>
                            <span style={{ color: 'var(--accent)' }}>{p.target_schema_name || 'ELT_STAGING'}.{p.target_table}</span>
                          </div>
                          <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 2 }}>
                            {p.connection_name}
                          </div>
                        </td>
                        <td>
                          {p.destination_name
                            ? <span style={{ fontSize: 12, color: '#29b6f6', fontFamily: 'var(--mono)' }}>❄ {p.destination_name}</span>
                            : <span style={{ fontSize: 12, color: 'var(--text-muted)' }}>mock (local)</span>
                          }
                        </td>
                        <td>
                          <span className="tag">{p.cron_expression || '0 2 * * *'}</span>
                        </td>
                        <td>
                          {p.last_run_at
                            ? <span style={{ fontSize: 12 }}>{new Date(p.last_run_at).toLocaleString()}</span>
                            : <span className="text-muted">Never</span>}
                        </td>
                        <td>
                          <span className="mono">{p.last_rows_loaded?.toLocaleString() ?? '—'}</span>
                        </td>
                        <td><StatusBadge status={p.last_status} /></td>
                        <td>
                          <div style={{ display: 'flex', gap: 6 }}>
                            <button
                              className="btn btn-secondary btn-sm"
                              onClick={() => trigger(p)}
                              disabled={triggering[p.id]}
                            >
                              {triggering[p.id] ? '⟳ Running...' : '▷ Run'}
                            </button>
                            <button
                              className="btn btn-danger btn-sm"
                              onClick={() => deletePipeline(p.id)}
                            >✕</button>
                          </div>
                        </td>
                      </tr>
                      {runResults[p.id] && (
                        <tr>
                          <td colSpan={8} style={{ padding: '8px 16px' }}>
                            {runResults[p.id].ok ? (
                              <div className="alert alert-success">
                                ✓ Run complete — {runResults[p.id].rows_loaded?.toLocaleString()} rows loaded
                                {runResults[p.id].volume_anomaly && ' ⚠ Volume anomaly detected'}
                              </div>
                            ) : (
                              <div className="alert alert-error">
                                ✗ Run failed: {runResults[p.id].error}
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>

      {showCreate && (
        <CreatePipelineModal
          onClose={() => setShowCreate(false)}
          onCreated={() => { setShowCreate(false); load() }}
        />
      )}
    </>
  )
}

function StatusBadge({ status }) {
  if (!status) return <span className="badge badge-neutral">— never run</span>
  const map = {
    complete: ['badge-success', '✓ complete'],
    failed: ['badge-error', '✗ failed'],
    running: ['badge-running', '● running'],
    pending: ['badge-neutral', '○ pending'],
  }
  const [cls, label] = map[status] || ['badge-neutral', status]
  return <span className={`badge ${cls}`}>{label}</span>
}

function CreatePipelineModal({ onClose, onCreated }) {
  const [step, setStep] = useState(1)
  const [connections, setConnections] = useState([])
  const [pipelines, setPipelines] = useState([])
  const [schema, setSchema] = useState(null)
  const [schemaLoading, setSchemaLoading] = useState(false)
  const [expandedTables, setExpandedTables] = useState({})
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const [form, setForm] = useState({
    name: '',
    connection_id: '',
    destination_connection_id: '',
    source_table: '',
    watermark_column: '',
    merge_key_column: '',
    target_schema_name: 'ELT_STAGING',
    target_table: '',
    cron_expression: '0 2 * * *',
    depends_on: [],
    volume_alert_upper_pct: 200,
    volume_alert_lower_pct: 50,
  })

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  const sources = connections.filter(c => c.db_type !== 'snowflake')
  const destinations = connections.filter(c => c.db_type === 'snowflake')

  useEffect(() => {
    Promise.all([api.getConnections(), api.getPipelines()])
      .then(([c, p]) => { setConnections(c); setPipelines(p) })
  }, [])

  const loadSchema = async (connId) => {
    if (!connId) return
    setSchemaLoading(true)
    setSchema(null)
    try {
      const s = await api.getSchema(connId)
      setSchema(s)
    } catch (e) {
      setError('Schema load failed: ' + e.message)
    } finally {
      setSchemaLoading(false)
    }
  }

  const selectTable = (tableName) => {
    set('source_table', tableName)
    set('target_table', tableName)
    const table = schema?.find(t => t.table_name === tableName)
    const wm = table?.columns.find(c => c.is_suggested_watermark)
    if (wm) set('watermark_column', wm.name)
    const pk = table?.columns.find(c => c.name === 'id')
    if (pk) set('merge_key_column', pk.name)
  }

  const toggleDep = (id) => {
    set('depends_on', form.depends_on.includes(id)
      ? form.depends_on.filter(d => d !== id)
      : [...form.depends_on, id]
    )
  }

  const submit = async () => {
    setError(null)
    setLoading(true)
    try {
      await api.createPipeline({
        ...form,
        connection_id: parseInt(form.connection_id),
        destination_connection_id: form.destination_connection_id ? parseInt(form.destination_connection_id) : null,
      })
      onCreated()
    } catch (e) {
      setError(e.message)
      setStep(1)
    } finally {
      setLoading(false)
    }
  }

  const selectedDestination = destinations.find(d => d.id === parseInt(form.destination_connection_id))

  const steps = ['Details', 'Source', 'Schedule', 'Review']

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" style={{ maxWidth: 680 }} onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <div className="modal-title">Create Pipeline</div>
          <button className="close-btn" onClick={onClose}>×</button>
        </div>

        {/* Step indicator */}
        <div style={{ padding: '12px 28px', borderBottom: '1px solid var(--border)', display: 'flex', gap: 0 }}>
          {steps.map((s, i) => (
            <div
              key={s}
              style={{
                flex: 1, textAlign: 'center', padding: '6px 0',
                fontSize: 12, fontFamily: 'var(--mono)',
                color: step === i + 1 ? 'var(--accent)' : step > i + 1 ? 'var(--text-dim)' : 'var(--text-muted)',
                borderBottom: `2px solid ${step === i + 1 ? 'var(--accent)' : 'transparent'}`,
              }}
            >
              {step > i + 1 ? '✓ ' : `${i + 1}. `}{s}
            </div>
          ))}
        </div>

        <div className="modal-body">
          {error && <div className="alert alert-error">⚠ {error}</div>}

          {/* Step 1: Basic Details */}
          {step === 1 && (
            <div className="form-grid">
              <div className="form-group">
                <label className="form-label">Pipeline Name</label>
                <input
                  className="form-input"
                  value={form.name}
                  onChange={e => set('name', e.target.value)}
                  placeholder="e.g. Load Daily Orders"
                />
              </div>
              <div className="form-group">
                <label className="form-label">Source Connection</label>
                <select
                  className="form-select"
                  value={form.connection_id}
                  onChange={e => { set('connection_id', e.target.value); loadSchema(e.target.value) }}
                >
                  <option value="">Select a source...</option>
                  {sources.map(c => (
                    <option key={c.id} value={c.id}>{c.name} ({c.db_type})</option>
                  ))}
                </select>
              </div>
              <div className="form-group">
                <label className="form-label">Destination</label>
                <select
                  className="form-select"
                  value={form.destination_connection_id}
                  onChange={e => {
                    set('destination_connection_id', e.target.value)
                    if (e.target.value) set('target_schema_name', 'ELT_STAGING')
                  }}
                >
                  <option value="">Mock (local Postgres)</option>
                  {destinations.map(c => (
                    <option key={c.id} value={c.id}>❄ {c.name}</option>
                  ))}
                </select>
                {destinations.length === 0 && (
                  <div className="form-hint">No Snowflake connections yet — add one on the Connections page</div>
                )}
              </div>
              {form.destination_connection_id && (
                <div className="form-group">
                  <label className="form-label">Target Schema</label>
                  <input
                    className="form-input"
                    value={form.target_schema_name}
                    onChange={e => set('target_schema_name', e.target.value)}
                    placeholder="e.g. ELT_STAGING"
                  />
                  <div className="form-hint">Schema will be created in Snowflake if it doesn't exist</div>
                </div>
              )}
              {pipelines.length > 0 && (
                <div className="form-group">
                  <label className="form-label">Depends On (optional)</label>
                  <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
                    {pipelines.map(p => (
                      <label
                        key={p.id}
                        style={{ display: 'flex', alignItems: 'center', gap: 10, cursor: 'pointer', padding: '8px 12px', background: 'var(--bg)', borderRadius: 6, border: '1px solid var(--border)' }}
                      >
                        <input
                          type="checkbox"
                          checked={form.depends_on.includes(p.id)}
                          onChange={() => toggleDep(p.id)}
                          style={{ accentColor: 'var(--accent)' }}
                        />
                        <span style={{ fontSize: 13, color: 'var(--text-dim)' }}>{p.name}</span>
                        <span style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--mono)', marginLeft: 'auto' }}>
                          {p.source_table}
                        </span>
                      </label>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Step 2: Source Table */}
          {step === 2 && (
            <div>
              {schemaLoading ? (
                <div className="loading"><div className="spinner" /> Loading schema...</div>
              ) : schema ? (
                <div>
                  <div style={{ marginBottom: 16 }}>
                    <div className="form-label" style={{ marginBottom: 8 }}>Select Source Table</div>
                    <div className="schema-tree">
                      {schema.map(table => (
                        <div key={table.table_name} className="schema-table">
                          <div
                            className="schema-table-header"
                            onClick={() => { toggleTable(table.table_name); selectTable(table.table_name) }}
                            style={{ background: form.source_table === table.table_name ? 'var(--accent-dim)' : undefined }}
                          >
                            <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                              <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>
                                {expandedTables[table.table_name] ? '▾' : '▸'}
                              </span>
                              <span className="schema-table-name" style={{ color: form.source_table === table.table_name ? 'var(--accent)' : undefined }}>
                                {table.table_name}
                              </span>
                            </div>
                            <div style={{ display: 'flex', gap: 8, alignItems: 'center' }}>
                              <span className="tag">{table.columns.length} cols</span>
                              {form.source_table === table.table_name && (
                                <span style={{ fontSize: 11, color: 'var(--accent)' }}>✓ selected</span>
                              )}
                            </div>
                          </div>
                          {expandedTables[table.table_name] && (
                            <div className="schema-columns">
                              {table.columns.map(col => (
                                <div key={col.name} className="schema-col">
                                  <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                                    <span className="col-name">{col.name}</span>
                                    {col.is_suggested_watermark && <span className="col-watermark">⧖ watermark</span>}
                                  </div>
                                  <span className="col-type">{col.type}</span>
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      ))}
                    </div>
                  </div>

                  {form.source_table && (
                    <div className="form-grid form-grid-2" style={{ marginTop: 16 }}>
                      <div className="form-group">
                        <label className="form-label">Watermark Column</label>
                        <select
                          className="form-select"
                          value={form.watermark_column}
                          onChange={e => set('watermark_column', e.target.value)}
                        >
                          <option value="">None (full extract)</option>
                          {schema.find(t => t.table_name === form.source_table)?.columns.map(col => (
                            <option key={col.name} value={col.name}>
                              {col.name} {col.is_suggested_watermark ? '⧖' : ''}
                            </option>
                          ))}
                        </select>
                        <div className="form-hint">Used for incremental extraction</div>
                      </div>
                      <div className="form-group">
                        <label className="form-label">Merge Key Column</label>
                        <select
                          className="form-select"
                          value={form.merge_key_column}
                          onChange={e => set('merge_key_column', e.target.value)}
                        >
                          <option value="">Select...</option>
                          {schema.find(t => t.table_name === form.source_table)?.columns.map(col => (
                            <option key={col.name} value={col.name}>{col.name}</option>
                          ))}
                        </select>
                        <div className="form-hint">Primary key for upsert / idempotency</div>
                      </div>
                      <div className="form-group" style={{ gridColumn: '1 / -1' }}>
                        <label className="form-label">Target Table Name</label>
                        <input
                          className="form-input"
                          value={form.target_table}
                          onChange={e => set('target_table', e.target.value)}
                          placeholder="Table name in destination"
                        />
                        <div className="form-hint">
                          Will land at: <span style={{ fontFamily: 'var(--mono)', color: 'var(--accent)' }}>
                            {selectedDestination ? `${selectedDestination.db_name}.${form.target_schema_name}` : 'snowflake_target'}.{form.target_table || '…'}
                          </span>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ) : (
                <div className="alert alert-warn">⚠ No schema loaded. Go back and select a source connection.</div>
              )}
            </div>
          )}

          {/* Step 3: Schedule */}
          {step === 3 && (
            <div className="form-grid">
              <div className="form-group">
                <label className="form-label">Cron Schedule</label>
                <select
                  className="form-select"
                  value={form.cron_expression}
                  onChange={e => set('cron_expression', e.target.value)}
                >
                  <option value="0 2 * * *">Daily at 2:00 AM</option>
                  <option value="0 3 * * *">Daily at 3:00 AM</option>
                  <option value="0 0 * * *">Daily at Midnight</option>
                  <option value="0 */6 * * *">Every 6 hours</option>
                  <option value="0 * * * *">Hourly</option>
                  <option value="0 2 * * 1">Weekly (Monday 2am)</option>
                </select>
              </div>
              <div className="form-grid form-grid-2">
                <div className="form-group">
                  <label className="form-label">Volume Alert: Lower Threshold</label>
                  <input className="form-input" type="number" value={form.volume_alert_lower_pct} onChange={e => set('volume_alert_lower_pct', parseInt(e.target.value))} />
                  <div className="form-hint">Alert if rows &lt; {form.volume_alert_lower_pct}% of 7-day average</div>
                </div>
                <div className="form-group">
                  <label className="form-label">Volume Alert: Upper Threshold</label>
                  <input className="form-input" type="number" value={form.volume_alert_upper_pct} onChange={e => set('volume_alert_upper_pct', parseInt(e.target.value))} />
                  <div className="form-hint">Alert if rows &gt; {form.volume_alert_upper_pct}% of 7-day average</div>
                </div>
              </div>
            </div>
          )}

          {/* Step 4: Review */}
          {step === 4 && (
            <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
              {[
                ['Pipeline Name', form.name],
                ['Source Connection', sources.find(c => c.id === parseInt(form.connection_id))?.name || '—'],
                ['Source Table', form.source_table],
                ['Destination', selectedDestination ? `❄ ${selectedDestination.name}` : 'Mock (local Postgres)'],
                ['Target', `${selectedDestination ? `${selectedDestination.db_name}.${form.target_schema_name}` : 'snowflake_target'}.${form.target_table}`],
                ['Watermark Column', form.watermark_column || 'None (full extract)'],
                ['Merge Key', form.merge_key_column || 'Not set'],
                ['Schedule', form.cron_expression],
                ['Dependencies', form.depends_on.length > 0 ? `${form.depends_on.length} pipeline(s)` : 'None'],
                ['Volume Alerts', `${form.volume_alert_lower_pct}% — ${form.volume_alert_upper_pct}%`],
              ].map(([label, value]) => (
                <div key={label} style={{ display: 'flex', justifyContent: 'space-between', padding: '10px 14px', background: 'var(--bg)', borderRadius: 6, border: '1px solid var(--border)' }}>
                  <span style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--mono)' }}>{label}</span>
                  <span style={{ fontSize: 13, color: 'var(--text)', fontFamily: 'var(--mono)' }}>{value}</span>
                </div>
              ))}
            </div>
          )}
        </div>

        <div className="modal-footer">
          {step > 1 && (
            <button className="btn btn-secondary" onClick={() => setStep(s => s - 1)}>← Back</button>
          )}
          <button className="btn btn-secondary" onClick={onClose}>Cancel</button>
          {step < 4 ? (
            <button
              className="btn btn-primary"
              onClick={() => setStep(s => s + 1)}
              disabled={
                (step === 1 && (!form.name || !form.connection_id)) ||
                (step === 2 && !form.source_table)
              }
            >
              Next →
            </button>
          ) : (
            <button className="btn btn-primary" onClick={submit} disabled={loading}>
              {loading ? 'Creating...' : '✓ Create Pipeline'}
            </button>
          )}
        </div>
      </div>
    </div>
  )

  function toggleTable(name) {
    setExpandedTables(prev => ({ ...prev, [name]: !prev[name] }))
  }
}
