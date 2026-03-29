import { useState, useEffect } from 'react'
import { api } from '../api'

export default function Connections() {
  const [connections, setConnections] = useState([])
  const [showCreate, setShowCreate] = useState(false)
  const [selectedConn, setSelectedConn] = useState(null)
  const [schema, setSchema] = useState(null)
  const [schemaLoading, setSchemaLoading] = useState(false)
  const [expandedTables, setExpandedTables] = useState({})
  const [loading, setLoading] = useState(true)

  const load = () => api.getConnections().then(setConnections).finally(() => setLoading(false))
  useEffect(() => { load() }, [])

  const sources = connections.filter(c => c.db_type !== 'snowflake')
  const destinations = connections.filter(c => c.db_type === 'snowflake')

  const browseSchema = async (conn, refresh = false) => {
    setSelectedConn(conn)
    setSchema(null)
    setSchemaLoading(true)
    try {
      const s = await api.getSchema(conn.id, refresh)
      setSchema(s)
    } catch (e) {
      alert('Schema discovery failed: ' + e.message)
    } finally {
      setSchemaLoading(false)
    }
  }

  const toggleTable = (tableName) => {
    setExpandedTables(prev => ({ ...prev, [tableName]: !prev[tableName] }))
  }

  const deleteConn = async (id) => {
    if (!confirm('Delete this connection?')) return
    await api.deleteConnection(id)
    load()
    if (selectedConn?.id === id) { setSelectedConn(null); setSchema(null) }
  }

  if (loading) return <div className="loading"><div className="spinner" /> Loading...</div>

  return (
    <>
      <div className="page-header">
        <div>
          <div className="page-title">Connections</div>
          <div className="page-subtitle">Manage source and destination connections</div>
        </div>
        <button className="btn btn-primary" onClick={() => setShowCreate(true)}>
          + Add Connection
        </button>
      </div>

      <div className="page-body">
        <div style={{ display: 'grid', gridTemplateColumns: '1fr 1.4fr', gap: 20, alignItems: 'start' }}>
          {/* Connection lists */}
          <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>

            {/* Sources */}
            <div className="card">
              <div className="card-header">
                <div className="card-title">Sources</div>
                <span className="badge badge-neutral">{sources.length}</span>
              </div>
              {sources.length === 0 ? (
                <div className="empty-state">
                  <div className="empty-icon">⌁</div>
                  <div className="empty-title">No source connections</div>
                  <div className="empty-desc">Add a PostgreSQL, MySQL or other source DB</div>
                </div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {sources.map(conn => (
                    <ConnRow
                      key={conn.id}
                      conn={conn}
                      selected={selectedConn?.id === conn.id}
                      onClick={() => browseSchema(conn)}
                      onDelete={() => deleteConn(conn.id)}
                      subtitle={`${conn.db_type} · ${conn.host}:${conn.port}/${conn.db_name}`}
                    />
                  ))}
                </div>
              )}
            </div>

            {/* Destinations */}
            <div className="card">
              <div className="card-header">
                <div className="card-title">Destinations</div>
                <span className="badge badge-neutral">{destinations.length}</span>
              </div>
              {destinations.length === 0 ? (
                <div className="empty-state">
                  <div className="empty-icon">❄</div>
                  <div className="empty-title">No destination connections</div>
                  <div className="empty-desc">Add a Snowflake account to load data into</div>
                </div>
              ) : (
                <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
                  {destinations.map(conn => (
                    <ConnRow
                      key={conn.id}
                      conn={conn}
                      selected={selectedConn?.id === conn.id}
                      onClick={() => { setSelectedConn(conn); setSchema(null) }}
                      onDelete={() => deleteConn(conn.id)}
                      subtitle={`snowflake · ${conn.host} / ${conn.db_name}`}
                      badge={<span className="badge" style={{ background: 'rgba(41,182,246,0.15)', color: '#29b6f6', border: '1px solid rgba(41,182,246,0.3)' }}>❄ snowflake</span>}
                    />
                  ))}
                </div>
              )}
            </div>
          </div>

          {/* Right panel: schema browser or connection detail */}
          <div className="card">
            {!selectedConn ? (
              <>
                <div className="card-header"><div className="card-title">Schema Browser</div></div>
                <div className="empty-state">
                  <div className="empty-icon">◫</div>
                  <div className="empty-title">Select a connection</div>
                  <div className="empty-desc">Click a source connection to browse its tables and columns</div>
                </div>
              </>
            ) : selectedConn.db_type === 'snowflake' ? (
              <>
                <div className="card-header">
                  <div className="card-title">Destination: {selectedConn.name}</div>
                </div>
                <div style={{ padding: '0 4px', display: 'flex', flexDirection: 'column', gap: 10 }}>
                  {[
                    ['Account', selectedConn.host],
                    ['Database', selectedConn.db_name],
                    ['Warehouse', selectedConn.snowflake_warehouse],
                    ['Role', selectedConn.snowflake_role],
                  ].map(([label, value]) => (
                    <div key={label} style={{ display: 'flex', justifyContent: 'space-between', padding: '10px 14px', background: 'var(--bg)', borderRadius: 6, border: '1px solid var(--border)' }}>
                      <span style={{ fontSize: 12, color: 'var(--text-muted)', fontFamily: 'var(--mono)' }}>{label}</span>
                      <span style={{ fontSize: 13, color: 'var(--text)', fontFamily: 'var(--mono)' }}>{value || '—'}</span>
                    </div>
                  ))}
                  <div className="form-hint" style={{ marginTop: 4 }}>
                    Schema discovery is not available for destination connections. Tables are created automatically by pipelines.
                  </div>
                </div>
              </>
            ) : (
              <>
                <div className="card-header">
                  <div className="card-title">Schema: {selectedConn.name}</div>
                  <button
                    className="btn btn-secondary btn-sm"
                    onClick={() => browseSchema(selectedConn, true)}
                    disabled={schemaLoading}
                  >↺ Refresh</button>
                </div>

                {schemaLoading ? (
                  <div className="loading" style={{ padding: 40 }}>
                    <div className="spinner" /> Discovering schema...
                  </div>
                ) : schema ? (
                  <div className="schema-tree">
                    <div style={{ fontSize: 12, color: 'var(--text-muted)', marginBottom: 12, fontFamily: 'var(--mono)' }}>
                      {schema.length} tables found
                    </div>
                    {schema.map(table => (
                      <div key={table.table_name} className="schema-table">
                        <div className="schema-table-header" onClick={() => toggleTable(table.table_name)}>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 10 }}>
                            <span style={{ color: 'var(--text-muted)', fontSize: 12 }}>
                              {expandedTables[table.table_name] ? '▾' : '▸'}
                            </span>
                            <span className="schema-table-name">{table.table_name}</span>
                          </div>
                          <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
                            <span className="tag">{table.columns.length} cols</span>
                            {table.columns.some(c => c.is_suggested_watermark) && (
                              <span style={{ fontSize: 11, color: 'var(--accent)', fontFamily: 'var(--mono)' }}>⧖ watermark</span>
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
                ) : (
                  <div className="empty-state">
                    <div className="empty-desc">Click a connection to browse its schema</div>
                  </div>
                )}
              </>
            )}
          </div>
        </div>
      </div>

      {showCreate && (
        <CreateConnectionModal
          onClose={() => setShowCreate(false)}
          onCreated={() => { setShowCreate(false); load() }}
        />
      )}
    </>
  )
}

function ConnRow({ conn, selected, onClick, onDelete, subtitle, badge }) {
  return (
    <div
      onClick={onClick}
      style={{
        padding: '14px 16px',
        border: `1px solid ${selected ? 'var(--accent-border)' : 'var(--border)'}`,
        borderRadius: 8,
        cursor: 'pointer',
        background: selected ? 'var(--accent-dim)' : 'var(--bg)',
        transition: 'all 0.15s',
      }}
    >
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
        <div>
          <div style={{ color: 'var(--text)', fontWeight: 500, fontSize: 14 }}>{conn.name}</div>
          <div style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--mono)', marginTop: 3 }}>
            {subtitle}
          </div>
        </div>
        <div style={{ display: 'flex', gap: 6, alignItems: 'center' }}>
          {badge || <span className="badge badge-success">● live</span>}
          <button
            className="btn btn-danger btn-sm"
            onClick={e => { e.stopPropagation(); onDelete() }}
          >✕</button>
        </div>
      </div>
    </div>
  )
}

function CreateConnectionModal({ onClose, onCreated }) {
  const [form, setForm] = useState({
    name: '',
    db_type: 'postgresql',
    host: '',
    port: '5432',
    db_name: '',
    db_user: '',
    db_password: '',
    snowflake_warehouse: '',
    snowflake_role: '',
  })
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState(null)

  const set = (k, v) => setForm(f => ({ ...f, [k]: v }))

  const isSnowflake = form.db_type === 'snowflake'

  const handleTypeChange = (type) => {
    set('db_type', type)
    if (type === 'snowflake') {
      set('port', '443')
      set('host', '')
    } else {
      set('port', '5432')
    }
  }

  const submit = async () => {
    setError(null)
    setLoading(true)
    try {
      await api.createConnection({
        ...form,
        port: parseInt(form.port),
        snowflake_warehouse: form.snowflake_warehouse || null,
        snowflake_role: form.snowflake_role || null,
      })
      onCreated()
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal" onClick={e => e.stopPropagation()}>
        <div className="modal-header">
          <div className="modal-title">Add Connection</div>
          <button className="close-btn" onClick={onClose}>×</button>
        </div>
        <div className="modal-body">
          {error && <div className="alert alert-error">⚠ {error}</div>}
          <div className="form-grid">
            <div className="form-group">
              <label className="form-label">Connection Name</label>
              <input className="form-input" value={form.name} onChange={e => set('name', e.target.value)} placeholder="e.g. Snowflake Production" />
            </div>
            <div className="form-group">
              <label className="form-label">Database Type</label>
              <select className="form-select" value={form.db_type} onChange={e => handleTypeChange(e.target.value)}>
                <optgroup label="Sources">
                  <option value="postgresql">PostgreSQL</option>
                  <option value="mysql">MySQL</option>
                  <option value="sqlserver">SQL Server</option>
                  <option value="oracle">Oracle</option>
                </optgroup>
                <optgroup label="Destinations">
                  <option value="snowflake">Snowflake</option>
                </optgroup>
              </select>
            </div>

            {isSnowflake ? (
              <>
                <div className="form-group">
                  <label className="form-label">Account Identifier</label>
                  <input className="form-input" value={form.host} onChange={e => set('host', e.target.value)} placeholder="e.g. LFSHBZM-QBB97092" />
                  <div className="form-hint">Found in your Snowflake URL: https://&lt;account&gt;.snowflakecomputing.com</div>
                </div>
                <div className="form-group">
                  <label className="form-label">Database</label>
                  <input className="form-input" value={form.db_name} onChange={e => set('db_name', e.target.value)} placeholder="e.g. ELT_STAGING" />
                </div>
                <div className="form-grid form-grid-2">
                  <div className="form-group">
                    <label className="form-label">Username</label>
                    <input className="form-input" value={form.db_user} onChange={e => set('db_user', e.target.value)} />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Password</label>
                    <input className="form-input" type="password" value={form.db_password} onChange={e => set('db_password', e.target.value)} />
                  </div>
                </div>
                <div className="form-grid form-grid-2">
                  <div className="form-group">
                    <label className="form-label">Warehouse</label>
                    <input className="form-input" value={form.snowflake_warehouse} onChange={e => set('snowflake_warehouse', e.target.value)} placeholder="e.g. COMPUTE_WH" />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Role</label>
                    <input className="form-input" value={form.snowflake_role} onChange={e => set('snowflake_role', e.target.value)} placeholder="e.g. ACCOUNTADMIN" />
                  </div>
                </div>
              </>
            ) : (
              <>
                <div className="form-grid form-grid-2">
                  <div className="form-group">
                    <label className="form-label">Host</label>
                    <input className="form-input" value={form.host} onChange={e => set('host', e.target.value)} placeholder="hostname or IP" />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Port</label>
                    <input className="form-input" type="number" value={form.port} onChange={e => set('port', e.target.value)} />
                  </div>
                </div>
                <div className="form-group">
                  <label className="form-label">Database Name</label>
                  <input className="form-input" value={form.db_name} onChange={e => set('db_name', e.target.value)} placeholder="database name" />
                </div>
                <div className="form-grid form-grid-2">
                  <div className="form-group">
                    <label className="form-label">Username</label>
                    <input className="form-input" value={form.db_user} onChange={e => set('db_user', e.target.value)} />
                  </div>
                  <div className="form-group">
                    <label className="form-label">Password</label>
                    <input className="form-input" type="password" value={form.db_password} onChange={e => set('db_password', e.target.value)} />
                  </div>
                </div>
              </>
            )}

            <div className="form-hint">
              💡 Connection will be tested before saving.
            </div>
          </div>
        </div>
        <div className="modal-footer">
          <button className="btn btn-secondary" onClick={onClose}>Cancel</button>
          <button className="btn btn-primary" onClick={submit} disabled={loading || !form.name}>
            {loading ? 'Testing connection...' : 'Test & Save'}
          </button>
        </div>
      </div>
    </div>
  )
}
