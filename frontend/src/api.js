const BASE = import.meta.env.VITE_API_URL || 'http://localhost:8000'

async function request(path, options = {}) {
  const res = await fetch(`${BASE}${path}`, {
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
    body: options.body ? JSON.stringify(options.body) : undefined
  })
  if (!res.ok) {
    const err = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error(err.detail || 'Request failed')
  }
  return res.json()
}

export const api = {
  // Stats
  getStats: () => request('/stats'),

  // Connections
  getConnections: () => request('/connections'),
  createConnection: (data) => request('/connections', { method: 'POST', body: data }),
  updateConnection: (id, data) => request(`/connections/${id}`, { method: 'PATCH', body: data }),
  deleteConnection: (id) => request(`/connections/${id}`, { method: 'DELETE' }),

  // Schema
  getSchema: (connId, refresh = false) =>
    request(`/connections/${connId}/schema${refresh ? '?refresh=true' : ''}`),

  // Pipelines
  getPipelines: () => request('/pipelines'),
  getPipeline: (id) => request(`/pipelines/${id}`),
  createPipeline: (data) => request('/pipelines', { method: 'POST', body: data }),
  updatePipeline: (id, data) => request(`/pipelines/${id}`, { method: 'PATCH', body: data }),
  deletePipeline: (id) => request(`/pipelines/${id}`, { method: 'DELETE' }),
  triggerPipeline: (id) => request(`/pipelines/${id}/trigger`, { method: 'POST' }),

  // Runs
  getRuns: (pipelineId, limit = 30) =>
    request(`/pipelines/${pipelineId}/runs?limit=${limit}`),
  getVolumeHistory: (pipelineId) =>
    request(`/pipelines/${pipelineId}/volume-history`),

  // Dependencies
  getDependencies: () => request('/dependencies'),

  // Promotion
  exportPipeline: (id) => request(`/pipelines/${id}/export`),
  promotePipeline: (id, targetBackendUrl) =>
    request(`/pipelines/${id}/promote`, { method: 'POST', body: { target_backend_url: targetBackendUrl } }),

  // Schema drift
  getPipelineDrift: (pipelineId) => request(`/pipelines/${pipelineId}/drift`),
  getAllPendingDrift: () => request('/drift/pending'),
  resolveDrift: (driftId, action) =>
    request(`/drift/${driftId}`, { method: 'PATCH', body: { action } }),
}
