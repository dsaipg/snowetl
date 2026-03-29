import { BrowserRouter, Routes, Route, NavLink, useLocation } from 'react-router-dom'
import Dashboard from './pages/Dashboard'
import Pipelines from './pages/Pipelines'
import Connections from './pages/Connections'
import Monitor from './pages/Monitor'

function Sidebar() {
  const location = useLocation()

  const navItems = [
    { to: '/', icon: '◈', label: 'Dashboard' },
    { to: '/pipelines', icon: '⟶', label: 'Pipelines' },
    { to: '/connections', icon: '⌁', label: 'Connections' },
    { to: '/monitor', icon: '◉', label: 'Monitor' },
  ]

  return (
    <aside className="sidebar">
      <div className="sidebar-logo">
        <div className="logo-mark">DataFlow</div>
        <div className="logo-sub">ELT Platform</div>
      </div>
      <nav className="sidebar-nav">
        <div className="nav-section">Navigation</div>
        {navItems.map(item => (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.to === '/'}
            className={({ isActive }) => `nav-item ${isActive ? 'active' : ''}`}
          >
            <span className="nav-icon">{item.icon}</span>
            {item.label}
          </NavLink>
        ))}
      </nav>
      <div style={{ padding: '16px', borderTop: '1px solid var(--border)' }}>
        <div style={{ fontSize: 11, color: 'var(--text-muted)', fontFamily: 'var(--mono)' }}>
          ENV: LOCAL
        </div>
        <div style={{ fontSize: 11, color: 'var(--text-muted)', marginTop: 4 }}>
          docker-compose
        </div>
      </div>
    </aside>
  )
}

export default function App() {
  return (
    <BrowserRouter>
      <div className="layout">
        <Sidebar />
        <main className="main-content">
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/pipelines" element={<Pipelines />} />
            <Route path="/connections" element={<Connections />} />
            <Route path="/monitor" element={<Monitor />} />
          </Routes>
        </main>
      </div>
    </BrowserRouter>
  )
}
