# ELT Platform

> A self-service Extract-Load-Transform platform running entirely in Docker Compose. Build pipelines visually, schedule them with Airflow, stage data through MinIO, and land it in a mock Snowflake destination — no cloud accounts required.

---

## Stack

| Service | Technology | Port |
|---|---|---|
| Frontend | React + Vite | 5173 |
| Backend API | FastAPI | 8000 |
| Metadata DB | Postgres | 5432 |
| Object Storage | MinIO (S3 substitute) | 9000 / 9001 |
| Orchestrator | Airflow (MWAA substitute) | 8080 |
| Destination | Mock Snowflake (Postgres schema) | 5432 |

---

## Prerequisites

- **Docker Desktop** ≥ 4.x (or Docker Engine + Compose plugin)
- **Git**
- Optional: `mc` (MinIO client) for manual bucket operations

```bash
# Verify
docker --version        # Docker version 24+
docker compose version  # Docker Compose version v2+
```

---

## Quick Start

### 1. Clone & enter the project

```bash
git clone <your-repo-url> elt-platform
cd elt-platform
```

### 2. Run the startup script (recommended)

The script starts all containers, waits for health checks, creates the MinIO bucket, and opens your browser automatically.

```bash
chmod +x start.sh
./start.sh
```

> **Windows users:** run via Git Bash or WSL 2, or follow the manual steps below.

### 3. Manual start (alternative)

```bash
# Start the stack
docker compose up -d --build

# Create the MinIO staging bucket
# Option A — using mc (MinIO client)
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/elt-staging

# Option B — using the MinIO console
# Open http://localhost:9001, log in as minioadmin / minioadmin, create bucket 'elt-staging'

# Open the platform
open http://localhost:5173   # macOS
xdg-open http://localhost:5173  # Linux
```

---

## Service URLs

| Service | URL | Credentials |
|---|---|---|
| **Frontend** | http://localhost:5173 | — |
| **API Docs** | http://localhost:8000/docs | — |
| **Airflow** | http://localhost:8080 | `admin` / `admin` |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` |

---

## Building Your First Pipeline

1. **Add a connection** → _Connections_ page → `+ New Connection`
   - Source: type `postgres`, host `source-db`, port `5432`, database `sourcedb`, user `postgres`, password `postgres`
   - Destination: type `postgres`, host `postgres`, port `5432`, database `eltplatform`, user `postgres`, password `postgres`

2. **Discover schema** → click `Discover Schema` on your source connection to browse tables.

3. **Create a pipeline** → _Pipelines_ page → `+ New Pipeline`, follow the 4-step wizard:
   - Choose source connection + tables
   - Choose destination connection
   - Set a cron schedule (e.g. `@hourly`) or leave as manual
   - Review & save

4. **Trigger a run** → click `▶ Run` on any pipeline card.

5. **Monitor** → _Monitor_ page shows run history, a 30-day volume chart, and anomaly flags.

---

## Project Structure

```
.
├── docker-compose.yml          # Full stack wiring
├── start.sh                    # One-command startup script
│
├── backend/
│   └── main.py                 # FastAPI — connections, pipelines, runs, stats
│
├── frontend/
│   ├── src/
│   │   ├── App.jsx             # Routing + sidebar
│   │   ├── api.js              # API client abstraction
│   │   ├── index.css           # Design system (CSS variables)
│   │   └── pages/
│   │       ├── Dashboard.jsx   # Stats, pipeline list, dependency graph
│   │       ├── Connections.jsx # Connection management + schema browser
│   │       ├── Pipelines.jsx   # Pipeline CRUD + 4-step create modal
│   │       └── Monitor.jsx     # Run history, volume chart, anomalies
│
├── airflow/
│   └── dags/
│       └── pipeline_dag.py     # Generic DAG factory (one DAG per pipeline)
│
├── postgres-init/
│   └── 01_schema.sql           # Platform metadata schema + seed data
│
└── mock-data/
    └── 01_init.sql             # Source DB (orders, customers, products, transactions)
```

---

## Airflow DAGs

The `pipeline_dag.py` file dynamically generates one Airflow DAG per pipeline at import time by querying the FastAPI backend. Each DAG follows this task graph:

```
start_run → extract → transform → load → finish_run
```

| Task | What it does |
|---|---|
| `start_run` | Registers the run in the backend, pushes `run_id` to XCom |
| `extract` | Queries source Postgres, stages JSON to MinIO |
| `transform` | Applies column renames, row filters, computed columns from pipeline config |
| `load` | Writes transformed data to destination schema (UPSERT if PK defined, else truncate-insert) |
| `finish_run` | Marks run `SUCCESS` in backend; `fail_run` callback marks `FAILED` on any error |

DAGs are triggered automatically on their configured schedule or manually from the Airflow UI / API.

---

## Environment Variables

Override any default by creating a `.env` file in the project root:

```env
# MinIO
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=elt-staging

# Backend
BACKEND_URL=http://localhost:8000

# Frontend
FRONTEND_URL=http://localhost:5173

# Postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=eltplatform
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
```

---

## Stopping the Platform

```bash
# Stop containers (preserves volumes / data)
docker compose down

# Stop and delete all data
docker compose down -v
```

---

## Troubleshooting

**Airflow keeps restarting**
Airflow needs its database to be ready first. Wait 60–90 seconds after `docker compose up` for the init container to finish, then check `docker compose logs airflow-webserver`.

**MinIO bucket not found**
Run `./start.sh` again (it's idempotent) or create the bucket manually in the MinIO console at http://localhost:9001.

**Backend returns 500 on `/pipelines`**
Ensure the Postgres init scripts ran. Check with:
```bash
docker compose logs postgres
```

**Port conflict**
If any port is already in use, edit `docker-compose.yml` and change the host-side port (left side of `:`).

---

## Design System

The frontend uses CSS custom properties defined in `src/index.css`:

```css
--accent:   #00d4aa;   /* teal highlight */
--bg:       #0a0a0f;   /* near-black background */
--surface:  #111118;   /* card / panel background */
--border:   #1e1e2e;   /* subtle dividers */
--text:     #e8e8f0;   /* primary text */
--text-muted: #6b6b80; /* secondary text */
--mono:     'Space Mono', monospace;
```

---

## License

MIT — build, fork, extend freely.
