#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# start.sh — ELT Platform Startup Script
# Waits for all services, creates MinIO bucket, then opens the browser.
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BOLD='\033[1m'
RESET='\033[0m'

# ── Config ────────────────────────────────────────────────────────────────────
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.yml}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://localhost:9000}"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"
MINIO_BUCKET="${MINIO_BUCKET:-elt-staging}"
BACKEND_URL="${BACKEND_URL:-http://localhost:8000}"
FRONTEND_URL="${FRONTEND_URL:-http://localhost:5173}"
AIRFLOW_URL="${AIRFLOW_URL:-http://localhost:8080}"
MAX_WAIT=120  # seconds

# ── Helpers ───────────────────────────────────────────────────────────────────
log()   { echo -e "${CYAN}[ELT]${RESET} $*"; }
ok()    { echo -e "${GREEN}[✓]${RESET}  $*"; }
warn()  { echo -e "${YELLOW}[!]${RESET}  $*"; }
err()   { echo -e "${RED}[✕]${RESET}  $*"; }
die()   { err "$*"; exit 1; }

wait_for_url() {
  local name="$1" url="$2" waited=0
  log "Waiting for ${name} at ${url} ..."
  until curl -sf --max-time 3 "${url}" > /dev/null 2>&1; do
    if (( waited >= MAX_WAIT )); then
      die "${name} did not become ready within ${MAX_WAIT}s"
    fi
    printf "."
    sleep 2
    (( waited += 2 )) || true
  done
  echo ""
  ok "${name} is ready"
}

wait_for_tcp() {
  local name="$1" host="$2" port="$3" waited=0
  log "Waiting for ${name} (${host}:${port}) ..."
  until nc -z "${host}" "${port}" > /dev/null 2>&1; do
    if (( waited >= MAX_WAIT )); then
      die "${name} TCP port did not open within ${MAX_WAIT}s"
    fi
    printf "."
    sleep 2
    (( waited += 2 )) || true
  done
  echo ""
  ok "${name} TCP port is open"
}

open_browser() {
  local url="$1"
  if command -v xdg-open &>/dev/null; then
    xdg-open "${url}" &
  elif command -v open &>/dev/null; then
    open "${url}" &
  elif command -v start &>/dev/null; then
    start "${url}" &
  else
    warn "Could not detect a browser launcher. Open manually: ${url}"
  fi
}

# ── Banner ────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}"
cat << 'EOF'
  ███████╗██╗  ████████╗    ██████╗ ██╗      █████╗ ████████╗███████╗
  ██╔════╝██║  ╚══██╔══╝    ██╔══██╗██║     ██╔══██╗╚══██╔══╝██╔════╝
  █████╗  ██║     ██║       ██████╔╝██║     ███████║   ██║   █████╗  
  ██╔══╝  ██║     ██║       ██╔═══╝ ██║     ██╔══██║   ██║   ██╔══╝  
  ███████╗███████╗██║       ██║     ███████╗██║  ██║   ██║   ██║     
  ╚══════╝╚══════╝╚═╝       ╚═╝     ╚══════╝╚═╝  ╚═╝   ╚═╝   ╚═╝     
EOF
echo -e "${RESET}"
echo -e "  ${BOLD}Self-Service ELT Platform${RESET} — Docker Compose Stack"
echo ""

# ── Step 1: Docker Compose up ─────────────────────────────────────────────────
log "Starting Docker Compose stack ..."
if ! docker compose -f "${COMPOSE_FILE}" up -d --build 2>&1; then
  # Fallback for older Docker versions
  docker-compose -f "${COMPOSE_FILE}" up -d --build
fi
ok "Containers started"
echo ""

# ── Step 2: Wait for Postgres ─────────────────────────────────────────────────
wait_for_tcp "Postgres" "localhost" "5432"

# ── Step 3: Wait for MinIO ───────────────────────────────────────────────────
wait_for_url "MinIO" "${MINIO_ENDPOINT}/minio/health/live"

# ── Step 4: Create MinIO bucket ──────────────────────────────────────────────
log "Configuring MinIO bucket '${MINIO_BUCKET}' ..."

# Prefer mc (MinIO client) if available; fall back to curl S3 API
if command -v mc &>/dev/null; then
  mc alias set eltlocal "${MINIO_ENDPOINT}" "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" --api S3v4 > /dev/null 2>&1
  if mc ls "eltlocal/${MINIO_BUCKET}" > /dev/null 2>&1; then
    ok "Bucket '${MINIO_BUCKET}' already exists"
  else
    mc mb "eltlocal/${MINIO_BUCKET}" > /dev/null
    ok "Bucket '${MINIO_BUCKET}' created"
  fi
else
  # Use Docker to run mc inside the stack network
  MINIO_CONTAINER=$(docker compose -f "${COMPOSE_FILE}" ps -q minio 2>/dev/null || true)
  if [[ -n "${MINIO_CONTAINER}" ]]; then
    docker exec "${MINIO_CONTAINER}" sh -c "
      mc alias set local http://localhost:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} --api S3v4 > /dev/null 2>&1
      mc mb --ignore-existing local/${MINIO_BUCKET} > /dev/null 2>&1
    " && ok "Bucket '${MINIO_BUCKET}' created via container exec" \
      || warn "Could not create bucket automatically — create it manually at ${MINIO_ENDPOINT}"
  else
    warn "mc not found and minio container not detected. Create bucket manually."
  fi
fi

# Set bucket policy to allow platform reads
if command -v mc &>/dev/null; then
  mc anonymous set download "eltlocal/${MINIO_BUCKET}" > /dev/null 2>&1 || true
fi

echo ""

# ── Step 5: Wait for Backend ─────────────────────────────────────────────────
wait_for_url "FastAPI backend" "${BACKEND_URL}/health"

# ── Step 6: Wait for Frontend ────────────────────────────────────────────────
wait_for_url "React frontend" "${FRONTEND_URL}"

# ── Step 7: Wait for Airflow (optional) ─────────────────────────────────────
if curl -sf --max-time 3 "${AIRFLOW_URL}/health" > /dev/null 2>&1; then
  ok "Airflow is ready"
else
  warn "Airflow not yet ready — it may take a few more minutes to initialise"
fi

# ── Step 8: Print service URLs ───────────────────────────────────────────────
echo ""
echo -e "${BOLD}  ─────────────────────────────────────────${RESET}"
echo -e "${BOLD}  Services${RESET}"
echo -e "${BOLD}  ─────────────────────────────────────────${RESET}"
echo -e "  ${CYAN}Frontend  ${RESET} ${FRONTEND_URL}"
echo -e "  ${CYAN}Backend   ${RESET} ${BACKEND_URL}/docs"
echo -e "  ${CYAN}Airflow   ${RESET} ${AIRFLOW_URL}  (admin / admin)"
echo -e "  ${CYAN}MinIO     ${RESET} ${MINIO_ENDPOINT}  (${MINIO_ACCESS_KEY} / ${MINIO_SECRET_KEY})"
echo -e "${BOLD}  ─────────────────────────────────────────${RESET}"
echo ""

# ── Step 9: Open browser ─────────────────────────────────────────────────────
log "Opening browser at ${FRONTEND_URL} ..."
open_browser "${FRONTEND_URL}"

ok "Platform is ready. Happy piping! 🚀"
echo ""
