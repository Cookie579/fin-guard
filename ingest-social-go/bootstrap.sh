#!/usr/bin/env bash
# Builds the Go runner (if missing) and starts the FastAPI orchestrator.
# Binary path:     /app/bin/ingest-esg-go

set -euo pipefail

APP_ROOT="/app"
GO_BIN="/usr/local/go/bin/go"
GO_MAIN_DIR="${APP_ROOT}/ingest-esg-go"
BIN_DIR="${APP_ROOT}/bin"
BIN_PATH="${BIN_DIR}/ingest-esg-go"

echo "🔎 Go version: $($GO_BIN version || echo 'go not found')"
echo "📂 App dir: ${GO_MAIN_DIR}"

mkdir -p "${BIN_DIR}"

# Optional: run worker-only mode (skip orchestrator) if env is set.
if [[ "${ORCHESTRATOR_DISABLED:-}" == "1" ]]; then
  echo "⚙️  ORCHESTRATOR_DISABLED=1 — running worker binary directly..."
  if [[ ! -s "${BIN_PATH}" ]]; then
    echo "🔧 Building Go runner..."
    export CGO_ENABLED=0
    export GOTOOLCHAIN=auto
    cd "${APP_ROOT}"
    "${GO_BIN}" build -trimpath -ldflags "-s -w" \
      -o "${BIN_PATH}" ./ingest-esg-go
    echo "✅ Built ${BIN_PATH}"
  fi
  exec "${BIN_PATH}" "$@"
fi

# Build the Go runner if missing
if [[ ! -s "${BIN_PATH}" ]]; then
  echo "🔧 Building Go runner..."
  export CGO_ENABLED=0
  export GOTOOLCHAIN=auto
  cd "${APP_ROOT}"
  "${GO_BIN}" build -trimpath -ldflags "-s -w" \
    -o "${BIN_PATH}" ./ingest-esg-go
  echo "✅ Built ${BIN_PATH}"
fi

# Sanity check: orchestrator must exist at single-level path
if [[ ! -f "${GO_MAIN_DIR}/orchestrator.py" ]]; then
  echo "❌ Missing ${GO_MAIN_DIR}/orchestrator.py"; exit 1
fi

echo "🚀 Starting orchestrator on :8081 ..."
exec python -m uvicorn orchestrator:app \
  --host 0.0.0.0 --port 8081 \
  --app-dir "${GO_MAIN_DIR}"