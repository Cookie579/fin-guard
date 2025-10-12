#!/usr/bin/env bash
# This script builds the Go ESG ingest binary at container start and then
# runs the Python FastAPI orchestrator. It is used by the orchestrator
# image (see Dockerfile.ingest-esg-go). The Go binary is compiled only
# if missing to avoid rebuilds on every startup. The orchestrator then
# runs at :8081 and exposes HTTP endpoints for launching ingest jobs as
# Kubernetes Jobs or CronJobs.

set -euo pipefail

# Application root inside the container
APP_ROOT="/app"
GO_MAIN_DIR="${APP_ROOT}/ingest-esg-go"
BIN_DIR="${APP_ROOT}/bin"
BIN_PATH="${BIN_DIR}/ingest-esg-go"

echo "🔎 Go version: $(/usr/local/go/bin/go version || echo 'go not found')"
echo "📂 App dir: ${GO_MAIN_DIR}"

# Ensure the bin directory exists
mkdir -p "${BIN_DIR}"

# Build the Go runner only if the binary is missing or empty. This keeps
# container startup fast on subsequent runs.
if [[ ! -s "${BIN_PATH}" ]]; then
  echo "🔧 Building Go runner..."
  export CGO_ENABLED=0
  export GOTOOLCHAIN=auto
  cd "${APP_ROOT}"
  # Build with -trimpath and stripped symbols to reduce binary size
  /usr/local/go/bin/go build -trimpath -ldflags "-s -w" \
    -o "${BIN_PATH}" ./ingest-esg-go
  echo "✅ Built ${BIN_PATH}"
fi

# Sanity check to ensure the orchestrator exists. This catches build‑time
# issues where files aren't copied into the image correctly.
if [[ ! -f "${GO_MAIN_DIR}/orchestrator.py" ]]; then
  echo "❌ Missing ${GO_MAIN_DIR}/orchestrator.py"; exit 1
fi

echo "🚀 Starting orchestrator on :8081 ..."
exec python -m uvicorn orchestrator:app \
  --host 0.0.0.0 --port 8081 \
  --app-dir "${GO_MAIN_DIR}"