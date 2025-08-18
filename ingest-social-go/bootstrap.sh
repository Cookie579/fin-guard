#!/usr/bin/env bash
set -euo pipefail

APP_ROOT="/app"
GO_MAIN_DIR="${APP_ROOT}/ingest-social-go"
BIN_DIR="${APP_ROOT}/bin"
BIN_PATH="${BIN_DIR}/ingest-social-go"

echo "🔎 Go version: $(/usr/local/go/bin/go version || echo 'go not found')"
echo "📂 App dir: ${GO_MAIN_DIR}"

mkdir -p "${BIN_DIR}"

# Build only if missing or empty
if [[ ! -s "${BIN_PATH}" ]]; then
  echo "🔧 Building Go runner..."
  export CGO_ENABLED=0
  export GOTOOLCHAIN=auto
  cd "${APP_ROOT}"
  /usr/local/go/bin/go build -trimpath -ldflags "-s -w" \
    -o "${BIN_PATH}" ./ingest-social-go
  echo "✅ Built ${BIN_PATH}"
fi

# Sanity checks (helpful when debugging)
if [[ ! -f "${GO_MAIN_DIR}/orchestrator.py" ]]; then
  echo "❌ Missing ${GO_MAIN_DIR}/orchestrator.py"; exit 1
fi

echo "🚀 Starting orchestrator on :8081 ..."
exec python -m uvicorn orchestrator:app \
  --host 0.0.0.0 --port 8081 \
  --app-dir "${GO_MAIN_DIR}"
