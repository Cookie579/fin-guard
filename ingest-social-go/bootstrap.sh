#!/usr/bin/env bash
set -euo pipefail

# 1) Build the Go binary if missing (fast repeat runs reuse the binary)
cd /app
mkdir -p /app/bin

# If your module root is repo root and package main is under ingest-social-go:
if [[ ! -x /app/bin/ingest-social-go ]]; then
  echo "🔧 Building Go binary (CGO_DISABLED)…"
  export CGO_ENABLED=0
  /usr/local/go/bin/go build -trimpath -ldflags "-s -w" -o /app/bin/ingest-social-go ./ingest-social-go
  echo "✅ Built /app/bin/ingest-social-go"
fi

# 2) Start the FastAPI orchestrator (same pattern as ingest-esg-go)
echo "🚀 Starting orchestrator (uvicorn)…"
exec python -m uvicorn orchestrator:app --host 0.0.0.0 --port 8081
