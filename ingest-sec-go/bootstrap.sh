#!/usr/bin/env bash
set -euo pipefail

# Optional sanity/logging
echo "🧪 Runner present? $(test -x /app/bin/ingest-sec-go && echo yes || echo no)"
echo "🌐 Starting SEC orchestrator on :8081"

# Run FastAPI app (orchestrator.py is under ingest-sec-go/)
cd /app/ingest-sec-go
exec python3 -m uvicorn orchestrator:app --host 0.0.0.0 --port 8081
