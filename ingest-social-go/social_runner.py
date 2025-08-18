import os
import subprocess
from pathlib import Path

BIN = Path("/app/bin/ingest-social-go")
REPO_ROOT = Path("/app")   # adjust if needed

def _exec(go_args: list[str]):
    # Prefer prebuilt binary (bootstrap built it). Fallback to 'go run' if missing.
    if BIN.exists():
        cmd = [str(BIN), *go_args]
        use = "binary"
    else:
        cmd = ["go", "run", "./ingest-social-go", *go_args]
        use = "go run"

    print(f"▶ Running ({use}): {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        env={**os.environ, "CGO_ENABLED": "0"},
    )
    print(f"stdout:\n{proc.stdout}")
    if proc.stderr:
        print(f"stderr:\n{proc.stderr}")
    if proc.returncode != 0:
        print(f"❌ exited with {proc.returncode}")

def run_stocktwits():
    _exec(["-source", "stocktwits"])

def run_reddit():
    _exec(["-source", "reddit"])

def run_youtube():
    _exec(["-source", "youtube"])
