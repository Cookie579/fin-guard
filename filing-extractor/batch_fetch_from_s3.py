#!/usr/bin/env python3
"""
Unified SEC + ESG downloader/processor
=====================================

• Pulls SEC filing-JSON manifests (new schema) from S3
• Calls fetch_filing_text.py to scrape plain text
• Re-uploads cleaned text using the SAME base-name as the JSON
  e.g. Apple-10-K-2016-10-26.txt
• Also downloads ESG text files and keeps local copies
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple
from datetime import datetime, timezone
from metadata_io import send_sec_metadata

import boto3

# ─────────────────────────── S3 settings ───────────────────────────
SEC_BUCKET = "fin-processed-filings"
ESG_BUCKET = "fin-processed-esg-reports"
ESG_PREFIX = "texts/"           # inside ESG_BUCKET
KINESIS_STREAM = "fin-guard-stream"

# ───────────────────────── Local paths ─────────────────────────────
BASE_JSON_DIR  = Path("filing-jsons")   # SEC JSON cache
LOCAL_TEXTS_DIR = Path("texts")         # cleaned filings + ESG
LOCAL_TMP_TXT   = Path("temp.txt")      # staging file for upload

FETCH_SCRIPT = Path(__file__).resolve().parent / "fetch_filing_text.py"

# ───────────────────────── Utilities ───────────────────────────────
def sanitize(name: str) -> str:
    return name.strip().replace("/", "-").replace(" ", "_")

s3 = boto3.client("s3")

# Ticker → company map (learned on the fly)
TICKER_MAP: dict[str, str] = {}


def learn_mapping(ticker: str, company: str) -> None:
    t = ticker.upper().strip()
    c = sanitize(company)
    if t and t not in TICKER_MAP:
        TICKER_MAP[t] = c
        print(f"🧠  Learned mapping: {t} → {c}")


def ticker_to_company(ticker: str) -> Optional[str]:
    return TICKER_MAP.get(ticker.upper().strip())

# ─────────────────────── SEC helper functions ──────────────────────
def list_sec_json_keys() -> List[str]:
    """Return every *original* *.json key under filing-jsons/, skipping -Cleaned copies."""
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for pg in paginator.paginate(Bucket=SEC_BUCKET):
        for obj in pg.get("Contents", []):
            k = obj["Key"]
            if (
                k.endswith(".json")
                and "/filing-jsons/" in k
                and not k.endswith("-Cleaned.json")   # ← skip cleaned
            ):
                keys.append(k)
    return keys


def download_sec_json(key: str) -> Tuple[Optional[Path], str, str, str]:
    """
    Download one JSON manifest, return (local_path, company_folder, form_safe, stem)
    """
    parts = key.split("/")  # {CompanyFolder}/filing-jsons/{Form}/file.json
    if len(parts) < 4:
        print(f"⚠️  Bad SEC key: {key}")
        return None, "", "", ""

    company_folder, form_raw, fname = parts[0], parts[2], parts[-1]
    form_safe = sanitize(form_raw)
    dest_dir  = BASE_JSON_DIR / sanitize(company_folder) / form_safe
    dest_dir.mkdir(parents=True, exist_ok=True)

    local_path = dest_dir / fname
    s3.download_file(SEC_BUCKET, key, str(local_path))
    return local_path, company_folder, form_safe, Path(fname).stem  # stem=base-name


def fetch_and_clean_filing(url: str, company: str, date: str, form_safe: str) -> bool:
    """
    Call fetch_filing_text.py (single-file mode).  On success, the scraper
    writes its output to texts/<Company>/<Form>/<Company>__<Date>__<Form>.txt.
    We copy that to LOCAL_TMP_TXT for upload.
    """
    proc = subprocess.run(
        [sys.executable, str(FETCH_SCRIPT), url, company, date, form_safe],
        capture_output=True, text=True, encoding="utf-8", errors="ignore"
    )
    if proc.returncode != 0:
        print("❌  scraper error:",
              *proc.stderr.splitlines()[:4], sep="\n    ")
        return False

    src = (LOCAL_TEXTS_DIR / company / form_safe /
           f"{company}__{date}__{form_safe}.txt")
    if not src.exists():
        return False

    LOCAL_TMP_TXT.write_text(src.read_text("utf-8", "replace"), "utf-8")
    src.unlink()
    return True


def sec_uploaded(company: str, form_safe: str, txt_name: str) -> bool:
    key = f"{company}/texts/{form_safe}/{txt_name}"
    try:
        s3.head_object(Bucket=SEC_BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def upload_clean_filing(local_path: Path, company: str,
                        form_safe: str, txt_name: str) -> None:
    key = f"{company}/texts/{form_safe}/{txt_name}"
    s3.upload_file(str(local_path), SEC_BUCKET, key)
    print(f"✅  Uploaded: s3://{SEC_BUCKET}/{key}")


def process_sec_filings() -> None:
    for key in list_sec_json_keys():
        print(f"\n📄  {key}")
        local_json, folder_company, form_safe, stem = download_sec_json(key)
        if not local_json:
            continue

        meta = json.loads(local_json.read_text("utf-8", "ignore"))

        url  = meta.get("url")
        date = meta.get("filingdate") or meta.get("filingDate")
        if not url or not date:
            print("⚠️  Missing url/date → skip")
            continue

        comp = sanitize(meta.get("company") or
                        meta.get("companyName") or folder_company)

        # Learn tickers
        for fld in ("ticker", "tickers"):
            val = meta.get(fld)
            if val:
                items = val if isinstance(val, list) else [val]
                for t in items:
                    learn_mapping(t, comp)

        txt_name = f"{stem.replace(' ', '_')}.txt"

        if sec_uploaded(comp, form_safe, txt_name):
            print("✔️  Already uploaded; skip")
            continue

        if fetch_and_clean_filing(url, comp, date, form_safe):
            upload_clean_filing(LOCAL_TMP_TXT, comp, form_safe, txt_name)

            # ── Build & push metadata record ───────────────────────
            preview_len = 8192             # 8 KiB
            
            with open(LOCAL_TMP_TXT, "rb") as fh:
                snippet = fh.read(preview_len).decode("utf-8", "replace")
                
            rec = {
                "company":     comp,
                "ticker":      meta.get("ticker")
                                or (meta.get("tickers") or [None])[0],
                "source":      "SEC",                
                "form":        meta.get("form") or form_safe.replace("_", " "),
                "filingDate":  date,
                "accession":   meta.get("accession"),
                "txt_s3_key":  f"{comp}/texts/{form_safe}/{txt_name}",
                "txt_bytes":   LOCAL_TMP_TXT.stat().st_size,
                "ingest_ts":   datetime.now(timezone.utc)
                                   .isoformat(timespec="milliseconds")
                                   .replace("+00:00", "Z"),
                "snippet":     snippet,
                "stage":       "clean",
            }

            send_sec_metadata(rec, stream_name=KINESIS_STREAM)         
            # ────────────────────────────────────────────────────────
            
            LOCAL_TMP_TXT.unlink(missing_ok=True)
            time.sleep(0.25)          # be nice to the SEC site
        else:
            print("⚠️  SEC scrape failed")

# ─────────────────────── ESG helper functions ─────────────────────
def list_esg_keys() -> List[str]:
    ks: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for pg in paginator.paginate(Bucket=ESG_BUCKET, Prefix=ESG_PREFIX):
        ks.extend(o["Key"] for o in pg.get("Contents", []) if o["Key"].endswith(".txt"))
    return ks


def fallback_from_filename(fname: str) -> Optional[str]:
    tokens = [t for t in re.split(r"[_\-]", fname) if len(t) > 3]
    for tok in tokens:
        for d in BASE_JSON_DIR.iterdir():
            if tok.lower() in d.name.lower():
                return d.name
    return None


def download_esg_text(key: str) -> None:
    parts = key.split("/")            # texts/{TICKER}/...
    if len(parts) < 3:
        return

    ticker = parts[1]
    company = ticker_to_company(ticker)

    # 1️⃣ prefix match against SEC folders
    if not company:
        for d in BASE_JSON_DIR.iterdir():
            if d.name.upper().startswith(ticker.upper()):
                company = d.name
                print(f"⚡ prefix {ticker} → {company}")
                break

    # 2️⃣ token match in filename
    if not company:
        company = fallback_from_filename(Path(key).name)
        if company:
            print(f"🔍 token match → {company}")

    if not company:
        company = sanitize(ticker)
        print(f"❓ no map for {ticker}; using ticker folder")
    else:
        learn_mapping(ticker, company)

    dest_dir = LOCAL_TEXTS_DIR / sanitize(company) / "ESG"
    dest_dir.mkdir(parents=True, exist_ok=True)

    dest = dest_dir / Path(parts[-1]).name
    if dest.exists():
        return

    s3.download_file(ESG_BUCKET, key, str(dest))
    print(f"✅ ESG → {dest}")


def process_esg_texts() -> None:
    for key in list_esg_keys():
        download_esg_text(key)

# ───────────────────────── Entrypoint ─────────────────────────────
def main() -> None:
    print("🚀  SEC pass …")
    process_sec_filings()

    print("\n🚀  ESG pass …")
    process_esg_texts()

    print("\n🎉  Done.")


if __name__ == "__main__":
    main()
