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
ESG_PREFIX = "esg-reports/"           
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

def safe_filename(name: str) -> str:
    # allow dots, dashes, underscores; replace everything else
    return re.sub(r"[^A-Za-z0-9._-]+", "_", name)

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
            
# ─────────────────────── ESG text extraction helpers ──────────────
def _parse_date_from_name(name: str) -> Optional[str]:
    """
    Try to find YYYY[-MM[-DD]] in filename. Return 'YYYY-MM-DD' when possible.
    """
    m = re.search(r'(?P<y>20\d{2})(?:[-_/\.](?P<m>\d{2}))?(?:[-_/\.](?P<d>\d{2}))?', name)
    if not m:
        return None
    y = m.group('y')
    mth = m.group('m') or '01'
    day = m.group('d') or '01'
    # basic sanity
    if not (1 <= int(mth) <= 12 and 1 <= int(day) <= 31):
        return None
    return f"{y}-{mth}-{day}"

def _extract_text_from_file(path: Path) -> str:
    """
    Return plain text from HTML/PDF/TXT/JSON. Raise on failure.
    """
    suf = path.suffix.lower()

    if suf in ('.txt',):
        return path.read_text(encoding='utf-8', errors='replace')

    if suf in ('.html', '.htm'):
        from bs4 import BeautifulSoup
        raw = path.read_text(encoding='utf-8', errors='replace')
        soup = BeautifulSoup(raw, 'html.parser')
        # Add line breaks for block-ish tags
        for tag in soup.find_all(['br', 'p', 'tr', 'table', 'div', 'li', 'h1','h2','h3','h4','h5','h6']):
            tag.append('\n')
        text = soup.get_text()
        text = re.sub(r'[ \t\r]+', ' ', text)
        text = re.sub(r'\n{2,}', '\n\n', text)
        return text.strip()

    if suf == '.pdf':
        # Try pdfminer.six (preferred), fall back to PyPDF2 if missing
        try:
            from pdfminer.high_level import extract_text
            txt = extract_text(str(path)) or ""
            return txt.strip()
        except Exception as e_pdfminer:
            try:
                import PyPDF2
                out = []
                with open(path, 'rb') as f:
                    reader = PyPDF2.PdfReader(f)
                    for page in reader.pages:
                        out.append(page.extract_text() or "")
                return "\n".join(out).strip()
            except Exception as e_pypdf:
                raise RuntimeError(f"PDF extract failed with pdfminer ({e_pdfminer}) and PyPDF2 ({e_pypdf})")

    if suf == '.json':
        try:
            obj = json.loads(path.read_text(encoding='utf-8', errors='replace'))
        except Exception as e:
            raise RuntimeError(f"JSON parse failed: {e}")
        # common fields where scraped text might live
        for key in ('text', 'content', 'body', 'html', 'plaintext'):
            if key in obj and isinstance(obj[key], str) and obj[key].strip():
                return obj[key]
        # sometimes an array of paragraphs
        for key in ('paragraphs', 'lines'):
            if key in obj and isinstance(obj[key], list):
                return "\n".join(str(x) for x in obj[key])
        raise RuntimeError("No obvious text field in JSON")

    raise RuntimeError(f"Unsupported ESG file type: {suf}")

def _esg_s3_key_for(ticker: str, txt_name: str) -> str:
    return f"texts/{sanitize(ticker.upper())}/{txt_name}"

def _esg_uploaded(ticker: str, txt_name: str) -> bool:
    key = _esg_s3_key_for(ticker, txt_name)
    try:
        s3.head_object(Bucket=ESG_BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False

def _upload_clean_esg(local_txt: Path, ticker: str, txt_name: str) -> None:
    key = _esg_s3_key_for(ticker, txt_name)
    s3.upload_file(str(local_txt), ESG_BUCKET, key)
    print(f"✅  Uploaded ESG: s3://{ESG_BUCKET}/{key}")

# ─────────────────────── ESG helper functions ─────────────────────
def list_esg_keys() -> List[str]:
    ks: List[str] = []
    seen_total = 0
    paginator = s3.get_paginator("list_objects_v2")
    for pg in paginator.paginate(Bucket=ESG_BUCKET, Prefix=ESG_PREFIX):
        contents = pg.get("Contents", [])
        seen_total += len(contents)
        for o in contents:
            key = o["Key"]
            # ignore "folders" (S3 keys that end with '/')
            if not key or key.endswith("/"):
                continue
            ks.append(key)

    print(f"🔎 ESG scan under s3://{ESG_BUCKET}/{ESG_PREFIX}")
    print(f"   Total objects seen: {seen_total}")
    print(f"   Objects (all types) matched: {len(ks)}")
    if not ks and seen_total > 0:
        examples = []
        for pg in s3.get_paginator("list_objects_v2").paginate(Bucket=ESG_BUCKET, Prefix=ESG_PREFIX):
            for o in pg.get("Contents", []):
                examples.append(o["Key"])
                if len(examples) >= 10:
                    break
            if len(examples) >= 10:
                break
        print("   e.g.:")
        for k in examples:
            print(f"     - {k}")
    return ks


def fallback_from_filename(fname: str) -> Optional[str]:
    tokens = [t for t in re.split(r"[_\-]", fname) if len(t) > 3]
    for tok in tokens:
        for d in BASE_JSON_DIR.iterdir():
            if tok.lower() in d.name.lower():
                return d.name
    return None


def download_esg_text(key: str) -> Optional[Path]:
    # expected: esg-reports/<TICKER>/<filename>
    parts = key.split("/")
    if len(parts) < 3:
        return None

    ticker = parts[1]
    company = ticker_to_company(ticker)

    # 1️⃣ prefix match against SEC folders
    if not company:
        for d in BASE_JSON_DIR.iterdir():
            if d.name.upper().startswith(ticker.upper()):
                company = d.name
                print(f"⚡ prefix {ticker} → {company}")
                break

    # 2️⃣ token match in filename (fallback)
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

    fname = safe_filename(Path(parts[-1]).name)
    dest = dest_dir / fname

    if dest.exists():
        # already downloaded this ESG artifact
        return dest

    s3.download_file(ESG_BUCKET, key, str(dest))
    print(f"✅ ESG ↓ {dest}  ({key})")
    return dest


def process_esg_texts() -> None:
    keys = list_esg_keys()
    if not keys:
        print(f"⚠️  No ESG files found under {ESG_PREFIX}")
        return

    for key in keys:
        print(f"📥  ESG candidate: {key}")
        # 1) Download the raw artifact locally
        local_src = download_esg_text(key)
        if not local_src:
            continue

        # derive company & names
        parts = key.split("/")
        ticker = parts[1].upper()
        company = ticker_to_company(ticker) or sanitize(ticker)

        # Build output txt name based on original ESG filename
        stem = Path(local_src.name).stem
        txt_name = f"{stem}.txt"

        # Skip if already uploaded
        if _esg_uploaded(ticker, txt_name):        # <- ticker here
            print("✔️  Already uploaded; skip")
            continue

        # 2) Extract text
        try:
            cleaned = _extract_text_from_file(local_src)
        except Exception as e:
            print(f"❌  ESG extract failed for {local_src.name}: {e}")
            continue

        # 3) Stage to temp and upload
        LOCAL_TMP_TXT.write_text(cleaned, encoding="utf-8", errors="replace")
        _upload_clean_esg(LOCAL_TMP_TXT, ticker, txt_name)

        # 4) Emit metadata (mimic SEC shape)
        preview_len = 8192
        with open(LOCAL_TMP_TXT, "rb") as fh:
            snippet = fh.read(preview_len).decode("utf-8", "replace")

        # try date from filename; fallback to current UTC date
        date_guess = _parse_date_from_name(local_src.name) or datetime.now(timezone.utc).strftime("%Y-%m-%d")


        rec = {
            "company":    company,
            "ticker":     ticker,
            "source":     "ESG",
            "form":       "ESG",
            "filingDate": date_guess,
            "accession":  None,
            "txt_s3_key": _esg_s3_key_for(ticker, txt_name),  # <- ticker-based key
            "txt_bytes":  LOCAL_TMP_TXT.stat().st_size,
            "ingest_ts":  datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "snippet":    snippet,
            "stage":      "clean",
        }

        # reuse existing sender; same stream name
        send_sec_metadata(rec, stream_name=KINESIS_STREAM)

        # cleanup temp
        LOCAL_TMP_TXT.unlink(missing_ok=True)


# ───────────────────────── Entrypoint ─────────────────────────────
def main() -> None:
    
    # Temp ESG Only
    # print("🚀  SEC pass …")
    # process_sec_filings()

    print("\n🚀  ESG pass …")
    process_esg_texts()

    print("\n🎉  Done.")


if __name__ == "__main__":
    main()
