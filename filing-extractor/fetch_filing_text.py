#!/usr/bin/env python3
"""
Batch-fetch SEC submissions from EDGAR, convert them to plain-text, and save
each filing as:

    ./texts/<Company>/<Form>/<Company>__<Date>__<Form>.txt

-----------------------------------------------------------------
Expected JSON schema (strict, no fallbacks):

{
  "company":     "<Company name>",
  "form":        "<Form type, e.g. 10-K>",
  "filingdate":  "YYYY-MM-DD",
  "accession":   "<ACCESSION>",
  "url":         "https://www.sec.gov/Archives/...-index.htm",
  "ticker":      "...",
  "source":      "SEC",
  "stage":       "raw",
  "year":        "2023"
}
-----------------------------------------------------------------
If any required key is missing the JSON file is skipped.

The script now supports **two modes**:
  • Batch mode (default)  – walks filing-jsons/**/*.json
  • Single-file mode      – 4 CLI args:  fetch_filing_text.py <url> <company> <date> <form>
"""

from __future__ import annotations

import html
import json
import re
import sys
import warnings
from pathlib import Path

import requests
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from tqdm import tqdm

# ────────────────────────────────────────────────────────────────────
# Console UTF-8 fix (Windows)
# ────────────────────────────────────────────────────────────────────
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ────────────────────────────────────────────────────────────────────
# Helper functions
# ────────────────────────────────────────────────────────────────────
def _clean_filing_block(raw_block: str) -> str:
    """Extract the <TEXT>…</TEXT> block and normalise whitespace."""
    m = re.search(r"<TEXT>(.*?)</TEXT>", raw_block, flags=re.S | re.I)
    if not m:
        raise ValueError("No <TEXT> section found")

    soup = BeautifulSoup(m.group(1), "html.parser")
    for tag in soup.find_all(["br", "p", "tr", "table", "div"]):
        tag.append("\n")

    text = html.unescape(soup.get_text())
    text = re.sub(r"[ \t\r]+", " ", text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()

def _default_txt_name(company: str, date: str, form: str) -> str:
    """Fallback file name when we don't have the JSON's original stem"""
    return f"{_sanitize(company)}__{date}__{_sanitize(form)}.txt"

def _extract_filing_text(index_url: str, form_type: str) -> str:
    headers = {
        "User-Agent": "finguard <esg.finguard@gmail.com>",
        "Accept": "text/plain, */*; q=0.1",
    }
    # Works for …-index.htm **and** …-index.html
    txt_url = re.sub(r"-index\.html?$", ".txt", index_url, flags=re.I)

    resp = requests.get(txt_url, headers=headers, timeout=30)
    resp.raise_for_status()

    docs = resp.text.split("<DOCUMENT>")
    target = form_type.upper().replace("_", " ").strip()
    block = next((d for d in docs if f"<TYPE>{target}" in d), None)
    if not block:
        raise LookupError(f"{target} document not found inside {txt_url}")

    return _clean_filing_block(block)


def _sanitize(name: str) -> str:  # FS-safe folder/file names
    return re.sub(r"[\\/]", "-", name).replace(" ", "_")


# ────────────────────────────────────────────────────────────────────
# Batch mode (legacy)
# ────────────────────────────────────────────────────────────────────
REQUIRED_KEYS = {
    "company",
    "form",
    "filingdate",
    "accession",
    "url",
    "ticker",
    "source",
    "stage",
    "year",
}


def _batch_mode() -> None:
    json_files = list(Path("filing-jsons").rglob("*.json"))
    if not json_files:
        print("❌  No JSON files found under filing-jsons/")
        return

    for jf in tqdm(json_files, desc="🔍  Processing filings"):
        try:
            meta = json.loads(jf.read_text(encoding="utf-8"))

            if not REQUIRED_KEYS.issubset(meta):
                missing = REQUIRED_KEYS - meta.keys()
                print(f"⚠️  {jf.name} skipped (missing {', '.join(missing)})")
                continue

            _save_one(
                url=meta["url"],
                company=meta["company"],
                date=meta["filingdate"],
                form=meta["form"],
            )

        except Exception as e:
            print(f"❌  {jf.name} – {e}")


# ────────────────────────────────────────────────────────────────────
# Single-file mode  (url company date form)
# ────────────────────────────────────────────────────────────────────
def _save_one(
    *,
    url: str,
    company: str,
    date: str,
    form: str,
    stem: str | None = None,  # ← new!  base-name from JSON
) -> None:
    safe_company = _sanitize(company)
    safe_form    = _sanitize(form)

    out_dir = Path("texts") / safe_company / safe_form
    out_dir.mkdir(parents=True, exist_ok=True)

    txt_name = f"{stem}.txt" if stem else _default_txt_name(company, date, form)
    out_file = out_dir / txt_name

    if out_file.exists():
        print(f"✅  {out_file.name} already exists – skipped")
        return

    cleaned = _extract_filing_text(url, form)
    out_file.write_text(cleaned, encoding="utf-8")

    try:
        rel = out_file.relative_to(Path.cwd())
    except ValueError:
        rel = out_file
    print(f"✅  Saved → {rel}")

# ────────────────────────────────────────────────────────────────────
# Single-file mode  (url company date form) – tolerant of failures
# ────────────────────────────────────────────────────────────────────
def _single_file_mode(args: list[str]) -> None:
    if len(args) != 4:
        print("usage: fetch_filing_text.py <url> <company> <date> <form>")
        sys.exit(2)

    url, company, date, form = args
    try:
        _save_one(url=url, company=company, date=date, form=form)
        sys.exit(0)          # success
    except Exception as e:
        # Log just one concise line for the parent process
        print(f"ERROR: {e.__class__.__name__}: {e}", file=sys.stderr)
        sys.exit(1)          # non-zero so caller knows it failed



# ────────────────────────────────────────────────────────────────────
# Entrypoint selector
# ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    if len(sys.argv) == 1:
        _batch_mode()                  # walk filing-jsons/
    else:
        _single_file_mode(sys.argv[1:])  # url company date form