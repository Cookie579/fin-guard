#!/usr/bin/env python3
"""
Batch fetch 10-K submissions from EDGAR, convert them to clean plain-text,
and save each as ./texts/<Company>/<Form Type>/<Company>__<Date>__<Form Type>.txt
"""
import requests, re, sys, html, warnings, json
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from pathlib import Path
from tqdm import tqdm

# Fix Windows console encoding if needed
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ───────────────────────── Text Cleaning ─────────────────────────
def clean_10k_block(raw_block: str) -> str:
    m = re.search(r"<TEXT>(.*?)</TEXT>", raw_block, flags=re.S | re.I)
    if not m:
        raise ValueError("No <TEXT> section inside 10-K document")
    soup = BeautifulSoup(m.group(1), "html.parser")
    for br in soup.find_all(["br", "p", "tr", "table", "div"]):
        br.append("\n")
    text = soup.get_text()
    text = html.unescape(text)
    text = re.sub(r"[ \t\r]+", " ", text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()

def extract_filing_text(index_url: str) -> str:
    headers = {
        "User-Agent": "finguard <esg.finguard@gmail.com>",
        "Accept": "text/plain, */*; q=0.1",
    }
    txt_url = index_url.replace("-index.htm", ".txt")
    resp = requests.get(txt_url, headers=headers)
    resp.raise_for_status()

    docs = resp.text.split("<DOCUMENT>")
    tenk_block = next((d for d in docs if "<TYPE>10-K" in d), None)
    if not tenk_block:
        raise Exception(f"No <DOCUMENT> TYPE 10-K found in {txt_url}")
    return clean_10k_block(tenk_block)

# ───────────────────────── Main Batch Loop ─────────────────────────
def main():
    json_files = list(Path("filing-jsons").rglob("*.json"))
    if not json_files:
        print("❌ No JSON files found under filing-jsons/")
        return

    for json_file in tqdm(json_files, desc="🔍 Processing filings"):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            url     = data.get("linkToFilingDetails")
            date    = data.get("filingDate")
            company = data.get("companyName")
            form    = data.get("formType", "10-K")

            if not all([url, date, company, form]):
                print(f"⚠️  Skipping {json_file.name}: missing required fields.")
                continue

            safe_company = company.replace(" ", "_").replace("/", "-")
            safe_form    = form.replace("/", "-")

            out_dir = Path("texts") / safe_company / safe_form
            out_dir.mkdir(parents=True, exist_ok=True)

            out_file = out_dir / f"{safe_company}__{date}__{safe_form}.txt"
            if out_file.exists():
                print(f"✅ Skipping existing: {out_file.name}")
                continue

            cleaned = extract_filing_text(url)
            out_file.write_text(cleaned, encoding="utf-8")
            print(f"✅ Saved → {out_file.relative_to(Path.cwd())}")

        except Exception as e:
            print(f"❌ Failed: {json_file.name} → {e}")

if __name__ == "__main__":
    main()
