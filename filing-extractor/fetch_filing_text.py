#!/usr/bin/env python3

import requests
import re
import sys
import warnings
from bs4 import XMLParsedAsHTMLWarning

# silence any XMLParsedAsHTMLWarning (we won't use BeautifulSoup here)
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

def extract_filing_text(index_url: str) -> dict:
    """
    Given an EDGAR index URL ending in '-index.htm', fetch the .txt submission,
    locate the <DOCUMENT> section with TYPE 10-K, strip tags, and return as text.
    """
    headers = {
        "User-Agent": "finguard <esg.finguard@gmail.com>",
        "Accept": "text/plain, */*; q=0.1",
    }

    # Derive the plain-text URL from the index URL:
    #   e.g. .../0000-...-index.htm → .../0000-....txt
    txt_url = index_url.replace("-index.htm", ".txt")

    # 1️⃣ Fetch the full submission text
    resp = requests.get(txt_url, headers=headers)
    resp.raise_for_status()
    full_text = resp.text

    # 2️⃣ Split into DOCUMENT blocks and pick the 10-K one
    docs = full_text.split("<DOCUMENT>")
    main_doc = None
    for doc in docs:
        # look for the TYPE line
        if "\n<TYPE>10-K" in doc or "<TYPE>10-K" in doc:
            main_doc = doc
            break

    if not main_doc:
        raise Exception(f"❌ No <DOCUMENT> TYPE 10-K found in {txt_url}")

    # 3️⃣ Strip out all remaining EDGAR tags
    # Remove anything that looks like <...>
    text_only = re.sub(r"<.*?>", "", main_doc)

    # 4️⃣ Collapse whitespace
    cleaned = re.sub(r"\s+", " ", text_only).strip()

    return {
        "source_url": txt_url,
        "cleaned_text": cleaned
    }

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python fetch_filing_text.py <index_url>")
        sys.exit(1)

    idx = sys.argv[1]
    result = extract_filing_text(idx)

    # print a short preview
    print(f"\n✅ Extracted from: {result['source_url']}\n")
    print(result["cleaned_text"][:1000] + "...\n")

    # save full text
    out_path = "filing-extractor/output-10k.txt"
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(result["cleaned_text"])
    print(f"💾 Full cleaned 10-K saved to {out_path}")
