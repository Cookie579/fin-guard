#!/usr/bin/env python3
"""
Simple filing fetcher
=====================

This module provides a minimal interface for downloading a single SEC
submission from EDGAR and converting it to plain text.  It is a
simplified replacement for the former ``fetch_filing_text.py`` which
included a batch mode that scanned an entire directory of manifests.  By
restricting the tool to a single filing at a time, we avoid inadvertently
pulling every filing in the dataset.  You must explicitly supply the
filing URL along with the company name, filing date and form type.

The script writes the cleaned filing text to ``texts/<Company>/<Form>/<Company>__<Date>__<Form>.txt``.

Usage examples::

    # Fetch a single 10-K filing
    python -m filing_extractor.fetch \
        https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/filing-index.htm \
        Apple \  # company
        2023-10-27 \  # filing date
        10-K

The output directory (``texts``) will be created if it does not already
exist.  Existing files will be overwritten.
"""

from __future__ import annotations

import html
import re
import sys
import os
from pathlib import Path
from typing import List, Optional, Dict, Any

import pandas as pd
import requests
from bs4 import BeautifulSoup
import json

# Additional dependencies for S3 support
from urllib.parse import urlparse
# fsspec is imported lazily inside helper functions to avoid a hard
# dependency when S3 support is not required.  If you plan to fetch
# manifests from S3 URIs you must install fsspec and boto3.
try:
    import fsspec  # type: ignore
except Exception:
    fsspec = None  # type: ignore


def _clean_filing_block(raw_block: str) -> str:
    """Extract the ``<TEXT>…</TEXT>`` block from a raw filing and normalise whitespace."""
    m = re.search(r"<TEXT>(.*?)</TEXT>", raw_block, flags=re.S | re.I)
    if not m:
        raise ValueError("No <TEXT> section found in filing")
    soup = BeautifulSoup(m.group(1), "html.parser")
    # Insert newlines after block‑level tags to preserve structure
    for tag in soup.find_all(["br", "p", "tr", "table", "div"]):
        tag.append("\n")
    text = html.unescape(soup.get_text())
    text = re.sub(r"[ \t\r]+", " ", text)
    text = re.sub(r"\n{2,}", "\n\n", text)
    return text.strip()


def _extract_filing_text(index_url: str, form_type: str) -> str:
    """Download the filing at *index_url* and return the cleaned text for the given form."""
    headers = {
        "User-Agent": "fin-guard/1.0 (contact: esg.finguard@gmail.com)",
        "Accept": "text/plain, */*; q=0.1",
    }
    # EDGAR exposes a ``.txt`` view of filings by replacing ``-index.html`` with ``.txt``
    txt_url = re.sub(r"-index\.html?$", ".txt", index_url, flags=re.I)
    resp = requests.get(txt_url, headers=headers, timeout=30)
    resp.raise_for_status()
    docs = resp.text.split("<DOCUMENT>")
    target = form_type.upper().replace("_", " ").strip()
    block = next((d for d in docs if f"<TYPE>{target}" in d), None)
    if block is None:
        raise LookupError(f"{target} document not found inside {txt_url}")
    return _clean_filing_block(block)


def _sanitize(name: str) -> str:
    """Return a filesystem‑safe version of *name* by replacing separators and spaces."""
    return re.sub(r"[\\/]+", "-", name).replace(" ", "_")


def save_filing(url: str, company: str, date: str, form: str, *, overwrite: bool = False) -> Path:
    """Download and save a single filing.

    Parameters
    ----------
    url : str
        The EDGAR index URL for the filing (e.g. ``...-index.htm``).
    company : str
        The company name.  This determines the output directory name.
    date : str
        Filing date in ``YYYY-MM-DD`` format.  Used in the output filename.
    form : str
        Filing type (e.g. ``10-K``).  Determines the subdirectory name.
    overwrite : bool, optional
        If ``True`` and the output file already exists, it will be
        overwritten.  Defaults to ``False`` (existing files are preserved).

    Returns
    -------
    pathlib.Path
        The path to the saved text file.
    """
    safe_company = _sanitize(company)
    safe_form = _sanitize(form)
    out_dir = Path("texts") / safe_company / safe_form
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{safe_company}__{date}__{safe_form}.txt"
    if out_file.exists() and not overwrite:
        print(f"✅  {out_file} already exists – skipped")
        return out_file
    cleaned = _extract_filing_text(url, form)
    out_file.write_text(cleaned, encoding="utf-8")
    print(f"✅  Saved → {out_file}")
    return out_file


# ---------------------------------------------------------------------------
# New helper functions for querying and fetching data by ticker
# ---------------------------------------------------------------------------

def _list_local_data_files(base_dir: Path, ticker: str, source: str) -> List[Path]:
    """Return a list of CSV/Parquet files matching a given ticker and source.

    This helper searches the provided *base_dir* for subdirectories of the form
    ``ticker=<TICKER>/source=<SOURCE>/stage=raw/year=<YEAR>/`` (case insensitive).
    Within each matching directory it collects all files with a ``.csv`` or
    ``.parquet`` extension.  If the base directory does not exist the list
    will be empty.  This function is intended to decouple the fetcher from
    any particular storage backend (e.g. S3) by operating on a local mirror
    of the data lake.

    Parameters
    ----------
    base_dir : pathlib.Path
        Root of the local data lake mirror (e.g. ``datalake/events``).
    ticker : str
        Company ticker symbol (case insensitive) used to match ``ticker=``
        directories.
    source : str
        Data source (e.g. "SEC", "ESG", "stocktwits", "youtube").  Matching is
        case insensitive.  Both lower and upper case variants will be
        considered.

    Returns
    -------
    list[pathlib.Path]
        All CSV or Parquet files found under matching paths.  The order is
        arbitrary.
    """
    files: List[Path] = []
    if not base_dir.exists():
        return files
    ticker_key = f"ticker={ticker}".lower()
    source_key = f"source={source}".lower()
    for root, dirs, _ in os.walk(base_dir):
        rel = os.path.relpath(root, base_dir)
        parts = rel.split(os.sep)
        # We look for both ticker and source segments in the relative path
        has_ticker = any(part.lower().startswith("ticker=") and ticker_key == part.lower() for part in parts)
        has_source = any(part.lower().startswith("source=") and source_key == part.lower() for part in parts)
        has_stage_raw = any(part.lower().startswith("stage=") and part.split("=",1)[1].lower() == "raw" for part in parts)
        if has_ticker and has_source and has_stage_raw:
            # Collect files in this directory or its subdirectories
            for entry in Path(root).rglob("*"):
                if entry.is_file() and entry.suffix.lower() in {".csv", ".parquet"}:
                    files.append(entry)
    return files


def _read_csv_any(path: Path) -> "pd.DataFrame":
    """Read a CSV or Parquet file into a pandas DataFrame.

    If the file ends with ``.parquet`` it is read using pandas.read_parquet,
    otherwise pandas.read_csv is used.  This helper centralises the import
    and error handling.  When pandas cannot read the file an informative
    exception is raised.

    Returns
    -------
    pandas.DataFrame
        Contents of the file.  Any missing values remain as NaN.
    """
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:
        raise ImportError("pandas is required for reading CSV or Parquet files") from exc
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    return pd.read_csv(path)


# --- S3 helper functions ---
#
# These functions support reading manifest files from either local
# directories or Amazon S3 buckets.  When a ``data_root`` parameter
# starts with ``s3://`` the helper functions use the ``fsspec`` library
# together with ``s3fs`` to recursively list Parquet and CSV files
# beneath the ticker‑specific prefix.  They also provide a unified
# way to read Parquet/CSV files (``_read_df``) and filter by the
# ``source=<...>`` directory component (``_filter_source``).  Local
# paths continue to work as before.

def _is_s3_path(p: str | Path) -> bool:
    """Return True if *p* appears to be an S3 URI (case‑insensitive)."""
    return str(p).lower().startswith("s3://")


def _norm(p: str) -> str:
    """Normalise path separators to forward slashes for consistent splitting."""
    return str(p).replace("\\", "/")


def _list_manifest_files(data_root: str | Path, ticker: str, *, suffixes=(".parquet", ".csv")) -> list[str]:
    """Discover all manifest files for a given ticker beneath *data_root*.

    Unlike the original implementation which assumed a fixed ``ticker=<ticker>``
    prefix, this version scans the entire data root and filters files whose
    path segments contain a matching ticker (case‑insensitive) after the
    ``ticker=`` prefix.  This makes the search robust to case differences
    in directory names (e.g. ``ticker=tsla`` or ``ticker=TSLA``) and avoids
    missing data when the casing does not match exactly.  Both S3 and local
    file systems are supported.

    Parameters
    ----------
    data_root : str or Path
        Root of the data lake (S3 URI or local path).
    ticker : str
        Ticker symbol to match in ``ticker=<ticker>`` directories (case‑insensitive).
    suffixes : tuple[str, ...], optional
        File extensions to include (defaults to ``(".parquet", ".csv")``).

    Returns
    -------
    list[str]
        Fully qualified file paths.  S3 URIs include the ``s3://`` prefix.  Local
        paths use OS‑native separators.
    """
    files: list[str] = []
    tick_lower = ticker.lower()
    # Normalise base path and strip trailing slashes
    root_str = _norm(str(data_root)).rstrip("/")
    # Case 1: S3
    if _is_s3_path(root_str):
        # If fsspec is unavailable, we cannot scan S3 manifests.  Warn and return empty.
        if fsspec is None:
            return []
        try:
            fs = fsspec.filesystem("s3")  # type: ignore[attr-defined]
        except Exception:
            return []
        # Remove scheme for S3 path to obtain bucket/prefix
        no_scheme = root_str.replace("s3://", "", 1)
        # Recursively find all files under the data root
        try:
            for f in fs.find(no_scheme):
                full_path = f"s3://{f}"
                if not any(full_path.lower().endswith(ext) for ext in suffixes):
                    continue
                parts = _norm(full_path).split("/")
                match = False
                for seg in parts:
                    if seg.lower().startswith("ticker="):
                        val = seg.split("=", 1)[1].lower()
                        if val == tick_lower:
                            match = True
                            break
                if match:
                    files.append(full_path)
        except Exception:
            return []
    else:
        # Local path scanning
        p = Path(root_str)
        if p.exists():
            for f in p.rglob("*"):
                if not f.is_file():
                    continue
                if f.suffix.lower() not in {ext.lower() for ext in suffixes}:
                    continue
                parts = [part for part in f.parts]
                match = False
                for seg in parts:
                    seg_low = seg.lower()
                    if seg_low.startswith("ticker="):
                        val = seg_low.split("=", 1)[1]
                        if val == tick_lower:
                            match = True
                            break
                if match:
                    files.append(str(f))
    return files


def _read_df(path: str) -> pd.DataFrame:
    """Read a Parquet or CSV manifest from local disk or S3.

    This helper determines the file type by extension and delegates to
    ``pandas.read_parquet`` or ``pandas.read_csv``.  For S3 URIs the
    appropriate filesystem is used automatically via ``fsspec``.

    Parameters
    ----------
    path : str
        Fully qualified file path (may start with ``s3://``).

    Returns
    -------
    pandas.DataFrame
        Contents of the manifest.
    """
    low = path.lower()
    if low.endswith(".parquet"):
        return pd.read_parquet(path)
    if low.endswith(".csv"):
        return pd.read_csv(path)
    raise ValueError(f"Unsupported manifest type: {path}")


def _filter_source(paths: list[str], desired: str) -> list[str]:
    """Filter manifest paths by matching ``source=DESIRED`` in the path.

    Case is ignored when comparing the ``source`` value.

    Parameters
    ----------
    paths : list[str]
        Candidate manifest paths (local paths or S3 URIs).
    desired : str
        Desired source name (e.g. ``"SEC"``, ``"ESG"``, ``"stocktwits"``,
        ``"youtube"``).

    Returns
    -------
    list[str]
        Only those paths whose ``source=`` segment matches ``desired``.
    """
    dlow = desired.lower()
    keep: list[str] = []
    for p in paths:
        parts = _norm(p).split("/")
        for seg in parts:
            if seg.lower().startswith("source="):
                val = seg.split("=", 1)[1].lower()
                if val == dlow:
                    keep.append(p)
                    break
    return keep


def fetch_sec_for_ticker(
    ticker: str,
    *,
    data_root: str | Path | None = None,
    csv_paths: Optional[List[Path]] = None,
    overwrite: bool = False,
    limit: Optional[int] = None,
) -> List[Path]:
    """Fetch all SEC filings for a given ticker from local data manifests.

    This function reads one or more manifests (CSV or Parquet) that describe
    SEC filings for the specified company.  Each manifest must contain at
    least the columns ``company``, ``filingdate``, ``form`` and ``url``.  For
    every row in the manifest a call to :func:`save_filing` is made to
    download and store the filing text locally.  The destination path is
    returned for each saved filing.  Existing files are reused unless
    ``overwrite`` is set to ``True``.

    Parameters
    ----------
    ticker : str
        Company ticker symbol to fetch filings for.
    data_root : pathlib.Path, optional
        Root directory containing ``ticker=``/``source=SEC`` data manifests.  If
        ``csv_paths`` is provided this argument is ignored.
    csv_paths : list[pathlib.Path] or None, optional
        Explicit list of manifest files to read.  When supplied, the
        manifests will be read directly and ``data_root`` will not be
        searched.  This is useful for testing or when the manifests have
        been exported to a single CSV.  If omitted, :func:`_list_local_data_files`
        is used to locate manifests under ``data_root``.
    overwrite : bool, optional
        Whether to re-download filings even if the destination file exists.
    limit : int or None, optional
        Maximum number of filings to fetch.  If ``None`` all filings will
        be processed.

    Returns
    -------
    list[pathlib.Path]
        Paths to the downloaded filing text files.
    """
    results: List[Path] = []
    # Build list of manifest files to load.  Support both explicit csv_paths and
    # discovery via a data root which may be an S3 URI or local path.  When
    # data_root is None, fall back to the default ``datalake/events``.
    manifests: list[str] = []
    if csv_paths:
        manifests.extend([str(p) for p in csv_paths])
    # Determine the base root for manifest discovery
    dr = data_root if data_root is not None else Path("datalake/events")
    if dr:
        # Discover all manifest files under ticker=<ticker> and filter to SEC source
        try:
            all_files = _list_manifest_files(dr, ticker)
            manifests.extend(_filter_source(all_files, "SEC"))
        except Exception:
            # If discovery fails (e.g. missing credentials) ignore silently
            pass
    if not manifests:
        print(f"ℹ️  No SEC manifests found for {ticker}")
        return results
    count = 0
    for manifest_path in manifests:
        try:
            df = _read_df(manifest_path)
        except Exception as exc:
            print(f"⚠️  Skipping {manifest_path} due to read error: {exc}")
            continue
        for _, row in df.iterrows():
            # Enforce limit on the number of attempts rather than successes.  This prevents
            # infinite looping when all rows fail to download.
            if limit is not None and count >= limit:
                return results
            url = row.get("url")
            # Always use the ticker symbol for the company folder.  This ensures
            # that SEC filings are saved under a directory that matches the
            # ticker string, so the pipeline's ticker filter will pick up
            # these files.  We still read the original company name but do
            # not use it for directory naming.
            company = ticker
            filingdate = row.get("filingdate") or row.get("date") or row.get("ts")
            form = row.get("form") or row.get("form_type") or "10-K"
            # Skip rows without an index URL
            if not isinstance(url, str) or not url or url == "\\N":
                count += 1
                continue
            if not isinstance(filingdate, str) or filingdate in {"", "\\N"}:
                filingdate = "unknown-date"
            if not isinstance(form, str) or form in {"", "\\N"}:
                form = "UNKNOWN"
            try:
                # Pass the ticker as the company to ensure saved folder names
                # contain the ticker string (see comment above).
                path = save_filing(url, ticker, filingdate, form, overwrite=overwrite)
                results.append(path)
            except Exception as exc:
                print(f"⚠️  Failed to fetch SEC filing at {url}: {exc}")
            finally:
                count += 1
    return results


def _download_text_from_url(url: str) -> str:
    """Download and extract visible text from a URL.

    This helper attempts to fetch *url* using a custom User‑Agent to
    maximise compatibility with sites that block default clients.  If
    the response contains HTML, visible text is extracted using
    ``BeautifulSoup`` with script and style tags removed.  For other
    content types (plain text, PDF, etc.) the response body is
    returned as text.  If fetching fails or text extraction yields an
    empty string, a ``RuntimeError`` is raised so callers can
    fallback to alternative content (e.g. manifest description).

    Parameters
    ----------
    url : str
        The HTTP(S) URL to download.

    Returns
    -------
    str
        Extracted textual content.  May be empty if the remote
        document contained no extractable text.

    Raises
    ------
    RuntimeError
        If the download fails (network error or non‑2xx status).
    """
    import requests
    from bs4 import BeautifulSoup
    # Use a realistic User‑Agent to improve the chances of bypassing
    # basic bot filters on news sites (e.g. Bloomberg, Reuters).  Sites
    # may still refuse access (HTTP 403/401), in which case we let
    # requests raise and propagate the error.
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        resp.raise_for_status()
    except Exception as exc:
        # Propagate with context so callers can decide whether to
        # fallback to manifest metadata.
        raise RuntimeError(f"Failed to download {url}: {exc}") from exc
    content_type = resp.headers.get("content-type", "").lower()
    text = ""
    try:
        if "html" in content_type:
            soup = BeautifulSoup(resp.text, "html.parser")
            # Remove script and style tags
            for script in soup(["script", "style"]):
                script.decompose()
            text = soup.get_text(separator="\n")
        else:
            # For non‑HTML (including PDFs), just decode the response.
            # Some sites deliver PDF files with a generic text/plain
            # header; rather than trying to parse PDFs (which requires
            # heavy dependencies), we return the raw text.  Callers
            # should fallback to manifest metadata if this yields an
            # empty string.
            text = resp.text
    except Exception:
        text = resp.text
    return text.strip()


def _sanitize_name(name: str) -> str:
    """Sanitise a company or form name to a filesystem‑safe string."""
    import re
    return re.sub(r"[\\/]+", "-", str(name)).replace(" ", "_")


def fetch_esg_for_ticker(
    ticker: str,
    *,
    data_root: str | Path | None = None,
    csv_paths: Optional[List[Path]] = None,
    overwrite: bool = False,
    limit: Optional[int] = None,
) -> List[Path]:
    """Fetch all ESG reports for a given ticker from local data manifests.

    This function reads one or more manifests (CSV or Parquet) that describe
    ESG reports for the specified company.  Each manifest should have at
    least a ``url`` column.  The function downloads each report, extracts
    visible text (if possible) and writes it to ``texts/<Company>/ESG/`` with
    a filename of the form ``<Company>__<Date>__ESG.txt``.  Existing files
    will be reused unless ``overwrite`` is ``True``.

    Parameters
    ----------
    ticker : str
        Company ticker symbol.
    data_root : pathlib.Path, optional
        Root directory containing ``ticker=``/``source=ESG`` data manifests.  Ignored
        if ``csv_paths`` is provided.
    csv_paths : list[pathlib.Path] or None, optional
        Explicit manifest files to read instead of searching ``data_root``.
    overwrite : bool, optional
        Whether to overwrite existing ESG text files.
    limit : int or None, optional
        Maximum number of reports to fetch.

    Returns
    -------
    list[pathlib.Path]
        Paths to the downloaded ESG text files.
    """
    results: List[Path] = []
    manifests: list[str] = []
    if csv_paths:
        manifests.extend([str(p) for p in csv_paths])
    dr = data_root if data_root is not None else Path("datalake/events")
    if dr:
        try:
            all_files = _list_manifest_files(dr, ticker)
            manifests.extend(_filter_source(all_files, "ESG"))
        except Exception:
            pass
    if not manifests:
        print(f"ℹ️  No ESG manifests found for {ticker}")
        return results
    count = 0
    # Track how many reports have been saved per date so that we can
    # generate unique filenames when multiple ESG reports share the same
    # filing date.  Without this, distinct rows with the same date will
    # map to the same output filename and later reports will be skipped.
    per_date_counter: Dict[str, int] = {}
    for manifest_path in manifests:
        try:
            df = _read_df(manifest_path)
        except Exception as exc:
            print(f"⚠️  Skipping {manifest_path} due to read error: {exc}")
            continue
        for _, row in df.iterrows():
            # Enforce limit on total attempts rather than successes.  This
            # prevents infinite loops when downloads consistently fail.
            if limit is not None and count >= limit:
                return results
            # Source URL of the ESG report
            url = row.get("url")
            # Always use the ticker symbol for the company folder.  See
            # comments in fetch_sec_for_ticker.
            company = ticker
            # Some manifests label the date differently
            date = row.get("filingdate") or row.get("date") or row.get("ts") or "unknown-date"
            # Determine the output location
            safe_company = _sanitize_name(company)
            out_dir = Path("texts") / safe_company / "ESG"
            out_dir.mkdir(parents=True, exist_ok=True)
            # Determine a unique output filename.  If multiple reports share
            # the same date, append an incrementing suffix to the stem to
            # avoid collisions.  For the first report on a given date we
            # preserve the simple "<company>__<date>__ESG.txt" naming
            # convention; subsequent reports become
            # "<company>__<date>_<idx>__ESG.txt".
            # Update the per‑date counter
            per_date_counter.setdefault(date, 0)
            per_date_counter[date] += 1
            idx = per_date_counter[date]
            if idx == 1:
                stem = f"{safe_company}__{date}__ESG"
            else:
                stem = f"{safe_company}__{date}_{idx}__ESG"
            out_file = out_dir / f"{stem}.txt"
            # Skip if file exists and we are not overwriting.  We check the
            # existence of the computed out_file rather than the base
            # "date" filename because duplicates will have unique stems.
            if out_file.exists() and not overwrite:
                results.append(out_file)
                count += 1
                continue
            text: Optional[str] = None
            # Try to download the ESG report if a URL is provided
            if isinstance(url, str) and url and url != "\\N":
                try:
                    text = _download_text_from_url(url)
                    # Normalise empty strings to None for fallback logic
                    if text:
                        text = text.strip()
                    else:
                        text = None
                except Exception as exc:
                    # Log and proceed to fallback on manifest metadata
                    print(f"⚠️  Failed to download ESG report at {url}: {exc}")
                    text = None
            # Fallback: if no text was obtained from the URL, use the manifest's
            # description or title fields (if present) as a proxy for the report
            if not text:
                desc = row.get("description") or row.get("title") or row.get("summary")
                if isinstance(desc, str) and desc and desc != "\\N":
                    text = desc.strip()
            # If still no text, skip this entry
            if not text:
                count += 1
                continue
            out_file.write_text(text, encoding="utf-8")
            print(f"✅  Saved ESG report → {out_file}")
            results.append(out_file)
            count += 1
    return results


def fetch_social_for_ticker(
    ticker: str,
    *,
    data_root: str | Path | None = None,
    csv_paths: Optional[List[Path]] = None,
    overwrite: bool = False,
) -> List[Path]:
    """Fetch social media posts for a given ticker from local data manifests.

    This helper aggregates posts from multiple sources (e.g. stocktwits and
    youtube).  It searches for manifests under ``source=stocktwits`` and
    ``source=youtube`` (case insensitive) and reads them into a JSON lines
    file ``social/<TICKER>.json``.  Each manifest must include a ``url`` or
    ``content`` column.  If a URL is provided the page content is downloaded
    and stored as a post with the text content; otherwise the row itself is
    treated as a post.  Missing dependencies (pandas, requests) will cause
    an ImportError.

    Parameters
    ----------
    ticker : str
        Company ticker symbol.
    data_root : pathlib.Path, optional
        Root directory containing ``ticker=``/``source=*`` data manifests.
    csv_paths : list[pathlib.Path] or None, optional
        Explicit manifest files to read.  If provided, only these files are
        processed.  Otherwise, the function searches ``data_root`` for both
        stocktwits and youtube sources.
    overwrite : bool, optional
        Whether to overwrite an existing social posts file.

    Returns
    -------
    list[pathlib.Path]
        A list containing the path to the aggregated social posts file (if
        any posts were downloaded).  If no posts were found the list will be
        empty.
    """
    try:
        import pandas as pd  # type: ignore
    except Exception as exc:
        raise ImportError("pandas is required to parse social manifests") from exc
    try:
        import requests  # type: ignore
    except Exception as exc:
        raise ImportError("requests is required to download social content") from exc
    manifests: list[str] = []
    if csv_paths:
        manifests.extend([str(p) for p in csv_paths])
    dr = data_root if data_root is not None else Path("datalake/events")
    if dr:
        try:
            all_files = _list_manifest_files(dr, ticker)
            manifests.extend(_filter_source(all_files, "stocktwits"))
            manifests.extend(_filter_source(all_files, "youtube"))
        except Exception:
            pass
    if not manifests:
        print(f"ℹ️  No social manifests found for {ticker}")
        return []
    posts: List[Dict[str, Any]] = []
    for manifest_path in manifests:
        try:
            # _read_df expects a string path.  Do not wrap S3 URIs or local
            # paths with Path(), because the resulting WindowsPath/PosixPath
            # object will not support .lower(), causing an AttributeError.
            df = _read_df(manifest_path)
        except Exception as exc:
            print(f"⚠️  Skipping {manifest_path} due to read error: {exc}")
            continue
        for _, row in df.iterrows():
            # Determine the text content: we prefer 'content', then 'text', then
            # 'description' or 'title' (for sources such as YouTube).  This
            # allows us to capture descriptions and titles when the stocktwits
            # 'content' field is absent.  Blank or '\\N' values are ignored.
            raw_content = (
                row.get("content")
                or row.get("text")
                or row.get("description")
                or row.get("title")
                or None
            )
            url = row.get("url")
            text: Optional[str] = None
            if isinstance(raw_content, str) and raw_content and raw_content != "\\N":
                text = raw_content
            elif isinstance(url, str) and url and url != "\\N":
                # Download content from the URL if present
                try:
                    dl = _download_text_from_url(url)
                    if dl:
                        text = dl.strip()
                except Exception:
                    text = None
            # Skip rows with no extractable text
            if not text:
                continue
            # Build a richer post object with additional metadata where available.
            # Extract timestamp from various possible columns.  We prioritise
            # created_at/published_at fields because some manifests provide
            # a blank ``timestamp`` column which would otherwise mask
            # usable values.  We choose the first non‑blank, non‐\N value.
            timestamp_val = None
            for col in [
                "created_at",
                "published_at",
                "timestamp",
                "datetime",
                "ts",
                "ingest_ts",
                "time",
            ]:
                val = row.get(col)
                # Normalise strings: strip whitespace and treat empty
                # strings as missing.  For non‑strings, leave as is.
                if isinstance(val, str):
                    v = val.strip()
                else:
                    v = val
                if v is None or v == "\\N" or v == "":
                    continue
                timestamp_val = v
                break
            # Normalise timestamp to a string using pandas for NaT detection when available
            ts_str = ""
            try:
                import pandas as _pd  # type: ignore
                if timestamp_val is not None and not (_pd.isna(timestamp_val)):
                    ts_str = str(timestamp_val)
            except Exception:
                if timestamp_val is not None:
                    ts_str = str(timestamp_val)
            # Extract numeric metadata
            likes = row.get("likes")
            # Convert to int if possible, otherwise None
            try:
                likes = int(likes) if likes not in (None, "\\N") else None
            except Exception:
                likes = None
            reshares = row.get("reshares")
            try:
                reshares = int(reshares) if reshares not in (None, "\\N") else None
            except Exception:
                reshares = None
            # Sentiment label (e.g. Bullish, Bearish)
            sentiment_label = row.get("sentiment")
            if isinstance(sentiment_label, str) and sentiment_label != "\\N":
                sentiment_label = sentiment_label.strip()
            else:
                sentiment_label = None
            # Symbols column may contain a list or string representation of list
            symbols = row.get("symbols")
            # Try to parse list if it's a string like "[AAPL, TSLA]"
            parsed_symbols: Optional[List[str]] = None
            if isinstance(symbols, list):
                parsed_symbols = [str(s).upper() for s in symbols if s not in (None, "\\N")]
            elif isinstance(symbols, str) and symbols and symbols != "\\N":
                s = symbols.strip()
                # Remove brackets and split on commas
                s = s.strip("[]")
                # Split by comma and strip whitespace
                parts = [p.strip().upper() for p in s.split(",") if p.strip()]
                # Filter out empty strings
                parsed_symbols = [p for p in parts if p]
            # Author or username fields
            author = None
            for col in ["user_username", "author_username", "username"]:
                val = row.get(col)
                if isinstance(val, str) and val and val != "\\N":
                    author = val
                    break
            # Fall back to YouTube channel title if no user fields found
            if author is None:
                yt = row.get("channel_title")
                if isinstance(yt, str) and yt and yt != "\\N":
                    author = yt
            # Assemble post object
            post: Dict[str, Any] = {
                "timestamp": ts_str,
                "content": text,
            }
            if likes is not None:
                post["likes"] = likes
            if reshares is not None:
                post["reshares"] = reshares
            if sentiment_label is not None:
                post["sentiment_label"] = sentiment_label
            if parsed_symbols:
                post["symbols"] = parsed_symbols
            if author is not None:
                post["author"] = author
            posts.append(post)
    if not posts:
        return []
    out_dir = Path("social")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{ticker.upper()}.json"
    if out_file.exists() and not overwrite:
        print(f"✅  Social posts file already exists → {out_file}")
        return [out_file]
    # Write posts to file.  Use JSON array format to ensure commas between entries,
    # making the file valid JSON rather than line‑delimited JSON.  Pretty print
    # with indentation so that users can inspect it easily.  If a large
    # dataset makes this representation too heavy, consider streaming
    # alternatives, but for moderate sizes JSON arrays are acceptable.
    with open(out_file, "w", encoding="utf-8") as fh:
        json.dump(posts, fh, ensure_ascii=False, indent=2)
    print(f"✅  Saved social posts → {out_file}")
    return [out_file]


def main(argv: list[str] | None = None) -> None:
    """Entry point for the command line interface.

    Expects exactly four positional arguments: ``url``, ``company``, ``date`` and
    ``form``.  If fewer or more arguments are provided, a usage message is
    displayed and the process exits with status code 2.
    """
    if argv is None:
        argv = sys.argv[1:]
    if len(argv) != 4:
        print("usage: fetch.py <url> <company> <date> <form>", file=sys.stderr)
        sys.exit(2)
    url, company, date, form = argv
    try:
        save_filing(url, company, date, form)
    except Exception as exc:
        print(f"ERROR: {exc.__class__.__name__}: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()