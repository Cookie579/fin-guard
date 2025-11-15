"""
Unified FinGuard ML Pipeline
===========================

This module defines a high‑level orchestration pipeline for FinGuard. Its
responsibilities include loading raw financial texts (SEC and ESG), running
the existing NLP analyses, computing cross‑document similarity metrics, and
aggregating results into a tabular feature store for downstream modelling.

The design follows the principles outlined in the specification: clear
separation of concerns, modularity, and extensibility.  Each component can
be unit tested independently.  Heavy NLP models are only loaded once per
instance to conserve GPU/CPU resources.  Embedding similarity is optional
and will gracefully degrade if the required libraries are unavailable.

Example usage::

    from pathlib import Path
    from pipeline import FinGuardPipeline

    pipeline = FinGuardPipeline(texts_dir=Path("texts"), social_dir=Path("social"))
    records = pipeline.process_all()
    pipeline.write_feature_store(records, Path("feature_store.csv"))

The resulting CSV will contain a row per company/date with aggregated
sentiment, ESG and similarity features.  See :meth:`process_all` for details
on the schema.

Note: this pipeline is designed for offline batch processing.  It does not
currently ingest new data from S3 or stream results to Kinesis; those
capabilities remain in the separate ingest scripts.
"""

from __future__ import annotations

import csv
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TYPE_CHECKING

# Existing analysis functions.  We import the ``main`` functions from the
# individual analyzers so that we can programmatically run them against
# arbitrary files.  These functions write their JSON outputs to disk; our
# orchestrator reads those outputs back to gather features.
# Ensure local modules are importable regardless of the package name containing
# a hyphen.  We insert the directory of this file into sys.path so that
# analyze_sec.py and other modules can be resolved without requiring a
# proper package structure.  This must occur before importing local modules.
import os as _os  # avoid polluting global namespace
import sys as _sys
_sys.path.insert(0, _os.path.dirname(__file__))

from analyze_sec import main as run_sec_analysis  # type: ignore
from analyze_esg import main as run_esg_analysis  # type: ignore
from analyze_social import SocialAnalyzer


# Optional embedding libraries.  These may not be installed in all
# environments.  If unavailable, similarity features will be omitted.
try:
    from sentence_transformers import SentenceTransformer  # type: ignore
    import numpy as np  # type: ignore
    _HAVE_ST_LIB = True
except Exception:
    # If sentence_transformers or numpy are missing at runtime, we
    # fall back gracefully.  Type checkers will still know their
    # shapes via the TYPE_CHECKING imports below.
    SentenceTransformer = None  # type: ignore
    np = None  # type: ignore
    _HAVE_ST_LIB = False

# During static type checking we import these modules so that type hints such
# as ``Optional[np.ndarray]`` and ``SentenceTransformer`` are understood by
# tools like mypy or pylance.  At runtime these imports are skipped to avoid
# ImportError when the packages are absent.
if TYPE_CHECKING:
    from sentence_transformers import SentenceTransformer as STType  # noqa: F401
    import numpy as np  # noqa: F401

# ----------------------------------------------------------------------------
# Configuration Data Classes
# ----------------------------------------------------------------------------

@dataclass
class FilingPaths:
    """Container for input/output paths related to a single filing.

    Attributes
    ----------
    input_path : Path
        Path to the raw text file to analyse.
    output_path : Path
        Path where the analysis JSON will be written.  Must end with
        ``__Analyzed.json`` to distinguish processed outputs.
    form_type : str
        Filing type (e.g. ``10-K`` or ``ESG``).
    company : str
        Sanitised company identifier (underscores instead of spaces).
    date : str
        Filing date (``YYYY-MM-DD`` for SEC filings).  For ESG reports this
        may be derived from the filename stem.
    """

    input_path: Path
    output_path: Path
    form_type: str
    company: str
    date: str


@dataclass
class PipelineConfig:
    """High‑level configuration for the pipeline.

    This configuration object centralises the various directories and
    parameters used by the :class:`FinGuardPipeline`.  In addition to the
    original settings (locations of input texts, analysis outputs, social
    sentiment data and embedding model name), a new optional ``ticker`` field
    allows callers to restrict processing to a single ticker/company.  When
    set, the pipeline will ignore any filings whose company identifier does
    not contain the given ticker.  This is useful when you only want to
    analyse a specific company without scanning the entire corpus.

    Attributes
    ----------
    texts_dir : Path
        Root directory containing raw text files under ``<company>/<form>/``.
    outputs_dir : Path
        Directory where intermediate analysis JSONs will be written.  Defaults
        to ``processed`` relative to the working directory.  Existing files
        will be reused to avoid redundant re‑analysis.
    social_dir : Optional[Path]
        Directory containing social sentiment data files.  Each file should
        correspond to a company ticker and be named ``<TICKER>.json`` or
        ``<TICKER>.parquet``.  Omit to disable social sentiment features.
    embed_model_name : str
        Name of the SentenceTransformer model to use for semantic embeddings.
        Defaults to ``all-MiniLM-L6-v2``.  You may substitute a FinBERT
        embedding model such as ``ProsusAI/finbert`` if available.
    ticker : Optional[str]
        If set, only filings whose company identifier contains this ticker
        (case‑insensitive) will be analysed.  The ticker should be provided
        in a sanitised form (underscores instead of spaces, no slashes).
    """

    texts_dir: Path = Path("texts")
    outputs_dir: Path = Path("processed")
    social_dir: Optional[Path] = None
    embed_model_name: str = "all-MiniLM-L6-v2"
    ticker: Optional[str] = None




# ----------------------------------------------------------------------------
# Main Pipeline
# ----------------------------------------------------------------------------

class FinGuardPipeline:
    """Orchestrate analysis of SEC filings, ESG reports and social sentiment.

    The pipeline scans the configured ``texts_dir`` for raw text files,
    dispatches each file to the appropriate analyser (SEC or ESG), loads the
    resulting JSON outputs, and computes cross‑document similarity as well as
    aggregated sentiment statistics.  Results are returned as a list of
    dictionaries which can then be persisted to CSV/Parquet via
    :meth:`write_feature_store`.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        # Instantiate the social analyser only if a social directory is provided and
        # the SocialAnalyzer class was successfully imported.  If either the
        # directory is absent or the import failed, social features will be
        # disabled gracefully.
        if config.social_dir and SocialAnalyzer:
            try:
                self.social_analyzer = SocialAnalyzer()  # type: ignore
            except Exception:
                self.social_analyzer = None
        else:
            self.social_analyzer = None
        # Initialise embedding model lazily; only load if similarity
        # computation is requested and the library is available.
        # Use a forward reference for the embed model type so that type
        # checkers can resolve it even when the runtime import is missing.
        self._embed_model: Optional["STType"] = None
        if _HAVE_ST_LIB:
            try:
                self._embed_model = SentenceTransformer(config.embed_model_name)
            except Exception:
                logging.warning("Could not load embedding model '%s'", config.embed_model_name)
                self._embed_model = None

    # ------------------------------------------------------------------
    # Helper methods
    # ------------------------------------------------------------------
    def _list_txt_files(self) -> List[FilingPaths]:
        """Return a list of :class:`FilingPaths` for each text file found.

        This method scans the ``texts_dir`` for files with a ``.txt``
        extension and constructs corresponding :class:`FilingPaths` objects.
        A new optional configuration field ``ticker`` can be used to
        restrict the returned list to only those filings whose company
        identifier contains the given ticker (case‑insensitive).  This
        avoids processing the entire corpus when focusing on a single
        company.

        Filenames are expected to follow the convention
        ``<Company>/<Form>/<stem>.txt``.  For SEC filings the stem must
        be ``company__date__form``, whereas ESG report stems may be
        arbitrary.  Existing processed outputs will be reused, avoiding
        redundant computation.
        """
        filings: List[FilingPaths] = []
        # Require a ticker filter to prevent processing the entire corpus.  Without a
        # ticker the pipeline would scan all filings, which is no longer supported.
        if not self.config.ticker:
            raise ValueError(
                "Ticker filter is required for listing filings. Set PipelineConfig.ticker"
            )
        # Normalise ticker: strip whitespace, lower case and replace spaces
        ticker_filter: str = self.config.ticker.replace(" ", "_").lower()
        for file_path in self.config.texts_dir.rglob("*.txt"):
            rel = file_path.relative_to(self.config.texts_dir)
            # We expect at least two parts: company and form
            if len(rel.parts) < 2:
                continue
            company = rel.parts[0]
            # Apply ticker filter if provided.  We check whether the
            # normalised company string contains the ticker string.  Both
            # strings are lower‑cased for case‑insensitive matching.
            if ticker_filter and ticker_filter not in company.lower():
                continue
            form_type = rel.parts[1]
            stem = file_path.stem
            if form_type.upper() == "ESG":
                # ESG report stems may encode a date or version but we treat
                # the entire stem as the date for output naming purposes.
                date = stem
                out_path = self.config.outputs_dir / company / "ESG" / f"{stem}__Analyzed.json"
            else:
                # Expect ``company__YYYY-MM-DD__FORM`` for SEC filings.
                parts = stem.split("__")
                if len(parts) != 3:
                    logging.warning("Skipping file with unexpected SEC name: %s", file_path)
                    continue
                company_part, date, ftype = parts
                # Normalise form type case
                form_type = ftype
                out_path = self.config.outputs_dir / company / form_type / f"{company_part}__{date}__{form_type}__Analyzed.json"
            filings.append(FilingPaths(file_path, out_path, form_type, company, date))
        return filings

    def _run_analysis(self, filing: FilingPaths) -> Dict[str, Any]:
        """Run the appropriate analysis on a single filing and return its JSON.

        If the output already exists it is loaded and returned.  Otherwise
        ``run_sec_analysis`` or ``run_esg_analysis`` is invoked.  Any
        exceptions are propagated to the caller.
        """
        if filing.output_path.exists():
            return json.loads(filing.output_path.read_text(encoding="utf-8"))
        # Ensure parent directory exists
        filing.output_path.parent.mkdir(parents=True, exist_ok=True)
        if filing.form_type.upper() == "ESG":
            run_esg_analysis(input_file=filing.input_path, output_file=filing.output_path)
        else:
            run_sec_analysis(input_file=filing.input_path, output_file=filing.output_path)
        return json.loads(filing.output_path.read_text(encoding="utf-8"))

    def _embed(self, texts: List[str]) -> Optional[Any]:
        """Return embeddings for ``texts`` or ``None`` if unavailable."""
        if self._embed_model is None:
            return None
        try:
            return self._embed_model.encode(texts)
        except Exception as exc:
            logging.warning("Embedding failed: %s", exc)
            return None

    def _cosine_similarity(self, vec1: Any, vec2: Any) -> float:
        """Compute cosine similarity between two 1D vectors."""
        if np is None:
            return float("nan")
        num = float(np.dot(vec1, vec2))
        denom = float(np.linalg.norm(vec1) * np.linalg.norm(vec2))
        return num / denom if denom != 0 else float("nan")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def process_all(self) -> List[Dict[str, Any]]:
        """Process all filings found in ``texts_dir`` and return feature rows.

        Instead of selecting only the latest SEC and ESG filings per company,
        this method processes every filing individually.  Each SEC or ESG
        document yields its own record.  SEC and ESG metrics are populated
        depending on the form type of the filing; similarity and social
        features are not computed in this simplified mode.  This change
        ensures that all documents fetched for a ticker are analysed and
        surfaced to the user.
        """
        # Gather social features once per company (ticker).  If a social directory
        # is configured and the analyser is available, we attempt to load a
        # posts file named ``<TICKER>.json`` from ``social_dir`` and compute
        # sentiment metrics.  Failures will result in ``social_metrics`` being
        # ``None``.
        social_metrics: Optional[Dict[str, Any]] = None
        # Load social metrics once per ticker.  The posts file may be either a
        # JSON array (generated by our fetcher) or JSON lines.  We attempt to
        # parse both formats.  If parsing fails or the analyser is unavailable
        # the resulting metrics will be ``None``.
        if self.config.social_dir and self.social_analyzer:
            try:
                # Build posts filename: uppercase ticker
                posts_file = self.config.social_dir / f"{self.config.ticker.upper()}.json"
                if posts_file.exists():
                    posts: List[Dict[str, Any]] = []
                    try:
                        # Attempt to load as a single JSON array first.  This is the
                        # default format written by fetch_social_for_ticker.  If the
                        # file contains an array of objects, ``json.load`` will
                        # succeed and return a list.
                        with open(posts_file, "r", encoding="utf-8") as fh:
                            data = json.load(fh)
                        if isinstance(data, list):
                            posts = data
                        else:
                            # Fall back to line‑by‑line parsing if the top‑level
                            # object is not a list.
                            raise ValueError("Not a JSON array")
                    except Exception:
                        # Fallback: treat file as JSON lines
                        with open(posts_file, "r", encoding="utf-8") as fh:
                            for line in fh:
                                line = line.strip()
                                if not line:
                                    continue
                                try:
                                    posts.append(json.loads(line))
                                except Exception:
                                    continue
                    if posts:
                        social_metrics = self.social_analyzer.compute_features(posts)
            except Exception:
                social_metrics = None
        filings = self._list_txt_files()
        records: List[Dict[str, Any]] = []
        for fp in filings:
            # Run analysis on this filing.  This will reuse cached outputs if available.
            data = self._run_analysis(fp)
            metrics = data.get("aggregated_metrics") if data else None
            # Determine social features per record.  We share the same metrics across
            # all filings for a ticker (not date‑specific).  If no metrics, set None.
            s_features = social_metrics.copy() if social_metrics is not None else None
            # Build record depending on form type.  Populate either SEC or ESG metrics.
            if fp.form_type.upper() == "ESG":
                record: Dict[str, Any] = {
                    "company": fp.company.replace("_", " "),
                    "sec_filing_date": None,
                    "esg_report_date": fp.date,
                    "sec_metrics": None,
                    "esg_metrics": metrics,
                    "sec_esg_similarity": None,
                    "social_features": s_features,
                }
            else:
                record = {
                    "company": fp.company.replace("_", " "),
                    "sec_filing_date": fp.date,
                    "esg_report_date": None,
                    "sec_metrics": metrics,
                    "esg_metrics": None,
                    "sec_esg_similarity": None,
                    "social_features": s_features,
                }
            records.append(record)
        return records

    @staticmethod
    def write_feature_store(records: List[Dict[str, Any]], dest: Path) -> None:
        """Persist a list of feature rows to CSV.

        Parameters
        ----------
        records : list of dict
            Rows returned from :meth:`process_all`.  The union of keys across
            all rows will be used as the CSV header.  Complex nested
            structures will be serialised to JSON strings.
        dest : Path
            Destination path for the CSV.  Parent directories will be created.
        """
        if not records:
            raise ValueError("No records to write")
        # Determine the full set of columns
        all_keys = set()
        for rec in records:
            all_keys.update(rec.keys())
        # Normalise records: ensure all keys are present and serialise dicts
        normalised: List[Dict[str, str]] = []
        for rec in records:
            row: Dict[str, str] = {}
            for k in all_keys:
                val = rec.get(k)
                if isinstance(val, (dict, list)):
                    row[k] = json.dumps(val, separators=(",", ":"))
                else:
                    row[k] = "" if val is None else str(val)
            normalised.append(row)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "w", newline="", encoding="utf-8") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=sorted(all_keys))
            writer.writeheader()
            writer.writerows(normalised)