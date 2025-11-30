"""
FinGuard Command Line Interface (CLI)
====================================

This module exposes a simple command line interface for running individual
components of the FinGuard ML pipeline or the full end‑to‑end pipeline.  It
allows you to analyse SEC filings, ESG reports and social media posts
individually, or to run the entire pipeline for a given company or ticker.

Usage examples::

    # Analyse a single SEC filing and write results to a JSON file
    python -m filing_extractor.cli sec path/to/Apple__2023-01-01__10-K.txt --output results.json

    # Analyse an ESG report
    python -m filing_extractor.cli esg path/to/Apple/ESG/report1.txt --output report1.json

    # Analyse a social posts file (JSON lines) and print sentiment metrics
    python -m filing_extractor.cli social path/to/AAPL_posts.json

    # Run the full pipeline on all data in texts/ and social/ directories
    python -m filing_extractor.cli pipeline --texts-dir texts --social-dir social

    # Run the pipeline and filter results for a specific company name
    python -m filing_extractor.cli pipeline --texts-dir texts --filter-company "Apple"

The CLI leverages the existing analysis functions and the high level
:class:`~filing_extractor.pipeline.FinGuardPipeline` orchestrator.  See
the ``--help`` flag for detailed usage.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

# Ensure the directory containing this script is on sys.path so that local
# modules (e.g. analyze_sec.py) can be imported when the package name
# contains a hyphen.  Without this, Python would search only the top‑level
# sys.path entries and fail to resolve our local modules.
import os as _os  # local import to avoid polluting global namespace
import sys as _sys
_sys.path.insert(0, _os.path.dirname(__file__))

from analyze_sec import main as run_sec_analysis  # type: ignore
from analyze_esg import main as run_esg_analysis  # type: ignore
from pipeline import PipelineConfig, FinGuardPipeline  # type: ignore
from social_charts import generate_social_charts  # type: ignore
from esg_sec_charts import generate_esg_charts, generate_sec_charts  # type: ignore
try:
    # Import the SocialAnalyzer from the dedicated module.  If unavailable
    # (e.g. missing nltk), social sentiment features will not be computed.
    from analyze_social import SocialAnalyzer  # type: ignore
except Exception:
    SocialAnalyzer = None  # type: ignore


def _analyse_sec(file_path: str, output: Optional[str]) -> None:
    """Run SEC analysis on a given file and print the summary.

    Parameters
    ----------
    file_path : str
        Path to the SEC text file.
    output : str or None
        Destination path for the JSON output.  If omitted, the analyser
        constructs a default path based on the input filename.
    """
    out = Path(output) if output else None
    run_sec_analysis(input_file=file_path, output_file=out)
    # Load and display aggregated metrics
    out_file = Path(output) if output else None
    # Determine actual output file path
    if out_file is None:
        # Reconstruct default path using the same logic as analyse_sec.py
        stem = Path(file_path).stem
        parts = stem.split("__")
        safe_company, date, form_type = parts
        out_file = Path("esg-outputs") / form_type / f"{safe_company}__{date}__{form_type}.json"
    data = json.loads(Path(out_file).read_text(encoding="utf-8"))
    print(json.dumps(data.get("aggregated_metrics", {}), indent=2))


def _analyse_esg(file_path: str, output: Optional[str]) -> None:
    """Run ESG analysis on a given file and print the summary."""
    out = Path(output) if output else None
    run_esg_analysis(input_file=file_path, output_file=out)
    out_file = Path(output) if output else Path(file_path).with_suffix("")
    if out is None:
        # Construct default as in analyse_esg: Company/ESG/<stem>.txt → esg-outputs/<Company>/ESG/<stem>__Analyzed.json
        inp = Path(file_path)
        company = inp.parent.parent.name.replace(" ", "_")
        stem = inp.stem
        out_file = Path("esg-outputs") / company / "ESG" / f"{stem}__Analyzed.json"
    data = json.loads(Path(out_file).read_text(encoding="utf-8"))
    print(json.dumps(data.get("aggregated_metrics", {}), indent=2))


def _analyse_social(posts_file: str) -> None:
    """Compute sentiment metrics for a social posts file.

    The file should contain one JSON object per line with at least a
    ``timestamp`` and ``content`` key.
    """
    posts: list[Dict[str, Any]] = []
    with open(posts_file, "r", encoding="utf-8") as fh:
        for line in fh:
            try:
                posts.append(json.loads(line))
            except Exception:
                continue
    if SocialAnalyzer is None:
        raise ImportError(
            "SocialAnalyzer could not be imported. Install the required dependencies to use social analysis."
        )
    analyser = SocialAnalyzer()  # type: ignore
    feats = analyser.compute_features(posts)
    print(json.dumps(feats, indent=2))


def _run_pipeline(
    texts_dir: str,
    outputs_dir: str,
    social_dir: Optional[str],
    filter_company: Optional[str],
    ticker: Optional[str],
    data_root: Optional[str] = None,
    fetch_only: bool = False,
    *,
    use_dynamo: bool = False,
    dynamo_table: str = "fin_guard_partitions",
    dynamo_region: Optional[str] = None,
    reprocess: bool = False,
) -> None:
    """Run the full FinGuard pipeline and print the resulting feature rows.

    This helper performs optional data fetching prior to running the
    analysis pipeline.  When a ``ticker`` is provided the function will
    attempt to download all relevant SEC filings, ESG reports and social
    posts for that ticker by invoking fetchers in :mod:`fetch`.  The
    ``data_root`` argument controls the root of the local data lake mirror.

    Parameters
    ----------
    texts_dir : str
        Directory containing raw text files under ``<company>/<form>/``.
    outputs_dir : str
        Directory where intermediate JSON results should be saved.
    social_dir : str or None
        Directory containing social posts files.  If ``None``, social
        sentiment features are skipped.
    filter_company : str or None
        If supplied, only rows whose company name contains this string (case
        insensitive) will be printed.
    ticker : str or None
        If supplied, restrict analysis to filings whose company identifier
        contains this ticker (case‑insensitive).  This is useful for
        processing a single company without scanning the entire corpus.
    data_root : str or None, optional
        Root directory for SEC/ESG/social manifests.  Defaults to
        ``datalake/events`` if not provided.
    fetch_only : bool, optional
        If ``True``, only perform the data fetching step and return without
        running the pipeline.  This is useful for staging data ahead of
        analysis.
    """
    # If a ticker is provided, fetch raw data for that ticker.  This will
    # populate the texts and social directories with the latest filings and
    # posts prior to running the pipeline.
    if ticker:
        if use_dynamo:
            # Fetch SEC manifests from DynamoDB partitions
            try:
                from fetch import (
                    fetch_sec_for_ticker_from_partitions,
                    fetch_esg_for_ticker_from_partitions,
                    fetch_social_for_ticker_from_partitions,
                )  # type: ignore
            except Exception as exc:
                raise ImportError("fetch module could not be imported") from exc
            # Fetch SEC partitions from DynamoDB
            try:
                fetch_sec_for_ticker_from_partitions(
                    ticker,
                    table_name=dynamo_table,
                    aws_region=dynamo_region,
                    reprocess=reprocess,
                )
            except Exception as exc:
                print(f"⚠️  SEC fetch from Dynamo failed: {exc}")
            # Fetch ESG partitions from DynamoDB
            try:
                fetch_esg_for_ticker_from_partitions(
                    ticker,
                    table_name=dynamo_table,
                    aws_region=dynamo_region,
                    reprocess=reprocess,
                )
            except Exception as exc:
                print(f"⚠️  ESG fetch from Dynamo failed: {exc}")
            # Fetch Social partitions from DynamoDB
            try:
                fetch_social_for_ticker_from_partitions(
                    ticker,
                    table_name=dynamo_table,
                    aws_region=dynamo_region,
                    reprocess=reprocess,
                )
            except Exception as exc:
                print(f"⚠️  Social fetch from Dynamo failed: {exc}")
        else:
            # Default local fetch
            try:
                from fetch import fetch_sec_for_ticker, fetch_esg_for_ticker, fetch_social_for_ticker  # type: ignore
            except Exception as exc:
                raise ImportError("fetch module could not be imported") from exc
            base = data_root if data_root is not None else Path("datalake/events")
            try:
                fetch_sec_for_ticker(ticker, data_root=base)
            except Exception as exc:
                print(f"⚠️  SEC fetch failed: {exc}")
            try:
                fetch_esg_for_ticker(ticker, data_root=base)
            except Exception as exc:
                print(f"⚠️  ESG fetch failed: {exc}")
            try:
                fetch_social_for_ticker(ticker, data_root=base)
            except Exception as exc:
                print(f"⚠️  Social fetch failed: {exc}")
        # If the user only wanted to fetch data, return early
        if fetch_only:
            return
    # Construct pipeline configuration and run analysis
    cfg = PipelineConfig(
        texts_dir=Path(texts_dir),
        outputs_dir=Path(outputs_dir),
        social_dir=Path(social_dir) if social_dir else None,
        ticker=ticker,
    )
    pipeline = FinGuardPipeline(cfg)
    rows = pipeline.process_all()
    for row in rows:
        if filter_company and filter_company.lower() not in row["company"].lower():
            continue
        print(json.dumps(row, indent=2))
    # After processing, if social metrics were computed and a social directory
    # is configured, generate charts for the ticker and enrich posts with
    # sentiment.  Social charts will be written to ``charts/<TICKER>/SOCIAL``
    # under the project root.  ESG and SEC charts will be written to
    # ``charts/<TICKER>/ESG`` and ``charts/<TICKER>/SEC`` respectively.  A
    # sentiment‑enriched posts file will be saved to
    # ``<outputs_dir>/<TICKER>/social/<TICKER>.json`` (creating the
    # directory if needed).  Failures to generate charts or enrich posts
    # are logged but do not interrupt pipeline execution.
    if ticker and social_dir:
        try:
            posts_file = Path(social_dir) / f"{ticker.upper()}.json"
            if posts_file.exists():
                # Generate social charts under charts/<ticker>/SOCIAL
                charts_root = Path("charts") / ticker.upper()
                social_out = charts_root / "SOCIAL"
                generate_social_charts(posts_file, social_out)
                # Compute sentiment for each post and save to processed/<ticker>/social
                if SocialAnalyzer is not None:
                    try:
                        import json as _json
                        posts = _json.load(open(posts_file, "r", encoding="utf-8"))
                        sa = SocialAnalyzer()  # type: ignore
                        for p in posts:
                            content = p.get("content", "")
                            score = sa.score_text(content)
                            p["sentiment_score"] = score
                            # Classify sentiment based on VADER thresholds
                            if score > 0.05:
                                label = "positive"
                            elif score < -0.05:
                                label = "negative"
                            else:
                                label = "neutral"
                            p["sentiment_class"] = label
                        # Save enriched posts
                        tgt_dir = Path(outputs_dir) / ticker.upper() / "SOCIAL"
                        tgt_dir.mkdir(parents=True, exist_ok=True)
                        out_path = tgt_dir / f"{ticker.upper()}.json"
                        with open(out_path, "w", encoding="utf-8") as fh:
                            _json.dump(posts, fh, ensure_ascii=False, indent=2)
                    except Exception as exc:
                        print(f"⚠️  Failed to enrich social posts: {exc}")
        except Exception as exc:
            print(f"⚠️  Failed to generate social charts: {exc}")
    # Generate ESG and SEC charts using aggregated metrics
    if ticker:
        # Collect all esg_metrics and sec_metrics from rows for this ticker
        from typing import Any, Dict, List
        esg_metrics_list: List[Dict[str, Any]] = []
        sec_metrics_list: List[Dict[str, Any]] = []
        for row in rows:
            if row.get("esg_metrics"):
                esg_metrics_list.append(row["esg_metrics"])
            if row.get("sec_metrics"):
                sec_metrics_list.append(row["sec_metrics"])
        if esg_metrics_list:
            try:
                charts_root = Path("charts") / ticker.upper() / "ESG"
                generate_esg_charts(esg_metrics_list, charts_root)
                # Time-based ESG charts
                from esg_sec_charts import generate_esg_time_charts  # type: ignore
                generate_esg_time_charts(rows, charts_root)
            except Exception as exc:
                print(f"⚠️  Failed to generate ESG charts: {exc}")
        if sec_metrics_list:
            try:
                charts_root = Path("charts") / ticker.upper() / "SEC"
                generate_sec_charts(sec_metrics_list, charts_root)
                # Time-based SEC charts
                from esg_sec_charts import generate_sec_time_charts  # type: ignore
                generate_sec_time_charts(rows, charts_root)
            except Exception as exc:
                print(f"⚠️  Failed to generate SEC charts: {exc}")




def main(argv: Optional[list[str]] = None) -> None:
    """
    Entry point for the FinGuard command line interface.

    A ``--local`` flag has been added to support running the CLI in an
    offline/air‑gapped environment.  When the flag is provided, no
    metadata is pushed to AWS services such as S3 or Kinesis.  The flag
    sets an environment variable (``FG_LOCAL_ONLY=1``) which downstream
    modules (e.g. :mod:`metadata_io`) can inspect to disable any network
    operations.  All analysis results will still be written to local
    disk as before.

    Parameters
    ----------
    argv : list[str] or None
        Command line arguments.  If ``None`` (the default) the
        arguments from ``sys.argv`` will be used.
    """
    parser = argparse.ArgumentParser(description="FinGuard CLI")
    # Global options
    parser.add_argument(
        "--local",
        action="store_true",
        help=(
            "Run in local/offline mode. When set, analysis will be confined "
            "to the local filesystem and no data will be sent to AWS services."
        ),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # SEC command
    sec_parser = subparsers.add_parser("sec", help="Analyse a SEC filing")
    sec_parser.add_argument("file", help="Path to SEC text file")
    sec_parser.add_argument("--output", help="Path to write JSON output", default=None)

    # ESG command
    esg_parser = subparsers.add_parser("esg", help="Analyse an ESG report")
    esg_parser.add_argument("file", help="Path to ESG text file")
    esg_parser.add_argument("--output", help="Path to write JSON output", default=None)

    # Social command
    social_parser = subparsers.add_parser("social", help="Analyse social posts JSONL file")
    social_parser.add_argument("posts_file", help="Path to JSON lines of social posts")

    # Fetch command
    fetch_parser = subparsers.add_parser("fetch", help="Fetch a single SEC filing and save as plain text")
    fetch_parser.add_argument("url", help="EDGAR index URL of the filing (ends with -index.htm)")
    fetch_parser.add_argument("company", help="Company name for the filing")
    fetch_parser.add_argument("date", help="Filing date in YYYY-MM-DD format")
    fetch_parser.add_argument("form", help="Filing type, e.g. 10-K or 10-Q")

    # Extract command
    extract_parser = subparsers.add_parser("extract", help="Extract and analyse SEC, ESG or social data")
    extract_parser.add_argument(
        "dtype",
        choices=["sec", "esg", "social"],
        help="Type of data to analyse",
    )
    extract_parser.add_argument("input", help="Path to input file")
    extract_parser.add_argument(
        "--output",
        help="Optional path to write JSON output",
        default=None,
    )

    # Pipeline command
    pipeline_parser = subparsers.add_parser("pipeline", help="Run the full pipeline")
    pipeline_parser.add_argument("--texts-dir", default="texts", help="Directory with raw text files")
    pipeline_parser.add_argument("--outputs-dir", default="processed", help="Directory to store processed JSONs")
    pipeline_parser.add_argument("--social-dir", default=None, help="Directory containing social posts files")
    pipeline_parser.add_argument(
        "--filter-company",
        default=None,
        help="Only output rows for companies containing this string (case insensitive)",
    )
    pipeline_parser.add_argument(
        "--ticker",
        required=True,
        help=(
            "Ticker symbol for the company to process. Only filings whose company identifier "
            "contains this ticker (case-insensitive) will be analysed. This option is "
            "required to avoid scanning the entire corpus."
        ),
    )
    pipeline_parser.add_argument(
        "--data-root",
        default="datalake/events",
        help="Root directory of local data lake manifests used for fetching SEC/ESG/social data",
    )
    pipeline_parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="Only fetch raw data for the specified ticker without running the pipeline",
    )

    # Dynamo options for pipeline
    pipeline_parser.add_argument(
        "--dynamo-table",
        default="fin_guard_partitions",
        help=(
            "DynamoDB table containing fin_guard partitions (default: fin_guard_partitions)"
        ),
    )
    pipeline_parser.add_argument(
        "--dynamo-region",
        default=None,
        help=(
            "AWS region for DynamoDB (use default AWS config if omitted)"
        ),
    )
    pipeline_parser.add_argument(
        "--use-dynamo",
        action="store_true",
        help=(
            "Use DynamoDB fin_guard_partitions as the source of manifests instead of walking data_root"
        ),
    )
    pipeline_parser.add_argument(
        "--reprocess",
        action="store_true",
        help=(
            "If set, ignore partition processed flag and re-fetch all manifests for this ticker"
        ),
    )


    args = parser.parse_args(argv)

    # Activate local mode if requested.  Downstream modules may read
    # this environment variable (``FG_LOCAL_ONLY``) to disable any
    # network operations.  Setting the variable here avoids the need to
    # propagate the flag through every function call.
    if getattr(args, "local", False):
        import os

        os.environ["FG_LOCAL_ONLY"] = "1"

    if args.command == "sec":
        _analyse_sec(args.file, args.output)
    elif args.command == "esg":
        _analyse_esg(args.file, args.output)
    elif args.command == "social":
        _analyse_social(args.posts_file)
    elif args.command == "fetch":
        # Defer to the fetch module's main function to perform the fetch
        try:
            from fetch import save_filing  # type: ignore
        except Exception as exc:
            raise ImportError("fetch module could not be imported") from exc
        save_filing(args.url, args.company, args.date, args.form)
    elif args.command == "extract":
        # Defer to the extract module's main function to perform analysis
        try:
            from extract import main as run_extract_main  # type: ignore
        except Exception as exc:
            raise ImportError("extract module could not be imported") from exc
        run_extract_main([args.dtype, args.input] + ([args.output] if args.output else []))
    elif args.command == "pipeline":
        _run_pipeline(
            args.texts_dir,
            args.outputs_dir,
            args.social_dir,
            args.filter_company,
            args.ticker,
            data_root=args.data_root,
            fetch_only=args.fetch_only,
            use_dynamo=args.use_dynamo,
            dynamo_table=args.dynamo_table,
            dynamo_region=args.dynamo_region,
            reprocess=args.reprocess,
        )
    else:
        parser.print_help()


if __name__ == "__main__":
    main(sys.argv[1:])