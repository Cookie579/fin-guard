#!/usr/bin/env python3
"""
Unified extractor for SEC, ESG and social data
=============================================

This module serves as a thin wrapper around the individual analysis scripts
for SEC filings, ESG reports and social media sentiment.  It provides a
single command line interface that accepts a data type (``sec``, ``esg`` or
``social``), an input file and an optional output file.  The appropriate
analysis function is invoked and the resulting JSON is either written to
disk or printed to standard output.

Example usage::

    # Analyse a single SEC filing and write the result to a JSON file
    python -m filing_extractor.extract sec texts/Apple/10-K/Apple__2023-10-27__10-K.txt \
        processed/Apple/10-K/Apple__2023-10-27__10-K__Analyzed.json

    # Analyse an ESG report, letting the script construct the default output path
    python -m filing_extractor.extract esg texts/Apple/ESG/report1.txt

    # Analyse a social posts file and print metrics to the console
    python -m filing_extractor.extract social social/AAPL_posts.json

Internally, the script imports the ``main`` functions from
``analyze_sec``, ``analyze_esg`` and ``analyze_social`` so that analysis
logic is centralised.  It does not perform any type detection; callers
must specify the data type explicitly.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

# Ensure local modules can be imported.  Insert the directory of this file
# onto sys.path so that analyze_sec.py and other modules can be found even
# when the package name contains a hyphen and Python cannot resolve
# relative imports.
import os as _os
_sys = sys  # alias to avoid name shadowing
_sys.path.insert(0, _os.path.dirname(__file__))

# Import analysis functions.  The ``main`` functions accept an input file
# and optionally an output file, and write JSON outputs to disk.  We
# ignore type hints here because these modules may not always be present in
# minimal installations.
try:
    from analyze_sec import main as run_sec_analysis  # type: ignore
except Exception as exc:
    run_sec_analysis = None  # type: ignore
    _sec_import_error = exc
try:
    from analyze_esg import main as run_esg_analysis  # type: ignore
except Exception as exc:
    run_esg_analysis = None  # type: ignore
    _esg_import_error = exc
try:
    from analyze_social import main as run_social_analysis  # type: ignore
except Exception as exc:
    run_social_analysis = None  # type: ignore
    _social_import_error = exc


def _analyse_sec(file_path: str, output: str | None) -> None:
    if run_sec_analysis is None:
        raise ImportError(
            "analyze_sec could not be imported"
        ) from _sec_import_error  # type: ignore
    out_path = Path(output) if output else None
    run_sec_analysis(input_file=file_path, output_file=out_path)


def _analyse_esg(file_path: str, output: str | None) -> None:
    if run_esg_analysis is None:
        raise ImportError(
            "analyze_esg could not be imported"
        ) from _esg_import_error  # type: ignore
    out_path = Path(output) if output else None
    run_esg_analysis(input_file=file_path, output_file=out_path)


def _analyse_social(file_path: str, output: str | None) -> None:
    if run_social_analysis is None:
        raise ImportError(
            "analyze_social could not be imported"
        ) from _social_import_error  # type: ignore
    # The social analyser will write JSON if an output path is provided,
    # otherwise it prints to stdout.  Capture its printed output so we
    # can embed metrics into a JSON if no output file is given.
    out_path = Path(output) if output else None
    if out_path:
        run_social_analysis(file_path, out_path)
    else:
        # When no output is provided, run_social_analysis prints metrics.
        run_social_analysis(file_path, None)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Extract and analyse SEC/ESG/social data")
    parser.add_argument(
        "dtype",
        choices=["sec", "esg", "social"],
        help="Type of data to analyse (sec, esg, social)",
    )
    parser.add_argument("input", help="Path to the input file")
    parser.add_argument(
        "--output",
        help="Path to write the JSON output. If omitted, a default path will be constructed",
        default=None,
    )
    args = parser.parse_args(argv)
    dtype = args.dtype
    input_file = args.input
    output_file = args.output
    if dtype == "sec":
        _analyse_sec(input_file, output_file)
    elif dtype == "esg":
        _analyse_esg(input_file, output_file)
    else:
        _analyse_social(input_file, output_file)


if __name__ == "__main__":
    main()