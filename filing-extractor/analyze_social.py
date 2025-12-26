#!/usr/bin/env python3
"""
Social sentiment analyser
=========================

This module exposes a simple interface to compute sentiment metrics from
social media posts.  Given a file containing one JSON object per line
(representing individual social posts), it loads the posts, computes basic
statistics using NLTK's VADER sentiment analyser and optionally writes
the results to a JSON file.  If no output file is provided the metrics
are printed to standard output.  The metrics include average sentiment,
sentiment volatility (standard deviation of daily mean sentiment) and
message volume (total number of posts).

Usage examples::

    # Compute metrics and print to the console
    python -m filing_extractor.analyze_social data/AAPL_posts.json

    # Write metrics to a JSON file
    python -m filing_extractor.analyze_social data/AAPL_posts.json results/AAPL_social.json

This script mirrors the behaviour of :mod:`analyze_sec` and :mod:`analyze_esg` by
providing a top‑level ``main`` function which accepts an input file and an
optional output file.  It leverages the :class:`SocialAnalyzer` defined in this
module to perform the heavy lifting so that sentiment analysis logic remains
centralised.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# NOTE: The social sentiment analysis logic used to live in the pipeline module.  In
# order to decouple responsibilities and make the pipeline a pure orchestrator,
# the ``SocialAnalyzer`` class has been moved here.  This allows the
# standalone social analysis script to operate without importing the entire
# pipeline and avoids circular dependencies.

try:
    from nltk.sentiment import SentimentIntensityAnalyzer  # type: ignore
    import nltk  # type: ignore
except Exception as exc:  # pragma: no cover - import errors are surfaced at runtime
    # We import nltk lazily inside SocialAnalyzer.__init__; if imports fail here
    # tests will still run, but the analyser will raise in __init__.
    pass


class SocialAnalyzer:
    """Compute sentiment metrics from social media posts.

    Given a collection of posts for a single ticker (as a list of strings or a
    loaded DataFrame), this analyser computes basic statistics: average
    sentiment, sentiment volatility (standard deviation) and message volume.
    Sentiment is calculated using VADER from NLTK.  You can plug in a
    different model by overriding :meth:`score_text`.
    """

    def __init__(self) -> None:
        """Initialise the sentiment analyser.

        The analyser attempts to use NLTK's VADER model if it is available. If
        NLTK or the VADER lexicon cannot be loaded, a lightweight fallback
        will be used which computes only message counts and assumes neutral
        sentiment.  This allows social sentiment features to be returned
        even when the full NLTK dependency is missing.
        """
        try:
            from nltk.sentiment import SentimentIntensityAnalyzer  # type: ignore
            import nltk  # type: ignore
            nltk.download("vader_lexicon", quiet=True)
            self._analyser = SentimentIntensityAnalyzer()
            self._use_vader = True
        except Exception:
            # Fallback: mark that VADER is unavailable and use neutral sentiment
            self._analyser = None
            self._use_vader = False

    def score_text(self, text: str) -> float:
        """Return a compound sentiment score for the given text.

        If VADER is available this will use the pretrained model to
        compute a score in [-1, 1].  Otherwise a neutral score of 0.0 is
        returned.  Override this method to integrate a different sentiment
        model.
        """
        if self._use_vader and self._analyser is not None:
            try:
                return float(self._analyser.polarity_scores(text)["compound"])
            except Exception:
                return 0.0
        # Fallback: neutral sentiment if no analyser available
        return 0.0

    def compute_features(
        self,
        posts: List[Dict[str, Any]],
        *,
        date_key: str = "timestamp",
        text_key: str = "content",
    ) -> Dict[str, float]:
        """Aggregate sentiment metrics over a collection of posts.

        Parameters
        ----------
        posts : list of dict
            Each post must have a timestamp and text content.  The date
            component of the timestamp is used to group messages by day.
        date_key : str, optional
            Key of the timestamp field in each post.  Must parse to a date via
            ``pandas.to_datetime``.
        text_key : str, optional
            Key of the text content field in each post.

        Returns
        -------
        dict
            Dictionary with average sentiment, sentiment volatility and
            message count.
        """
        # If there are no posts, return zeros
        if not posts:
            return {"avg_sentiment": 0.0, "sentiment_volatility": 0.0, "message_count": 0.0}
        # When the VADER analyser is available we rely on pandas for grouping by day
        if self._use_vader:
            try:
                import pandas as pd  # type: ignore
            except Exception:
                # If pandas is unavailable we fall back to a simplified computation
                self._use_vader = False
        if self._use_vader:
            # Convert to DataFrame for convenience
            import pandas as pd  # type: ignore
            df = pd.DataFrame(posts)
            if date_key not in df or text_key not in df:
                # Missing required columns; compute naive features instead
                self._use_vader = False
            else:
                df["date"] = pd.to_datetime(df[date_key], errors="coerce").dt.date
                df["score"] = df[text_key].astype(str).map(self.score_text)
                daily = df.groupby("date")["score"].mean()
                avg = df["score"].mean()
                vol = daily.std(ddof=0) if len(daily) > 1 else 0.0
                return {
                    "avg_sentiment": float(round(avg, 6)),
                    "sentiment_volatility": float(round(vol, 6)),
                    "message_count": float(len(df)),
                }
        # Fallback path: compute only message count and assume neutral sentiment
        return {
            "avg_sentiment": 0.0,
            "sentiment_volatility": 0.0,
            "message_count": float(len(posts)),
        }


def _load_posts(file_path: str) -> List[Dict[str, Any]]:
    """Load a JSON lines file containing social posts.

    Each line should be a valid JSON object with at least a ``timestamp`` and
    ``content`` field.  Lines that fail to parse are silently skipped.

    Parameters
    ----------
    file_path : str
        Path to the JSON lines file containing social posts.

    Returns
    -------
    list of dict
        A list of post records.
    """
    posts: List[Dict[str, Any]] = []
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                posts.append(json.loads(line))
            except Exception:
                # Skip malformed lines silently
                continue
    return posts


def _load_posts(file_path: str) -> List[Dict[str, Any]]:
    """Load a JSON lines file containing social posts.

    Each line should be a valid JSON object with at least a ``timestamp`` and
    ``content`` field.  Lines that fail to parse are silently skipped.

    Parameters
    ----------
    file_path : str
        Path to the JSON lines file containing social posts.

    Returns
    -------
    list of dict
        A list of post records.
    """
    posts: List[Dict[str, Any]] = []
    with open(file_path, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                posts.append(json.loads(line))
            except Exception:
                # Skip malformed lines silently
                continue
    return posts


def main(input_file: str, output_file: Optional[str] = None) -> None:
    """Compute social sentiment metrics for a given posts file.

    Parameters
    ----------
    input_file : str
        Path to a JSON lines file containing social posts.
    output_file : str or None
        If provided, the metrics will be written as JSON to this path.  If
        omitted, the metrics are printed to standard output.
    """
    posts = _load_posts(input_file)
    analyser = SocialAnalyzer()
    metrics = analyser.compute_features(posts)
    if output_file:
        out_path = Path(output_file)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    else:
        print(json.dumps(metrics, indent=2))

    # Publish processed record to Kinesis for downstream processing
    try:
        from kinesis_client import publish_processed_filing, extract_metadata_from_path
        
        metadata = extract_metadata_from_path(input_file)
        
        # Extract source from filename: NVDA_stocktwits.json or NVDA_youtube.json
        source = "SOCIAL"  # default fallback
        file_stem = Path(input_file).stem  # "NVDA_stocktwits" or "NVDA_youtube"
        if "_" in file_stem:
            source_part = file_stem.split("_")[-1].upper()  # "STOCKTWITS" or "YOUTUBE"
            # Only use if it's a valid source
            if source_part in ["STOCKTWITS", "YOUTUBE"]:
                source = source_part
        
        # For social posts, use the max timestamp as the filing date
        filing_date = ""
        if posts:
            try:
                import pandas as pd
                timestamps = [p.get("timestamp", "") for p in posts if p.get("timestamp")]
                if timestamps:
                    filing_date = str(pd.to_datetime(max(timestamps), errors="coerce").date())
            except Exception:
                pass
        
        filing_record = {
            "company": "",  # Not available for social posts
            "ticker": metadata["ticker"],
            "source": source,  # Changed from hardcoded "SOCIAL"
            "form": source,    # Changed from hardcoded "STOCKTWITS"
            "filingDate": filing_date,
            "accession": "",
            "url": "",
            "stage": "PROCESSED"
        }
        publish_processed_filing(filing_record)
    except Exception as e:
        print(f"⚠️  Failed to publish to Kinesis: {e}")


if __name__ == "__main__":
    # When invoked from the command line, parse up to two positional
    # arguments: the input file and an optional output file.  Display a
    # helpful usage message if insufficient arguments are supplied.
    args = sys.argv[1:]
    if not args:
        print("Usage: analyze_social.py <input_file> [output_file]", file=sys.stderr)
        sys.exit(1)
    input_path = args[0]
    output_path = args[1] if len(args) > 1 else None
    main(input_path, output_path)