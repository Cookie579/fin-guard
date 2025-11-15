#!/usr/bin/env python3
"""
Social Charts Generator
=======================

This module provides a helper function to visualise social media data
for a given ticker.  It loads a JSON file containing a list of posts
and produces a series of charts that illustrate trends relevant to
economic investment and investor sentiment.  The generated images are
written to a specified output directory.

The charts include:

1. Daily message volume over time.
2. Daily average sentiment over time.
3. Distribution of sentiment scores.
4. Counts of positive, neutral and negative messages.
5. Top 10 most common words (excluding stopwords and ticker symbols).
6. Scatter of daily message count vs average sentiment.
7. Distribution of posts by day of week.

Usage::

    from pathlib import Path
    from social_charts import generate_social_charts

    generate_social_charts(Path("social/TSLA.json"), Path("charts/TSLA"))

If the posts file is not a JSON array, the function will attempt to
parse it as JSON lines.  Missing dependencies (pandas, matplotlib)
will cause an ImportError.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import List, Dict, Any


def _load_posts_generic(file_path: Path) -> List[Dict[str, Any]]:
    """Load posts from a JSON array or JSON lines file.

    The posts file may be either a single JSON array or one JSON object
    per line.  This helper will try to parse the file accordingly.

    Parameters
    ----------
    file_path : Path
        Path to the posts file.

    Returns
    -------
    list of dict
        Parsed posts.
    """
    posts: List[Dict[str, Any]] = []
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)
        if isinstance(data, list):
            return data
        # If the top-level object is not a list, fall back to JSON lines
    except Exception:
        pass
    # Fallback: JSON lines
    try:
        with open(file_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                posts.append(obj)
    except Exception:
        pass
    return posts


def generate_social_charts(posts_file: Path, output_dir: Path) -> List[Path]:
    """Generate a suite of charts visualising social data for a ticker.

    This function reads a posts file containing social media posts and
    produces a number of charts that are relevant to investor behaviour.
    The charts are saved as PNG files in the specified output directory.

    Parameters
    ----------
    posts_file : Path
        Path to the JSON file containing social posts.
    output_dir : Path
        Directory where charts will be written.  It will be created if
        necessary.

    Returns
    -------
    list[Path]
        Paths to the generated image files.
    """
    try:
        import pandas as pd  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
        from collections import Counter
        import re
    except Exception as exc:
        raise ImportError("pandas and matplotlib are required to generate charts") from exc

    posts = _load_posts_generic(posts_file)
    if not posts:
        return []
    df = pd.DataFrame(posts)
    # Ensure expected columns exist
    if "timestamp" not in df.columns:
        df["timestamp"] = None
    if "content" not in df.columns:
        df["content"] = ""
    # Convert timestamp to datetime and extract date.  Missing timestamps
    # produce ``NaT``, which we fill with a sentinel.  We then derive
    # ``date`` and ``weekday`` fields using this sentinel so that
    # charts remain populated even when timestamps are absent.  Using
    # the string "Unknown" for missing dates avoids empty plots.
    # Convert timestamps to pandas datetime.  Use errors='coerce' to produce
    # NaT for unparsable values.  Afterwards, fill NaT values with a
    # fallback datetime (today).  Finally, cast the column to datetime64
    # explicitly to guarantee that the .dt accessor is available even
    # when the column contains a mixture of datetime and object types.
    df["datetime"] = pd.to_datetime(df["timestamp"], errors="coerce")
    df.loc[df["datetime"].isna(), "datetime"] = pd.Timestamp.today()
    # Force dtype to datetime64[ns] so .dt accessor works reliably
    df["datetime"] = pd.to_datetime(df["datetime"])  # type: ignore
    df["date"] = df["datetime"].dt.date
    # Compute sentiment scores using SocialAnalyzer if available
    try:
        from analyze_social import SocialAnalyzer  # type: ignore
        analyser = SocialAnalyzer()  # type: ignore
        df["score"] = df["content"].astype(str).apply(lambda x: analyser.score_text(x))
    except Exception:
        # If analyser cannot be loaded, assume neutral sentiment
        df["score"] = 0.0
    # Classify sentiment categories
    def _cat(score: float) -> str:
        if score > 0.05:
            return "positive"
        if score < -0.05:
            return "negative"
        return "neutral"
    df["sentiment"] = df["score"].apply(_cat)
    # Prepare output directory
    output_dir.mkdir(parents=True, exist_ok=True)
    generated: List[Path] = []
    # 1. Daily message volume
    vol = df.groupby("date")["content"].count().sort_index()
    plt.figure()
    vol.plot()
    plt.title("Daily Message Volume")
    plt.xlabel("Date")
    plt.ylabel("Number of Posts")
    plt.xticks(rotation=45)
    plt.tight_layout()
    path1 = output_dir / "daily_message_volume.png"
    plt.savefig(path1)
    plt.close()
    generated.append(path1)
    # 2. Daily average sentiment
    daily_sent = df.groupby("date")["score"].mean().sort_index()
    plt.figure()
    daily_sent.plot()
    plt.title("Daily Average Sentiment")
    plt.xlabel("Date")
    plt.ylabel("Average Sentiment Score")
    plt.xticks(rotation=45)
    plt.tight_layout()
    path2 = output_dir / "daily_average_sentiment.png"
    plt.savefig(path2)
    plt.close()
    generated.append(path2)
    # 3. Distribution of sentiment scores
    plt.figure()
    df["score"].hist(bins=30)
    plt.title("Distribution of Sentiment Scores")
    plt.xlabel("Sentiment Score")
    plt.ylabel("Frequency")
    plt.tight_layout()
    path3 = output_dir / "sentiment_score_distribution.png"
    plt.savefig(path3)
    plt.close()
    generated.append(path3)
    # 4. Sentiment categories count
    counts = df["sentiment"].value_counts()
    plt.figure()
    counts.plot(kind="bar")
    plt.title("Sentiment Classification Counts")
    plt.xlabel("Sentiment Category")
    plt.ylabel("Number of Posts")
    plt.tight_layout()
    path4 = output_dir / "sentiment_classification_counts.png"
    plt.savefig(path4)
    plt.close()
    generated.append(path4)
    # 5. Top 10 words (excluding stopwords and tickers)
    try:
        from nltk.corpus import stopwords  # type: ignore
        stop_words = set(stopwords.words("english"))
    except Exception:
        stop_words = set()
    # Build a simple regex to find words consisting of letters only
    word_pattern = re.compile(r"\b[a-zA-Z]{3,}\b")
    words: List[str] = []
    tickers_lower = {str(posts_file.stem).lower()}
    for text in df["content"].astype(str):
        for match in word_pattern.findall(text):
            w = match.lower()
            # Skip stopwords and known tickers (e.g. tsla) and single letters
            if w in stop_words or w in tickers_lower:
                continue
            words.append(w)
    common = Counter(words).most_common(10)
    if common:
        labels, freqs = zip(*common)
        plt.figure()
        plt.bar(labels, freqs)
        plt.title("Top 10 Words in Posts")
        plt.xlabel("Word")
        plt.ylabel("Frequency")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path5 = output_dir / "top_words.png"
        plt.savefig(path5)
        plt.close()
        generated.append(path5)
    # 6. Daily volume vs sentiment scatter
    if not vol.empty and not daily_sent.empty:
        # Align indices
        aligned = pd.DataFrame({"volume": vol, "avg_sent": daily_sent}).dropna()
        if not aligned.empty:
            plt.figure()
            plt.scatter(aligned["volume"], aligned["avg_sent"])
            plt.title("Daily Volume vs Average Sentiment")
            plt.xlabel("Number of Posts")
            plt.ylabel("Average Sentiment Score")
            plt.tight_layout()
            path6 = output_dir / "volume_vs_sentiment.png"
            plt.savefig(path6)
            plt.close()
            generated.append(path6)
    # 7. Posts by day of week
    df["weekday"] = df["datetime"].dt.day_name().fillna("Unknown")
    w_counts = df["weekday"].value_counts().reindex(
        ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday", "Unknown"],
        fill_value=0,
    )
    plt.figure()
    w_counts.plot(kind="bar")
    plt.title("Posts by Day of Week")
    plt.xlabel("Day of Week")
    plt.ylabel("Number of Posts")
    plt.tight_layout()
    path7 = output_dir / "posts_by_weekday.png"
    plt.savefig(path7)
    plt.close()
    generated.append(path7)

    # -------------------------------------------------------------------------
    # Additional charts for deeper social insights (economic investment focus)
    # -------------------------------------------------------------------------
    # 8. Posts by month of year
    df["month"] = df["datetime"].dt.month_name().fillna("Unknown")
    month_order = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
        "Unknown",
    ]
    month_counts = df["month"].value_counts().reindex(month_order, fill_value=0)
    plt.figure()
    month_counts.plot(kind="bar")
    plt.title("Posts by Month")
    plt.xlabel("Month")
    plt.ylabel("Number of Posts")
    plt.tight_layout()
    path8 = output_dir / "posts_by_month.png"
    plt.savefig(path8)
    plt.close()
    generated.append(path8)
    # 9. Posts by hour of day
    df["hour"] = df["datetime"].dt.hour.fillna(-1).astype(int)
    hour_counts = df["hour"].value_counts().sort_index()
    plt.figure()
    hour_counts.plot(kind="bar")
    plt.title("Posts by Hour of Day")
    plt.xlabel("Hour of Day")
    plt.ylabel("Number of Posts")
    plt.tight_layout()
    path9 = output_dir / "posts_by_hour.png"
    plt.savefig(path9)
    plt.close()
    generated.append(path9)
    # 10. Cumulative posts over time
    # We reuse the daily message volume computed earlier
    if not vol.empty:
        cum_posts = vol.cumsum()
        plt.figure()
        cum_posts.plot()
        plt.title("Cumulative Posts Over Time")
        plt.xlabel("Date")
        plt.ylabel("Cumulative Posts")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path10 = output_dir / "cumulative_posts.png"
        plt.savefig(path10)
        plt.close()
        generated.append(path10)
    # 11. Top 10 bigrams
    # Build tokens by splitting on non‑alphabetic characters
    import re
    tokens: List[str] = []
    for text in df["content"].astype(str):
        tokens += re.findall(r"[A-Za-z]{3,}", text.lower())
    # Filter tokens: remove stopwords and tickers
    filtered_tokens: List[str] = []
    for w in tokens:
        if w in stop_words or w in tickers_lower:
            continue
        filtered_tokens.append(w)
    # Compute bigrams
    bigrams: List[tuple[str, str]] = []
    for i in range(len(filtered_tokens) - 1):
        a, b = filtered_tokens[i], filtered_tokens[i + 1]
        if a in stop_words or b in stop_words:
            continue
        bigrams.append((a, b))
    from collections import Counter as _Counter
    bigram_counts = _Counter(bigrams).most_common(10)
    if bigram_counts:
        bigram_labels = [f"{w1} {w2}" for w1, w2 in [bg for bg, _ in bigram_counts]]
        bigram_freqs = [count for _, count in bigram_counts]
        plt.figure()
        plt.bar(bigram_labels, bigram_freqs)
        plt.title("Top 10 Bigrams in Posts")
        plt.xlabel("Bigram")
        plt.ylabel("Frequency")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path11 = output_dir / "top_bigrams.png"
        plt.savefig(path11)
        plt.close()
        generated.append(path11)
    # 12. Average sentiment per top tickers (by post count)
    # Extract tickers from content, e.g. $TSLA; allow both uppercase and lowercase
    ticker_pattern = re.compile(r"\$[A-Za-z]{1,5}")
    ticker_scores: Dict[str, List[float]] = {}
    for _, row in df.iterrows():
        content = str(row["content"]).upper()
        for tick in ticker_pattern.findall(content):
            t = tick[1:]  # strip leading $
            ticker_scores.setdefault(t, []).append(row["score"])
    # Compute average sentiment and counts
    ticker_avgs = {t: sum(vals) / len(vals) for t, vals in ticker_scores.items() if vals}
    # Order tickers by number of mentions
    ticker_counts = {t: len(vals) for t, vals in ticker_scores.items()}
    if ticker_counts:
        # Take top 10 tickers by count
        top_tickers = sorted(ticker_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
        labels_tick = [t for t, _ in top_tickers]
        counts_tick = [ticker_counts[t] for t in labels_tick]
        avg_scores = [ticker_avgs.get(t, 0.0) for t in labels_tick]
        # Plot counts
        plt.figure()
        plt.bar(labels_tick, counts_tick)
        plt.title("Top Ticker Mentions")
        plt.xlabel("Ticker")
        plt.ylabel("Number of Mentions")
        plt.tight_layout()
        path12 = output_dir / "top_ticker_mentions.png"
        plt.savefig(path12)
        plt.close()
        generated.append(path12)
        # Plot average sentiment for same tickers
        plt.figure()
        plt.bar(labels_tick, avg_scores)
        plt.title("Average Sentiment per Top Ticker")
        plt.xlabel("Ticker")
        plt.ylabel("Average Sentiment")
        plt.tight_layout()
        path13 = output_dir / "avg_sentiment_per_ticker.png"
        plt.savefig(path13)
        plt.close()
        generated.append(path13)
    # 13. Sentiment category counts per month (stacked bar)
    sent_pivot = df.pivot_table(index="month", columns="sentiment", values="content", aggfunc="count", fill_value=0)
    # Ensure order of months
    sent_pivot = sent_pivot.reindex(month_order, fill_value=0)
    plt.figure()
    # Stacked bar: plot each sentiment as a layer
    bottom = None
    for cat in ["positive", "neutral", "negative"]:
        vals = sent_pivot.get(cat, pd.Series([0]*len(sent_pivot), index=sent_pivot.index))
        plt.bar(sent_pivot.index, vals, bottom=bottom, label=cat)
        bottom = vals if bottom is None else bottom + vals
    plt.title("Sentiment Distribution by Month")
    plt.xlabel("Month")
    plt.ylabel("Number of Posts")
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    path14 = output_dir / "sentiment_by_month.png"
    plt.savefig(path14)
    plt.close()
    generated.append(path14)
    # 14. Post length distribution (number of words)
    # Compute length as count of word tokens in each post
    df["length"] = df["content"].astype(str).apply(lambda x: len(re.findall(r"\b\w+\b", x)))
    plt.figure()
    df["length"].hist(bins=30)
    plt.title("Distribution of Post Lengths")
    plt.xlabel("Number of Words in Post")
    plt.ylabel("Frequency")
    plt.tight_layout()
    path15 = output_dir / "post_length_distribution.png"
    plt.savefig(path15)
    plt.close()
    generated.append(path15)
    # 15. Top 10 hashtags
    hashtag_pattern = re.compile(r"#[A-Za-z0-9_]+")
    hashtag_counts: Dict[str, int] = {}
    for text in df["content"].astype(str):
        for tag in hashtag_pattern.findall(text):
            tag_u = tag.lower()
            hashtag_counts[tag_u] = hashtag_counts.get(tag_u, 0) + 1
    if hashtag_counts:
        top_hashtags = sorted(hashtag_counts.items(), key=lambda kv: kv[1], reverse=True)[:10]
        labels_hash, freq_hash = zip(*top_hashtags)
        plt.figure()
        plt.bar(labels_hash, freq_hash)
        plt.title("Top 10 Hashtags")
        plt.xlabel("Hashtag")
        plt.ylabel("Frequency")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path16 = output_dir / "top_hashtags.png"
        plt.savefig(path16)
        plt.close()
        generated.append(path16)
    # 16. Weighted average sentiment per month
    # Weighted by post length so that longer posts have more influence
    if "length" not in df.columns:
        df["length"] = df["content"].astype(str).apply(lambda x: len(re.findall(r"\b\w+\b", x)))
    df["weighted_score"] = df["score"] * df["length"]
    monthly_length = df.groupby("month")["length"].sum()
    monthly_weighted = df.groupby("month")["weighted_score"].sum()
    # Avoid division by zero
    weighted_avg = monthly_weighted.divide(monthly_length.where(monthly_length != 0, 1))
    # Reindex to month_order
    weighted_avg = weighted_avg.reindex(month_order, fill_value=0)
    plt.figure()
    weighted_avg.plot(kind="bar")
    plt.title("Weighted Average Sentiment per Month")
    plt.xlabel("Month")
    plt.ylabel("Weighted Avg Sentiment")
    plt.tight_layout()
    path17 = output_dir / "weighted_sentiment_by_month.png"
    plt.savefig(path17)
    plt.close()
    generated.append(path17)

    # ---------------------------------------------------------------------
    # Additional charts leveraging likes, reshares and dataset sentiment
    # ---------------------------------------------------------------------
    # Some posts may include numeric fields 'likes' and 'reshares'.  Ensure
    # these exist as numeric columns (convert non-numeric values to NaN).
    for col in ["likes", "reshares"]:
        if col not in df.columns:
            df[col] = None
    # Convert to numeric, coercing errors to NaN
    df["likes"] = pd.to_numeric(df["likes"], errors="coerce")
    df["reshares"] = pd.to_numeric(df["reshares"], errors="coerce")
    # 18. Distribution of likes per post
    if df["likes"].notna().any():
        plt.figure()
        df["likes"].dropna().astype(int).hist(bins=30)
        plt.title("Distribution of Likes per Post")
        plt.xlabel("Likes")
        plt.ylabel("Frequency")
        plt.tight_layout()
        path18 = output_dir / "likes_distribution.png"
        plt.savefig(path18)
        plt.close()
        generated.append(path18)
    # 19. Distribution of reshares per post
    if df["reshares"].notna().any():
        plt.figure()
        df["reshares"].dropna().astype(int).hist(bins=30)
        plt.title("Distribution of Reshares per Post")
        plt.xlabel("Reshares")
        plt.ylabel("Frequency")
        plt.tight_layout()
        path19 = output_dir / "reshares_distribution.png"
        plt.savefig(path19)
        plt.close()
        generated.append(path19)
    # 20. Daily average likes
    if df["likes"].notna().any():
        daily_likes = df.groupby("date")["likes"].mean().sort_index()
        plt.figure()
        daily_likes.plot()
        plt.title("Daily Average Likes")
        plt.xlabel("Date")
        plt.ylabel("Average Likes")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path20 = output_dir / "daily_average_likes.png"
        plt.savefig(path20)
        plt.close()
        generated.append(path20)
    # 21. Daily average reshares
    if df["reshares"].notna().any():
        daily_reshares = df.groupby("date")["reshares"].mean().sort_index()
        plt.figure()
        daily_reshares.plot()
        plt.title("Daily Average Reshares")
        plt.xlabel("Date")
        plt.ylabel("Average Reshares")
        plt.xticks(rotation=45)
        plt.tight_layout()
        path21 = output_dir / "daily_average_reshares.png"
        plt.savefig(path21)
        plt.close()
        generated.append(path21)
    # 22. Counts of dataset sentiment labels (Bullish/Bearish/Other)
    if "sentiment_label" in df.columns:
        sent_counts = df["sentiment_label"].fillna("Unknown").value_counts()
        plt.figure()
        sent_counts.plot(kind="bar")
        plt.title("Counts of Dataset Sentiment Labels")
        plt.xlabel("Sentiment Label")
        plt.ylabel("Number of Posts")
        plt.tight_layout()
        path22 = output_dir / "sentiment_label_counts.png"
        plt.savefig(path22)
        plt.close()
        generated.append(path22)
        # Create numeric dataset sentiment: Bullish=1, Bearish=-1, else 0
        mapping = {"Bullish": 1, "Bearish": -1}
        df["dataset_sent"] = df["sentiment_label"].map(mapping).fillna(0)
        # 23. Daily average dataset sentiment
        daily_dataset_sent = df.groupby("date")["dataset_sent"].mean().sort_index()
        if not daily_dataset_sent.empty:
            plt.figure()
            daily_dataset_sent.plot()
            plt.title("Daily Average Dataset Sentiment")
            plt.xlabel("Date")
            plt.ylabel("Average Dataset Sentiment")
            plt.xticks(rotation=45)
            plt.tight_layout()
            path23 = output_dir / "daily_average_dataset_sentiment.png"
            plt.savefig(path23)
            plt.close()
            generated.append(path23)
    # 24. Top tickers by average likes
    # If symbols column exists and likes available
    if "symbols" in df.columns and df["likes"].notna().any():
        ticker_likes: Dict[str, List[int]] = {}
        for _, row in df.iterrows():
            syms = row.get("symbols")
            if isinstance(syms, list) and len(syms) > 0 and pd.notna(row["likes"]):
                for sym in syms:
                    ticker_likes.setdefault(sym, []).append(int(row["likes"]))
        if ticker_likes:
            avg_likes = {t: (sum(vals) / len(vals)) for t, vals in ticker_likes.items() if vals}
            top_likes = sorted(avg_likes.items(), key=lambda kv: kv[1], reverse=True)[:10]
            if top_likes:
                labels_like, avgs_like = zip(*top_likes)
                plt.figure()
                plt.bar(labels_like, avgs_like)
                plt.title("Top Tickers by Average Likes")
                plt.xlabel("Ticker")
                plt.ylabel("Average Likes")
                plt.tight_layout()
                path24 = output_dir / "top_tickers_avg_likes.png"
                plt.savefig(path24)
                plt.close()
                generated.append(path24)
    # 25. Sentiment score vs Likes scatter (only for posts with both score and likes)
    if df["likes"].notna().any():
        subset = df[["score", "likes"]].dropna()
        if not subset.empty:
            plt.figure()
            plt.scatter(subset["likes"], subset["score"])
            plt.title("Sentiment Score vs Likes")
            plt.xlabel("Likes")
            plt.ylabel("Sentiment Score")
            plt.tight_layout()
            path25 = output_dir / "sentiment_vs_likes.png"
            plt.savefig(path25)
            plt.close()
            generated.append(path25)
    # 26. Likes vs Reshares scatter
    if df[["likes", "reshares"]].notna().all(axis=1).any():
        subset2 = df[["likes", "reshares"]].dropna()
        if not subset2.empty:
            plt.figure()
            plt.scatter(subset2["likes"], subset2["reshares"])
            plt.title("Likes vs Reshares")
            plt.xlabel("Likes")
            plt.ylabel("Reshares")
            plt.tight_layout()
            path26 = output_dir / "likes_vs_reshares.png"
            plt.savefig(path26)
            plt.close()
            generated.append(path26)
    # 27. Top tickers by average reshares
    if "symbols" in df.columns and df["reshares"].notna().any():
        ticker_res: Dict[str, List[int]] = {}
        for _, row in df.iterrows():
            syms = row.get("symbols")
            if isinstance(syms, list) and len(syms) > 0 and pd.notna(row["reshares"]):
                for sym in syms:
                    ticker_res.setdefault(sym, []).append(int(row["reshares"]))
        if ticker_res:
            avg_reshares = {t: (sum(vals) / len(vals)) for t, vals in ticker_res.items() if vals}
            top_res = sorted(avg_reshares.items(), key=lambda kv: kv[1], reverse=True)[:10]
            if top_res:
                labels_res, avgs_res = zip(*top_res)
                plt.figure()
                plt.bar(labels_res, avgs_res)
                plt.title("Top Tickers by Average Reshares")
                plt.xlabel("Ticker")
                plt.ylabel("Average Reshares")
                plt.tight_layout()
                path27 = output_dir / "top_tickers_avg_reshares.png"
                plt.savefig(path27)
                plt.close()
                generated.append(path27)

    return generated
