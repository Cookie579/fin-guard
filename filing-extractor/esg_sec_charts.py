#!/usr/bin/env python3
"""
ESG and SEC Charts Generator
============================

This module provides helper functions to visualise aggregated ESG and SEC
analysis results for a company.  The FinGuard pipeline produces
``esg_metrics`` and ``sec_metrics`` objects per report or filing, each of
which includes counts for risk categories (positive, neutral, negative,
high risk) and ESG categories (Environmental, Social, Governance).

The chart generators take a list of these metrics (one per ESG report
or SEC filing) and produce bar charts to illustrate the distribution of
risk categories and ESG categories.  The output directory will be
created if it does not exist.

Example usage::

    from pathlib import Path
    from esg_sec_charts import generate_esg_charts, generate_sec_charts

    generate_esg_charts(list_of_esg_metrics, Path("charts/TSLA/ESG"))
    generate_sec_charts(list_of_sec_metrics, Path("charts/TSLA/SEC"))

Missing dependencies (pandas, matplotlib) will cause an ImportError.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Dict, Any


def _aggregate_metrics(metrics_list: List[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """Aggregate risk and ESG counts across multiple metrics objects.

    Parameters
    ----------
    metrics_list : list of dict
        Each element is a metrics dict (as returned by analyse_esg or
        analyse_sec) containing ``risk_counts`` and ``esg_counts``.

    Returns
    -------
    dict
        A mapping with keys ``risk_counts`` and ``esg_counts``, each of
        which maps category names to aggregated counts.
    """
    agg_risk: Dict[str, float] = {}
    agg_esg: Dict[str, float] = {}
    for m in metrics_list:
        rc = m.get("risk_counts") or {}
        ec = m.get("esg_counts") or {}
        # Sum risk counts
        for k, v in rc.items():
            if v is None:
                continue
            agg_risk[k] = agg_risk.get(k, 0.0) + float(v)
        # Sum ESG counts
        for k, v in ec.items():
            if v is None:
                continue
            agg_esg[k] = agg_esg.get(k, 0.0) + float(v)
    return {"risk_counts": agg_risk, "esg_counts": agg_esg}


def generate_esg_charts(metrics_list: List[Dict[str, Any]], output_dir: Path) -> List[Path]:
    """Generate ESG charts from a list of ESG metrics dicts.

    This function aggregates the ``risk_counts`` and ``esg_counts`` across
    all provided metrics and produces bar charts for each.  The charts
    are saved in ``output_dir``, which will be created if necessary.

    Parameters
    ----------
    metrics_list : list of dict
        A list of ESG metrics objects (each containing ``risk_counts`` and
        ``esg_counts`` as returned by analyse_esg).
    output_dir : Path
        Directory where the generated charts will be written.

    Returns
    -------
    list of Path
        Paths to the generated image files.
    """
    try:
        import pandas as pd  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:
        raise ImportError("pandas and matplotlib are required to generate charts") from exc
    output_dir.mkdir(parents=True, exist_ok=True)
    # Aggregate counts
    agg = _aggregate_metrics(metrics_list)
    risk_counts = agg["risk_counts"]
    esg_counts = agg["esg_counts"]
    generated: List[Path] = []
    # Risk categories bar chart
    if risk_counts:
        rc_series = pd.Series(risk_counts).reindex([
            "positive",
            "neutral",
            "negative",
            "high risk",
        ], fill_value=0)
        plt.figure()
        rc_series.plot(kind="bar")
        plt.title("ESG Risk Counts")
        plt.xlabel("Risk Category")
        plt.ylabel("Count")
        plt.tight_layout()
        out = output_dir / "esg_risk_counts.png"
        plt.savefig(out)
        plt.close()
        generated.append(out)
    # ESG categories bar chart
    if esg_counts:
        esg_series = pd.Series(esg_counts).reindex([
            "Environmental",
            "Social",
            "Governance",
        ], fill_value=0)
        plt.figure()
        esg_series.plot(kind="bar")
        plt.title("ESG Category Counts")
        plt.xlabel("ESG Category")
        plt.ylabel("Count")
        plt.tight_layout()
        out = output_dir / "esg_category_counts.png"
        plt.savefig(out)
        plt.close()
        generated.append(out)
    return generated


def generate_sec_charts(metrics_list: List[Dict[str, Any]], output_dir: Path) -> List[Path]:
    """Generate SEC charts from a list of SEC metrics dicts.

    This function aggregates the ``risk_counts`` and ``esg_counts`` across
    all provided metrics and produces bar charts for each.  The charts
    are saved in ``output_dir``.

    Parameters
    ----------
    metrics_list : list of dict
        A list of SEC metrics objects (each containing ``risk_counts`` and
        ``esg_counts`` as returned by analyse_sec).
    output_dir : Path
        Directory where the generated charts will be written.

    Returns
    -------
    list of Path
        Paths to the generated image files.
    """
    # Reuse the ESG generator for SEC metrics since the structure is identical
    return generate_esg_charts(metrics_list, output_dir)

# -----------------------------------------------------------------------------
# Time-based ESG/SEC charts
#
# These helpers create charts that show how the counts of risk and ESG
# categories vary over time (e.g. by reporting or filing date).  They accept
# a list of pipeline result rows, each containing a ``sec_filing_date`` or
# ``esg_report_date`` and a corresponding metrics object.  The charts are
# saved in ``output_dir`` and include: (1) risk counts by date, (2) ESG
# counts by date.

def generate_esg_time_charts(rows: List[Dict[str, Any]], output_dir: Path) -> List[Path]:
    """Generate time-based ESG charts from pipeline rows.

    This function reads the ESG metrics from each row and aggregates the
    counts by the ``esg_report_date``.  For each date it builds two bar
    charts: one for risk counts (positive, neutral, negative, high risk)
    and one for ESG counts (Environmental, Social, Governance).  The
    resulting images are written to the specified output directory.

    Parameters
    ----------
    rows : list of dict
        Rows returned by ``pipeline.process_all()``.  Each row must
        contain an ``esg_report_date`` and ``esg_metrics``.
    output_dir : Path
        Directory where the generated charts will be written.

    Returns
    -------
    list of Path
        Paths to the generated image files.
    """
    try:
        import pandas as pd  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:
        raise ImportError("pandas and matplotlib are required to generate charts") from exc
    output_dir.mkdir(parents=True, exist_ok=True)
    # Build DataFrame: one row per ESG report
    data_rows = []
    for row in rows:
        date = row.get("esg_report_date")
        metrics = row.get("esg_metrics")
        if date and metrics:
            rc = metrics.get("risk_counts") or {}
            ec = metrics.get("esg_counts") or {}
            data_rows.append({
                "date": date,
                "positive": rc.get("positive", 0),
                "neutral": rc.get("neutral", 0),
                "negative": rc.get("negative", 0),
                "high risk": rc.get("high risk", 0),
                "Environmental": ec.get("Environmental", 0),
                "Social": ec.get("Social", 0),
                "Governance": ec.get("Governance", 0),
            })
    if not data_rows:
        return []
    df = pd.DataFrame(data_rows)
    # Sort by date
    try:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
    except Exception:
        pass
    # Plot risk counts by date
    fig1, ax1 = plt.subplots()
    df.set_index("date")[["positive", "neutral", "negative", "high risk"]].plot(kind="bar", stacked=True, ax=ax1)
    ax1.set_title("ESG Risk Counts by Report Date")
    ax1.set_xlabel("Report Date")
    ax1.set_ylabel("Count")
    ax1.legend(title="Risk Category")
    fig1.autofmt_xdate()
    fig1.tight_layout()
    out1 = output_dir / "esg_risk_counts_over_time.png"
    fig1.savefig(out1)
    plt.close(fig1)
    # Plot ESG counts by date
    fig2, ax2 = plt.subplots()
    df.set_index("date")[["Environmental", "Social", "Governance"]].plot(kind="bar", stacked=True, ax=ax2)
    ax2.set_title("ESG Category Counts by Report Date")
    ax2.set_xlabel("Report Date")
    ax2.set_ylabel("Count")
    ax2.legend(title="ESG Category")
    fig2.autofmt_xdate()
    fig2.tight_layout()
    out2 = output_dir / "esg_category_counts_over_time.png"
    fig2.savefig(out2)
    plt.close(fig2)
    return [out1, out2]


def generate_sec_time_charts(rows: List[Dict[str, Any]], output_dir: Path) -> List[Path]:
    """Generate time-based SEC charts from pipeline rows.

    This function reads the SEC metrics from each row and aggregates the
    counts by the ``sec_filing_date``.  For each filing date it builds two
    bar charts: one for risk counts (positive, neutral, negative, high
    risk) and one for ESG counts (Environmental, Social, Governance).  The
    resulting images are written to the specified output directory.

    Parameters
    ----------
    rows : list of dict
        Rows returned by ``pipeline.process_all()``.  Each row must
        contain a ``sec_filing_date`` and ``sec_metrics``.
    output_dir : Path
        Directory where the generated charts will be written.

    Returns
    -------
    list of Path
        Paths to the generated image files.
    """
    try:
        import pandas as pd  # type: ignore
        import matplotlib.pyplot as plt  # type: ignore
    except Exception as exc:
        raise ImportError("pandas and matplotlib are required to generate charts") from exc
    output_dir.mkdir(parents=True, exist_ok=True)
    data_rows = []
    for row in rows:
        date = row.get("sec_filing_date")
        metrics = row.get("sec_metrics")
        if date and metrics:
            rc = metrics.get("risk_counts") or {}
            ec = metrics.get("esg_counts") or {}
            data_rows.append({
                "date": date,
                "positive": rc.get("positive", 0),
                "neutral": rc.get("neutral", 0),
                "negative": rc.get("negative", 0),
                "high risk": rc.get("high risk", 0),
                "Environmental": ec.get("Environmental", 0),
                "Social": ec.get("Social", 0),
                "Governance": ec.get("Governance", 0),
            })
    if not data_rows:
        return []
    df = pd.DataFrame(data_rows)
    try:
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
    except Exception:
        pass
    fig1, ax1 = plt.subplots()
    df.set_index("date")[["positive", "neutral", "negative", "high risk"]].plot(kind="bar", stacked=True, ax=ax1)
    ax1.set_title("SEC Risk Counts by Filing Date")
    ax1.set_xlabel("Filing Date")
    ax1.set_ylabel("Count")
    ax1.legend(title="Risk Category")
    fig1.autofmt_xdate()
    fig1.tight_layout()
    out1 = output_dir / "sec_risk_counts_over_time.png"
    fig1.savefig(out1)
    plt.close(fig1)
    fig2, ax2 = plt.subplots()
    df.set_index("date")[["Environmental", "Social", "Governance"]].plot(kind="bar", stacked=True, ax=ax2)
    ax2.set_title("SEC ESG Category Counts by Filing Date")
    ax2.set_xlabel("Filing Date")
    ax2.set_ylabel("Count")
    ax2.legend(title="ESG Category")
    fig2.autofmt_xdate()
    fig2.tight_layout()
    out2 = output_dir / "sec_category_counts_over_time.png"
    fig2.savefig(out2)
    plt.close(fig2)
    return [out1, out2]
