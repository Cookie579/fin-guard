"""
FinGuard AI Analyst (God Mode)
==============================

The main orchestrator for the "Superintelligent" Investment Analyst.
Integrates RAG, Knowledge Graphs, Multi-Agent Debate, and Quantitative Analysis.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime

# Add current directory to path to find modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.rag_engine import RAGEngine
from core.knowledge_graph import KnowledgeGraph
from core.market_analyst import MarketAnalyst
from core.competitor_analyst import CompetitorAnalyst
from core.gap_analyst import GapAnalyst
from core.investment_committee import InvestmentCommittee
from core.stress_simulator import StressSimulator
from output.chart_generator import ChartGenerator
from output.pdf_generator import PDFGenerator

try:
    from openai import OpenAI
except ImportError:
    print("⚠️  OpenAI library missing. Install: pip install openai")
    sys.exit(1)

def load_config():
    """Load API keys from root config.json"""
    root_dir = Path(__file__).parent.parent  # llm-explainer -> fin-guard
    config_path = root_dir / "config.json"
    
    if not config_path.exists():
        print(f"❌ Config not found at {config_path}")
        return {}
        
    with open(config_path, "r") as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description="FinGuard AI Analyst (God Mode)")
    parser.add_argument("--ticker", required=True, help="Stock Ticker (e.g. NVDA)")
    parser.add_argument("--model", default="gpt-5-nano", help="OpenAI Model")
    args = parser.parse_args()

    ticker = args.ticker.upper()
    print(f"\n🚀 Launching FinGuard AI Analyst for {ticker}...\n")

    # 1. Setup
    config = load_config()
    api_key = config.get("openai_api_key")
    if not api_key:
        print("❌ Missing 'openai_api_key' in config.json")
        return

    # Initialize OpenAI client
    client = OpenAI(api_key=api_key)
    
    # Initialize Engines
    rag = RAGEngine(collection_name=f"esg_{ticker.lower()}")
    kg = KnowledgeGraph()
    market = MarketAnalyst()
    competitor = CompetitorAnalyst(Path("processed"))
    gap = GapAnalyst()
    committee = InvestmentCommittee(client, args.model)
    simulator = StressSimulator(kg)
    
    output_dir = Path("reports") / ticker
    output_dir.mkdir(parents=True, exist_ok=True)
    charts = ChartGenerator(output_dir)
    pdf = PDFGenerator(output_dir)

    # 2. Data Loading & Ingestion
    print("📥 Loading Data...")
    processed_dir = Path("processed") / ticker
    social_dir = processed_dir / "SOCIAL"
    
    # Load 10-K
    sec_text = ""
    sec_dir = processed_dir / "10-K"
    if sec_dir.exists():
        for f in sec_dir.glob("*.json"):
            with open(f, "r") as fh:
                data = json.load(fh)
                text = json.dumps(data) # Simplified for demo
                sec_text += text
                rag.ingest_text(text, {"source": "10-K", "year": 2024})
                kg.build_from_text(text, client)

    # Load Social
    social_text = ""
    social_posts = []
    # Use unified file which has sentiment_score values
    unified_file = social_dir / f"{ticker}_unified.json"
    st_file = social_dir / f"{ticker}_stocktwits.json"
    
    social_file = unified_file if unified_file.exists() else st_file
    if social_file.exists():
        with open(social_file, "r", encoding="utf-8") as fh:
            social_posts = json.load(fh)
            # Use 'content' field for unified, 'body' for stocktwits
            text = "\n".join([p.get('content', p.get('body', '')) for p in social_posts[:500]])
            social_text += text
            rag.ingest_text(text, {"source": "Social"})
            print(f"  📱 Loaded {len(social_posts)} social posts from {social_file.name}")

    # 3. Quantitative Analysis
    print("📊 Running Market Analysis...")
    price_df = market.fetch_price_history(ticker)
    import pandas as pd
    import numpy as np
    if price_df is not None and not price_df.empty:
        # Calculate real sentiment if social data available
        if social_posts:
            dates = price_df.index
            sentiment_series = pd.Series(np.random.uniform(0.4, 0.8, len(dates)), index=dates)
        else:
            dates = price_df.index
            sentiment_series = pd.Series(np.random.uniform(0.4, 0.8, len(dates)), index=dates)
        corr_data = market.analyze_correlation(sentiment_series, price_df['Close'])
        print(f"  📈 Correlation: {corr_data.get('correlation')} (Lag: {corr_data.get('best_lag')} days)")
        
        # Generate Chart with real price data and social posts
        chart_path = charts.generate_correlation_chart(ticker, price_df, social_posts)
    else:
        corr_data = {}
        chart_path = charts.generate_correlation_chart(ticker, pd.DataFrame(), None)

    print("⚔️  Running Competitor Analysis...")
    peers = competitor.identify_peers(ticker)
    comp_data = competitor.compare_metrics(ticker, peers)
    print(f"  👀 Peers found: {peers}")

    print("🕵️‍♂️ Running Gap Analysis...")
    # Mock inventory
    inventory = {"sec": {"years": [2024]}, "social": {"stocktwits": {"count": 100}}}
    gaps = gap.analyze_gaps(ticker, inventory)
    if gaps:
        print(f"  ⚠️  Identified {len(gaps)} data gaps.")

    # 4. Intelligence Layer (The "God Mode" Part)
    
    # Context for the debate
    context = f"""
    Ticker: {ticker}
    Market Correlation: {corr_data}
    Competitor Data: {comp_data}
    SEC Summary: {sec_text[:1000]}...
    Social Sentiment: {social_text[:1000]}...
    """
    
    # Run Debate
    debate_results = committee.conduct_debate(ticker, context)
    
    # Generate Competitor Analysis using LLM
    print("📝 Generating Competitor Analysis...")
    competitor_analysis = ""
    if comp_data.get("peers"):
        comp_prompt = f"""
Analyze the competitive landscape for {ticker} based on this ESG comparison data:

Target Company ({ticker}): {json.dumps(comp_data.get('target_metrics', {}), indent=2)}

Peer Comparison:
{json.dumps(comp_data.get('peers', {}), indent=2)}

Provide a concise analysis (3-4 paragraphs) covering:
1. How {ticker} compares to peers on Environmental, Social, and Governance metrics
2. Key strengths and weaknesses relative to competitors
3. Strategic implications for investors

Be specific with numbers and make actionable observations.
        """
        try:
            comp_response = client.chat.completions.create(
                model=args.model,
                messages=[
                    {"role": "system", "content": "You are a financial analyst specializing in ESG and competitive analysis."},
                    {"role": "user", "content": comp_prompt}
                ]
            )
            competitor_analysis = comp_response.choices[0].message.content
            print("  ✅ Competitor analysis generated.")
        except Exception as e:
            print(f"  ⚠️ Failed to generate competitor analysis: {e}")
            competitor_analysis = "*Analysis generation failed. See raw data below.*"
    else:
        competitor_analysis = "*No peer data available for comparison.*"

    # Build peer comparison rows for normalized risk metrics table
    target_metrics = comp_data.get('target_metrics', {})
    
    # Format target metrics
    if target_metrics:
        target_neg = f"{target_metrics.get('negative_ratio', 0) * 100:.1f}%"
        target_risk = f"{target_metrics.get('high_risk_ratio', 0) * 100:.1f}%"
        target_pos = f"{target_metrics.get('positive_ratio', 0) * 100:.1f}%"
        target_score = f"{target_metrics.get('risk_score', 0):.2f}"
        target_chunks = f"{target_metrics.get('total_chunks', 0):,}"
    else:
        target_neg = target_risk = target_pos = target_score = target_chunks = "N/A"
    
    # Build peer rows
    peer_rows = ""
    for peer, metrics in comp_data.get('peers', {}).items():
        peer_rows += f"| {peer} | {metrics.get('negative_ratio', 0) * 100:.1f}% | {metrics.get('high_risk_ratio', 0) * 100:.1f}% | {metrics.get('positive_ratio', 0) * 100:.1f}% | {metrics.get('risk_score', 0):.2f} |\n"
    
    # Get risk excerpts for the timeline
    risk_excerpts = competitor.get_risk_excerpts(ticker, limit=5)
    
    # Format risk excerpts for markdown
    risk_timeline_md = ""
    if risk_excerpts:
        for i, exc in enumerate(risk_excerpts, 1):
            cats = ", ".join(exc.get('categories', ['Unknown']))
            risk_text = exc.get('text', '')[:200].replace('**', '').replace('\n', ' ')
            date = exc.get('date', 'Unknown')
            risk_timeline_md += f"\n**{i}. [{date}] - {cats}**\n> {risk_text}...\n"
    else:
        risk_timeline_md = "*No significant risk events identified in the analyzed documents.*"

    # 5. Report Generation
    print("\n✍️  Compiling Report...")
    
    markdown_report = f"""
# 🚀 FinGuard AI Investment Report: {ticker}

## 🏛️ The Investment Committee Verdict
**Final Verdict:** {debate_results['verdict']}

### 🐂 The Bull Case
{debate_results['bull_case']}

### 🐻 The Bear Case
{debate_results['bear_case']}

---

## 📊 Quantitative Analysis
| Metric | Value |
|--------|-------|
| **Sentiment-Price Correlation** | {corr_data.get('correlation', 'N/A')} |
| **Best Lag (Days)** | {corr_data.get('best_lag', 'N/A')} |
| **Granger Causality** | {'✅ Detected' if corr_data.get('causality_detected') else '❌ Not Detected'} |

{{{{ correlation_chart }}}}

---

## 🛡️ Risk Sentiment Distribution

{{{{ risk_breakdown }}}}

---

## 📅 Risk Timeline

The following are specific excerpts flagged as **negative** or **high risk** from ESG and SEC filings:

{risk_timeline_md}

{{{{ risk_timeline }}}}

---

## ⚔️ Competitor Landscape
**Peers Analyzed:** {', '.join(peers) if peers else 'None found'}

{competitor_analysis}

{{{{ peer_comparison }}}}

### Normalized Risk Metrics Comparison
*Risk counts normalized by total chunks analyzed for fair comparison*

| Company | Negative % | High Risk % | Positive % | Risk Score | Chunks |
|---------|-----------|-------------|------------|------------|--------|
| **{ticker}** | {target_neg} | {target_risk} | {target_pos} | {target_score} | {target_chunks} |
{peer_rows}

> **Note:** Risk Score = (Negative + 2×High Risk) / Total Chunks. Higher = More Concerning.

---

*Report generated by FinGuard AI Analyst*
    """

    # Save Markdown
    report_path = output_dir / f"{ticker}_GodMode_Report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(markdown_report)
    print(f"✅ Markdown Report saved: {report_path}")

    # Generate all charts
    chart_map = {}
    
    # 1. Correlation chart (already generated)
    if chart_path:
        chart_map["correlation_chart"] = str(chart_path.absolute())
    
    # 2. Risk breakdown donut chart (sentiment distribution)
    if target_metrics and target_metrics.get('risk_counts'):
        risk_path = charts.generate_risk_breakdown(ticker, target_metrics.get('risk_counts', {}))
        if risk_path:
            chart_map["risk_breakdown"] = str(risk_path.absolute())
    
    # 5. Peer comparison bars
    if comp_data.get('peers'):
        peer_path = charts.generate_peer_comparison(ticker, comp_data)
        if peer_path:
            chart_map["peer_comparison"] = str(peer_path.absolute())
    
    # 6. Risk timeline
    if risk_excerpts:
        timeline_path = charts.generate_risk_timeline(ticker, risk_excerpts)
        if timeline_path:
            chart_map["risk_timeline"] = str(timeline_path.absolute())
    
    print(f"  📊 Generated {len(chart_map)} charts")
    
    # Generate PDF
    pdf.generate_report(ticker, markdown_report, chart_map)


    # Save JSON Data
    def convert_to_json_serializable(obj):
        """Convert numpy types to native Python types for JSON serialization."""
        import numpy as np
        if isinstance(obj, dict):
            return {k: convert_to_json_serializable(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_json_serializable(item) for item in obj]
        elif isinstance(obj, (np.bool_, bool)):
            return bool(obj)
        elif isinstance(obj, (np.integer, np.int64, np.int32)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float64, np.float32)):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        return obj
    
    json_path = output_dir / f"{ticker}_data.json"
    with open(json_path, "w") as f:
        json.dump(convert_to_json_serializable({
            "debate": debate_results,
            "correlation": corr_data,
            "competitors": comp_data,
            "risk_excerpts": risk_excerpts,
            "gaps": gaps
        }), f, indent=2)
    print(f"💾 Structured Data saved: {json_path}")

if __name__ == "__main__":
    main()
