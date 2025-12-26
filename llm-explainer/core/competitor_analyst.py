"""
Competitor Analyst (The Strategist)
===================================

Identifies peers and compares metrics against local data.
Uses normalized risk counts for meaningful ESG comparison.
"""

from pathlib import Path
from typing import List, Dict, Any
import json

class CompetitorAnalyst:
    def __init__(self, data_dir: Path):
        self.data_dir = data_dir

    def identify_peers(self, ticker: str) -> List[str]:
        """
        Identify potential peers based on local directory existence.
        In a real app, we'd ask an LLM "Who are NVDA's competitors?" first.
        Here, we just scan processed/ folder.
        """
        all_tickers = [d.name for d in self.data_dir.iterdir() if d.is_dir()]
        # Simple heuristic: Just return other tickers found locally
        peers = [t for t in all_tickers if t != ticker]
        return peers[:3] # Top 3

    def compare_metrics(self, target_ticker: str, peer_tickers: List[str]) -> Dict[str, Any]:
        """Compare ESG risk metrics and sentiment across companies."""
        comparison = {
            "target": target_ticker,
            "peers": {}
        }
        
        target_metrics = self._load_metrics(target_ticker)
        if not target_metrics:
            return comparison

        comparison["target_metrics"] = target_metrics

        for peer in peer_tickers:
            metrics = self._load_metrics(peer)
            if metrics:
                comparison["peers"][peer] = metrics
                
        return comparison

    def _load_metrics(self, ticker: str) -> Dict[str, Any]:
        """
        Load aggregated ESG/risk metrics from disk.
        Uses normalized risk counts (per chunk) for fair comparison.
        Falls back to SEC 10-K data if ESG folder doesn't exist.
        """
        esg_dir = self.data_dir / ticker / "ESG"
        sec_dir = self.data_dir / ticker / "10-K"
        
        # Check which data source to use
        use_sec_fallback = False
        if not esg_dir.exists():
            if sec_dir.exists():
                esg_dir = sec_dir  # Use SEC data as fallback
                use_sec_fallback = True
            else:
                return {}
            
        # Aggregate metrics across ALL ESG files for this ticker
        total_chunks = 0
        total_risk_counts = {"positive": 0, "neutral": 0, "negative": 0, "high risk": 0}
        total_esg_counts = {"Environmental": 0, "Social": 0, "Governance": 0}
        high_risk_excerpts = []  # Store some examples
        
        files = list(esg_dir.glob("*.json"))
        if not files:
            return {}
        
        for f in files:
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                    
                    agg = data.get("aggregated_metrics", {})
                    num_chunks = agg.get("num_chunks", 0)
                    
                    if num_chunks == 0:
                        continue
                    
                    total_chunks += num_chunks
                    
                    # Sum up risk counts
                    for label, count in agg.get("risk_counts", {}).items():
                        if label in total_risk_counts:
                            total_risk_counts[label] += count
                    
                    # Sum up ESG category counts
                    for cat, count in agg.get("esg_counts", {}).items():
                        if cat in total_esg_counts:
                            total_esg_counts[cat] += count
                    
                    # Extract high-risk excerpts (up to 5 total)
                    if len(high_risk_excerpts) < 5:
                        for result in data.get("esg_results", [])[:10]:
                            risk_scores = result.get("risk_scores", {})
                            if "high risk" in risk_scores or "negative" in risk_scores:
                                text = result.get("text", "")[:250]
                                if text and len(high_risk_excerpts) < 5:
                                    high_risk_excerpts.append({
                                        "text": text,
                                        "categories": result.get("esg_categories", []),
                                        "risk_scores": risk_scores,
                                        "filing_date": data.get("filing_date", "")
                                    })
            except Exception:
                continue
        
        if total_chunks == 0:
            return {}
        
        # Calculate normalized ratios (per chunk averages)
        return {
            "total_chunks": total_chunks,
            "files_analyzed": len(files),
            # Normalized risk metrics (0-1 scale based on chunk count)
            "negative_ratio": round(total_risk_counts["negative"] / total_chunks, 3),
            "high_risk_ratio": round(total_risk_counts["high risk"] / total_chunks, 3),
            "positive_ratio": round(total_risk_counts["positive"] / total_chunks, 3),
            "neutral_ratio": round(total_risk_counts["neutral"] / total_chunks, 3),
            # Combined risk score (higher = more concerning)
            "risk_score": round((total_risk_counts["negative"] + total_risk_counts["high risk"] * 2) / total_chunks, 3),
            # ESG coverage (what categories are discussed most)
            "esg_focus": {
                "Environmental": round(total_esg_counts["Environmental"] / total_chunks, 3),
                "Social": round(total_esg_counts["Social"] / total_chunks, 3),
                "Governance": round(total_esg_counts["Governance"] / total_chunks, 3),
            },
            # Raw counts for reference
            "risk_counts": total_risk_counts,
            "esg_counts": total_esg_counts,
            # Sample high-risk excerpts for the report
            "high_risk_excerpts": high_risk_excerpts
        }
    
    def get_risk_excerpts(self, ticker: str, limit: int = 10) -> List[Dict]:
        """
        Get specific high-risk and negative sentiment excerpts with dates.
        Useful for the Risk Timeline section.
        Falls back to SEC 10-K data if ESG folder doesn't exist.
        """
        esg_dir = self.data_dir / ticker / "ESG"
        sec_dir = self.data_dir / ticker / "10-K"
        
        # Check which data source to use
        if not esg_dir.exists():
            if sec_dir.exists():
                esg_dir = sec_dir
            else:
                return []
        
        excerpts = []
        files = sorted(esg_dir.glob("*.json"), key=lambda x: x.name, reverse=True)  # Newest first
        
        # Date threshold: only include data from last 6 months
        from datetime import datetime, timedelta
        six_months_ago = datetime.now() - timedelta(days=180)
        
        for f in files:
            try:
                with open(f, "r", encoding="utf-8") as fh:
                    data = json.load(fh)
                    filing_date = data.get("filing_date", f.stem.split("__")[1] if "__" in f.stem else "Unknown")
                    
                    # Parse the date to filter old data
                    try:
                        date_part = filing_date.split("_")[0] if "_" in filing_date else filing_date
                        parsed_date = datetime.strptime(date_part, "%Y-%m-%d")
                        if parsed_date < six_months_ago:
                            continue  # Skip old data
                    except:
                        # If can't parse, check if year is old
                        import re
                        year_match = re.search(r'(\d{4})', filing_date)
                        if year_match and int(year_match.group(1)) < datetime.now().year - 1:
                            continue  # Skip data older than last year
                    
                    for result in data.get("esg_results", []):
                        risk_scores = result.get("risk_scores", {})
                        
                        # Only include negative or high-risk items
                        if "high risk" in risk_scores or "negative" in risk_scores:
                            text = result.get("text", "")
                            # Clean up markdown highlight markers
                            text = text.replace("**", "")[:300]
                            
                            if text:
                                excerpts.append({
                                    "date": filing_date,
                                    "text": text,
                                    "categories": result.get("esg_categories", []),
                                    "risk_scores": risk_scores
                                })
                                
                                if len(excerpts) >= limit:
                                    return excerpts
            except Exception:
                continue
        
        return excerpts
