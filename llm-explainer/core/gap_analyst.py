"""
Gap Analyst (The Meta-Cognition)
================================

Identifies missing data and generates structured requests.
"""

from typing import Dict, List, Any
from datetime import datetime

class GapAnalyst:
    def __init__(self):
        pass

    def analyze_gaps(self, ticker: str, data_inventory: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Scan inventory for missing years or sources.
        data_inventory example:
        {
            "sec": {"years": [2023, 2024]},
            "social": {"stocktwits": {"count": 500}, "youtube": {"count": 0}}
        }
        """
        requests = []
        current_year = datetime.now().year
        
        # Check SEC Recency
        sec_years = data_inventory.get("sec", {}).get("years", [])
        if current_year not in sec_years and (current_year - 1) not in sec_years:
            requests.append({
                "type": "SEC",
                "ticker": ticker,
                "year": current_year,
                "reason": "Missing recent 10-K"
            })

        # Check Social Parity
        social = data_inventory.get("social", {})
        st_count = social.get("stocktwits", {}).get("count", 0)
        yt_count = social.get("youtube", {}).get("count", 0)
        
        if st_count > 0 and yt_count == 0:
            requests.append({
                "type": "SOCIAL",
                "source": "YOUTUBE",
                "ticker": ticker,
                "reason": "Imbalanced social data (Have StockTwits, missing YouTube)"
            })
            
        if yt_count > 0 and st_count == 0:
            requests.append({
                "type": "SOCIAL",
                "source": "STOCKTWITS",
                "ticker": ticker,
                "reason": "Imbalanced social data (Have YouTube, missing StockTwits)"
            })

        return requests
