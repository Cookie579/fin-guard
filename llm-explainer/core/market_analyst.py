"""
Market Analyst (The Quant)
==========================

Analyzes stock price history and correlates it with sentiment data.
Uses Granger Causality to test if sentiment predicts price.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta

try:
    import yfinance as yf
    from scipy.stats import pearsonr
    from statsmodels.tsa.stattools import grangercausalitytests
except ImportError:
    print("⚠️  Market Analyst dependencies missing. Install: yfinance scipy statsmodels")
    yf = None

class MarketAnalyst:
    def __init__(self):
        pass

    def fetch_price_history(self, ticker: str, days: int = 1825) -> pd.DataFrame:
        """Fetch daily OHLC data. Default 5 years to match social sentiment data."""
        if not yf:
            return pd.DataFrame()
        
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        try:
            df = yf.download(ticker, start=start_date, end=end_date, progress=False)
            if df.empty:
                return pd.DataFrame()
            return df
        except Exception as e:
            print(f"⚠️  Failed to fetch price for {ticker}: {e}")
            return pd.DataFrame()

    def analyze_correlation(self, sentiment_series: pd.Series, price_series: pd.Series) -> Dict[str, Any]:
        """
        Calculate Pearson correlation and Granger Causality.
        sentiment_series: Daily sentiment score (index=Date)
        price_series: Daily closing price (index=Date)
        """
        if sentiment_series.empty or price_series.empty:
            return {"correlation": 0.0, "lag": 0, "granger_p_value": 1.0}

        # Align data
        df = pd.concat([sentiment_series, price_series], axis=1).dropna()
        df.columns = ['sentiment', 'price']
        
        if len(df) < 10:
            return {"correlation": 0.0, "msg": "Insufficient data"}

        # Pearson Correlation
        corr, _ = pearsonr(df['sentiment'], df['price'])
        
        # Lag Analysis (Cross-Correlation)
        # Find lag where correlation is highest
        lags = range(-5, 6) # -5 to +5 days
        best_lag = 0
        max_corr = 0
        
        for lag in lags:
            shifted_sent = df['sentiment'].shift(lag)
            valid = pd.concat([shifted_sent, df['price']], axis=1).dropna()
            if len(valid) > 5:
                c, _ = pearsonr(valid.iloc[:, 0], valid.iloc[:, 1])
                if abs(c) > abs(max_corr):
                    max_corr = c
                    best_lag = lag

        # Granger Causality (Does Sentiment cause Price?)
        # We check if sentiment (x) causes price (y)
        granger_p = 1.0
        try:
            # maxlag=3 days
            gc_res = grangercausalitytests(df[['price', 'sentiment']], maxlag=[3], verbose=False)
            # Get p-value for lag 1 (index 1, tuple index 0, dict key 'ssr_ftest', index 1)
            granger_p = gc_res[3][0]['ssr_ftest'][1]
        except Exception:
            pass

        return {
            "correlation": round(corr, 3),
            "best_lag": best_lag,
            "max_lag_correlation": round(max_corr, 3),
            "granger_p_value": round(granger_p, 4),
            "causality_detected": granger_p < 0.05
        }

    def detect_divergence(self, sentiment_series: pd.Series, price_series: pd.Series) -> List[Dict]:
        """Identify dates where Price and Sentiment moved in opposite directions."""
        # Implementation placeholder
        return []
