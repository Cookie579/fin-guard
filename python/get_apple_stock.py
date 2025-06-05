#!/usr/bin/env python3
"""
get_apple_stock.py

Provides Apple's (AAPL) regular and post-market price as:
- CLI output (if run directly)
- FastAPI HTTP API (if run as a web service)
"""

import yfinance as yf
import sys
from fastapi import FastAPI
from fastapi.responses import JSONResponse

app = FastAPI()

def fetch_stock_data():
    ticker = "AAPL"
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        return {
            "symbol": ticker,
            "regular_market_price": info.get("regularMarketPrice"),
            "post_market_price": info.get("postMarketPrice")
        }

    except Exception as e:
        return {
            "error": f"Failed to fetch stock data: {str(e)}"
        }

@app.get("/apple-stock")
def get_apple_stock():
    data = fetch_stock_data()
    return JSONResponse(content=data)

@app.get("/health")
def health():
    return {"status": "ok"}

def main():
    data = fetch_stock_data()

    if "error" in data:
        print(data["error"], file=sys.stderr)
        sys.exit(1)

    print("Apple (AAPL) Stock Prices:")
    if data["regular_market_price"] is not None:
        print(f" • Regular Market Price:  ${data['regular_market_price']:.2f}")
    else:
        print(" • Regular Market Price:  N/A")

    if data["post_market_price"] is not None:
        print(f" • Post Market Price:     ${data['post_market_price']:.2f}")
    else:
        print(" • Post Market Price:     N/A (market closed or data unavailable)")

if __name__ == "__main__":
    main()
