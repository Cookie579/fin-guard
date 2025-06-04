#!/usr/bin/env python3
"""
get_apple_stock.py

Fetches and prints Apple's (AAPL) regular market price and post market price
(using yfinance). Run after-hours as well to see post-market values.
"""

import yfinance as yf
import sys

def main():
    ticker = "AAPL"
    try:
        stock = yf.Ticker(ticker)
        info = stock.info

        # Regular market price
        reg_price = info.get("regularMarketPrice", None)
        # Post market (after-hours) price
        post_price = info.get("postMarketPrice", None)

        print(f"Apple (AAPL) Stock Prices:")
        if reg_price is not None:
            print(f" • Regular Market Price:  ${reg_price:.2f}")
        else:
            print(" • Regular Market Price:  N/A")

        if post_price is not None:
            print(f" • Post Market Price:     ${post_price:.2f}")
        else:
            print(" • Post Market Price:     N/A (market closed or data unavailable)")

    except Exception as e:
        print(f"Error fetching data for {ticker}: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
