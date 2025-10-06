package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// secTickerEntry represents a single record in the SEC company tickers JSON
// document. The JSON served at https://www.sec.gov/files/company_tickers.json
// maps numeric keys to objects with these fields. The cik_str field holds
// the CIK as an integer, the ticker is the stock symbol, and title is
// the full company name.
type secTickerEntry struct {
	CIKStr int    `json:"cik_str"`
	Ticker string `json:"ticker"`
	Title  string `json:"title"`
}

// FetchCIKFromRemote downloads the SEC company tickers file and searches
// for the provided ticker symbol. If found, it returns the CIK as a
// zero‑padded 10‑digit string along with the company title. If the ticker
// is not present or an error occurs, a non‑nil error is returned.
func FetchCIKFromRemote(ctx context.Context, ticker string) (string, string, error) {
	// Build a HTTP request bound to the provided context. We set a
	// User‑Agent header to be polite when accessing SEC resources.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.sec.gov/files/company_tickers.json", nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("User-Agent", "fin-guard/1.0")
	// Perform the request using the default client. This blocks until the
	// response is available or the context is cancelled.
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", fmt.Errorf("unexpected status %d fetching SEC company tickers", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	var data map[string]secTickerEntry
	if err := json.Unmarshal(body, &data); err != nil {
		return "", "", err
	}
	for _, entry := range data {
		if strings.EqualFold(entry.Ticker, ticker) {
			// CIKs are always 10 digits with leading zeros. Format the
			// integer accordingly.
			cik := fmt.Sprintf("%010d", entry.CIKStr)
			return cik, entry.Title, nil
		}
	}
	return "", "", fmt.Errorf("ticker not found: %s", ticker)
}

// FetchCIKFromTickerJSON fetches the SEC company_tickers.json at runtime.
// Returns zero-padded CIK (10 chars) and company title.
func FetchCIKFromTickerJSON(ctx context.Context, ticker string) (string, string, error) {
	body, err := FetchCompanyTickersJSON(ctx)
	if err != nil {
		return "", "", fmt.Errorf("unexpected status fetching company tickers: %w", err)
	}

	var m map[string]secTickerEntry
	if err := json.Unmarshal(body, &m); err != nil {
		return "", "", fmt.Errorf("json parse error: %w", err)
	}

	up := strings.ToUpper(ticker)
	for _, v := range m {
		if strings.EqualFold(v.Ticker, up) {
			// Zero-pad to 10
			cik := fmt.Sprintf("%010d", v.CIKStr)
			return cik, v.Title, nil
		}
	}
	return "", "", fmt.Errorf("ticker %s not found in SEC tickers map", ticker)
}
