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

// FetchCIKFromTickerJSON downloads the SEC company tickers JSON file and
// searches for the provided ticker symbol. The JSON is available at
// https://www.sec.gov/files/company_tickers.json and contains a map
// keyed by an integer ID. Each entry has the fields cik_str, ticker
// and title. This function iterates through the map and returns the
// zero‑padded CIK string and company name when a matching ticker is
// found. If the ticker is not present or an error occurs fetching or
// parsing the file, an error is returned. A polite User‑Agent header
// is set on the request to comply with SEC guidance.
func FetchCIKFromTickerJSON(ctx context.Context, ticker string) (string, string, error) {
    // Build the HTTP request. Use a context to allow cancellation. Set a
    // descriptive User‑Agent to improve the odds of a successful response.
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.sec.gov/files/company_tickers.json", nil)
    if err != nil {
        return "", "", err
    }
    req.Header.Set("User-Agent", "fin-guard/1.0")
    // Perform the request using the default HTTP client.
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return "", "", err
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return "", "", fmt.Errorf("unexpected status %d fetching company tickers", resp.StatusCode)
    }
    // Read the entire response body. The file is a few megabytes and
    // should comfortably fit in memory for a single lookup.
    body, err := io.ReadAll(resp.Body)
    if err != nil {
        return "", "", err
    }
    // The JSON structure is a map from string keys to objects. Use a
    // temporary struct to unmarshal each entry. We do not know the
    // numeric keys ahead of time, so unmarshal into a map.
    var data map[string]struct {
        CIKStr int    `json:"cik_str"`
        Ticker string `json:"ticker"`
        Title  string `json:"title"`
    }
    if err := json.Unmarshal(body, &data); err != nil {
        return "", "", err
    }
    // Iterate over the entries to find the matching ticker (case‑insensitive).
    for _, entry := range data {
        if strings.EqualFold(entry.Ticker, ticker) {
            cik := fmt.Sprintf("%010d", entry.CIKStr)
            return cik, entry.Title, nil
        }
    }
    return "", "", fmt.Errorf("ticker not found: %s", ticker)
}
