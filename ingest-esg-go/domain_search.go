package main

// domain_search.go wraps the Google Custom Search API used for domain
// discovery. It is identical to the original utils/domain_search.go
// implementation but resides in the top‑level package for ease of
// compilation.

import (
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
)

// GoogleSearchItem represents a single search result item from the
// Google Custom Search API. Only title, link and snippet fields are
// parsed. Additional fields in the JSON response are ignored.
type GoogleSearchItem struct {
    Title   string `json:"title"`
    Link    string `json:"link"`
    Snippet string `json:"snippet"`
}

// GoogleSearchResponse wraps the items array returned by the Custom
// Search API. An empty Items slice means no results were found.
type GoogleSearchResponse struct {
    Items []GoogleSearchItem `json:"items"`
}

// GoogleSearch performs a programmable search using the Google Custom
// Search API. It constructs the API URL from the query and the
// provided Config values (API key and search CX). It returns a
// parsed response or an error if the request fails or the response
// cannot be decoded. The caller is responsible for handling
// non‑existent items.
func GoogleSearch(query string, cfg *Config) (*GoogleSearchResponse, error) {
    encodedQuery := url.QueryEscape(query)
    apiURL := fmt.Sprintf(
        "https://www.googleapis.com/customsearch/v1?q=%s&key=%s&cx=%s",
        encodedQuery,
        cfg.GoogleAPIKey,
        cfg.GoogleSearchCX,
    )
    fmt.Println("🔍 Final Google Search URL:", apiURL)
    resp, err := http.Get(apiURL)
    if err != nil {
        return nil, fmt.Errorf("failed to call Google Search API: %w", err)
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        bodyBytes, _ := io.ReadAll(resp.Body)
        return nil, fmt.Errorf("non-200 from Google Search: %s\nBody: %s", resp.Status, string(bodyBytes))
    }
    var searchResults GoogleSearchResponse
    if err := json.NewDecoder(resp.Body).Decode(&searchResults); err != nil {
        return nil, fmt.Errorf("failed to parse search response: %w", err)
    }
    return &searchResults, nil
}