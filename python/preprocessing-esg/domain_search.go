package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
)

// GoogleSearchItem represents one search result
type GoogleSearchItem struct {
	Title   string `json:"title"`
	Link    string `json:"link"`
	Snippet string `json:"snippet"`
}

// GoogleSearchResponse maps the full response
type GoogleSearchResponse struct {
	Items []GoogleSearchItem `json:"items"`
}

// GoogleSearch performs a search using Google Custom Search API
func GoogleSearch(query string, cfg *Config) (*GoogleSearchResponse, error) {
	encodedQuery := url.QueryEscape(query)

	apiURL := fmt.Sprintf(
		"https://www.googleapis.com/customsearch/v1?q=%s&key=%s&cx=%s",
		encodedQuery,
		cfg.GoogleAPIKey,
		cfg.GoogleSearchCX,
	)

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
