package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type SearchItem struct {
	Title string `json:"title"`
	Link  string `json:"link"`
}

type SearchResults struct {
	Items []SearchItem `json:"items"`
}

func SearchESGLinks(company string, cfg *Config) ([]SearchItem, error) {
	// Pillars / keywords to widen coverage
	pillars := []string{
		"ESG report",
		"sustainability report",
		"CSR report",
		"climate report",
		"DEI report",
	}

	seen := make(map[string]bool)
	var combined []SearchItem

	for _, pillar := range pillars {
		query := url.QueryEscape(fmt.Sprintf("%s %s site:.com", company, pillar))
		apiURL := fmt.Sprintf(
			"https://www.googleapis.com/customsearch/v1?q=%s&cx=%s&key=%s",
			query, cfg.GoogleSearchCX, cfg.GoogleAPIKey,
		)

		resp, err := http.Get(apiURL)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		var parsed SearchResults
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
			return nil, err
		}

		// Deduplicate by link
		for _, item := range parsed.Items {
			if !seen[item.Link] {
				combined = append(combined, item)
				seen[item.Link] = true
			}
		}
	}

	return combined, nil
}
