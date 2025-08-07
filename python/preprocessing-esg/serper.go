package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type SearchItem struct {
	Title string `json:"title"`
	Link  string `json:"link"`
}

type SearchResults struct {
	Items []SearchItem `json:"items"`
}

// Serper response (we only need the organic list)
type serperResp struct {
	Organic []struct {
		Title string `json:"title"`
		Link  string `json:"link"`
	} `json:"organic"`
}

// SearchESGLinks queries Serper.dev (100 results per pillar, no time window)
func SearchESGLinks(company string, cfg *Config) ([]SearchItem, error) {
	if cfg.SerperAPIKey == "" {
		return nil, fmt.Errorf("SerperAPIKey missing: set serper_api_key in config.json or SERPER_API_KEY env var")
	}

	// Broaden coverage with a few pillar keywords (same as before)
	pillars := []string{
		"ESG report",
		"sustainability report",
		"CSR report",
		"climate report",
		"DEI report",
	}

	client := &http.Client{Timeout: 20 * time.Second}

	seen := make(map[string]bool)
	var combined []SearchItem

	for _, pillar := range pillars {
		// Plain query; no time window, no filetype restriction (returns PDFs + HTML)
		q := fmt.Sprintf("%s %s site:.com", company, pillar)

		payload, _ := json.Marshal(map[string]any{
			"q":           q,
			"num":         100, // ask for the full top-100
			"gl":          "us",
			"hl":          "en",
			"autocorrect": false,
		})

		req, _ := http.NewRequest("POST", "https://google.serper.dev/search", bytes.NewReader(payload))
		req.Header.Set("X-API-KEY", cfg.SerperAPIKey)
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode == http.StatusTooManyRequests {
			return nil, fmt.Errorf("serper.dev: rate limited (429). Slow down requests")
		}
		if resp.StatusCode >= 300 {
			return nil, fmt.Errorf("serper.dev HTTP %d", resp.StatusCode)
		}

		var parsed serperResp
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
			return nil, err
		}

		for _, o := range parsed.Organic {
			if o.Link == "" {
				continue
			}
			// normalize URL (trim tracking params if you want)
			if u, err := url.Parse(o.Link); err == nil {
				u.Fragment = ""
				o.Link = u.String()
			}
			if !seen[o.Link] {
				combined = append(combined, SearchItem{Title: strings.TrimSpace(o.Title), Link: o.Link})
				seen[o.Link] = true
			}
		}
	}

	return combined, nil
}
