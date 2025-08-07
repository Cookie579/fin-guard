package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
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

type serperResp struct {
	Organic []struct {
		Title string `json:"title"`
		Link  string `json:"link"`
	} `json:"organic"`
}

// 100 results per pillar, PDFs + HTML, no date filters
func SearchESGLinks(company string, cfg *Config) ([]SearchItem, error) {
	if strings.TrimSpace(cfg.SerperAPIKey) == "" {
		return nil, fmt.Errorf("SerperAPIKey missing: set serper_api_key in config.json or SERPER_API_KEY")
	}
	client := &http.Client{Timeout: 20 * time.Second}

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
		q := fmt.Sprintf("%s %s site:.com", company, pillar)

		body, _ := json.Marshal(map[string]any{
			"q":   q,
			"num": 100,  // ask for full top-100
			"gl":  "us", // locale hints; safe
			"hl":  "en",
		})

		req, _ := http.NewRequest("POST", "https://google.serper.dev/search", bytes.NewReader(body))
		req.Header.Set("X-API-KEY", strings.TrimSpace(cfg.SerperAPIKey))
		req.Header.Set("Content-Type", "application/json")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		if resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("serper.dev HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		}

		var parsed serperResp
		if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
			return nil, err
		}

		for _, o := range parsed.Organic {
			if o.Link == "" {
				continue
			}
			if u, err := url.Parse(o.Link); err == nil {
				u.Fragment = "" // drop #anchors
				o.Link = u.String()
			}
			if !seen[o.Link] {
				combined = append(combined, SearchItem{
					Title: strings.TrimSpace(o.Title),
					Link:  o.Link,
				})
				seen[o.Link] = true
			}
		}
	}

	return combined, nil
}
