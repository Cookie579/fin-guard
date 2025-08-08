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

type serperResp struct {
	Organic []struct {
		Title string `json:"title"`
		Link  string `json:"link"`
	} `json:"organic"`
}

func SearchESGLinks(company string, cfg *Config) ([]SearchItem, error) {
	if strings.TrimSpace(cfg.SerperAPIKey) == "" {
		return nil, fmt.Errorf("SerperAPIKey missing: set serper_api_key in config.json or SERPER_API_KEY")
	}

	client := &http.Client{Timeout: 20 * time.Second}
	seen := make(map[string]bool)
	var combined []SearchItem

	pillars := []string{
		"ESG report",
		"sustainability report",
		"CSR report",
		"climate report",
		"DEI report",
	}

	post := func(body any) (*serperResp, int, []byte, error) {
		b, _ := json.Marshal(body)
		req, _ := http.NewRequest("POST", "https://google.serper.dev/search", bytes.NewReader(b))
		req.Header.Set("X-API-KEY", strings.TrimSpace(cfg.SerperAPIKey))
		req.Header.Set("Content-Type", "application/json")
		resp, err := client.Do(req)
		if err != nil {
			return nil, 0, nil, err
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		if resp.StatusCode >= 300 {
			return nil, resp.StatusCode, raw, fmt.Errorf("serper.dev HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(raw)))
		}
		var out serperResp
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, resp.StatusCode, raw, err
		}
		return &out, resp.StatusCode, raw, nil
	}

	for _, pillar := range pillars {
		// start with a plain query (matches playground behavior)
		qPlain := fmt.Sprintf("%s %s", company, pillar)

		// try asking for 100; if Serper rejects it, fall back to minimal payload
		qFull := map[string]any{
			"q":   qPlain, // NO site:, NO date filters, NO extras
			"num": 100,    // ask for top-100 (may be rejected on some keys)
		}
		respParsed, _, _, err := post(qFull)
		if err != nil && strings.Contains(err.Error(), "Query not allowed") {
			// fallback to EXACTLY what their sample shows
			respParsed, _, _, err = post(map[string]any{"q": qPlain})
		}
		if err != nil {
			// bubble one pillar error; continue to next company in your caller
			return nil, err
		}

		for _, o := range respParsed.Organic {
			link := strings.TrimSpace(o.Link)
			if link == "" || seen[link] {
				continue
			}
			if u, err := url.Parse(link); err == nil {
				u.Fragment = ""
				link = u.String()
			}
			seen[link] = true
			combined = append(combined, SearchItem{
				Title: strings.TrimSpace(o.Title),
				Link:  link,
			})
		}

		// tiny pause to be polite (and avoid flaky rejects)
		time.Sleep(200 * time.Millisecond)
	}

	return combined, nil
}
