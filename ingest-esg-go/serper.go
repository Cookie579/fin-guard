package main

// serper.go implements ESG link search via the Serper API. It has
// been copied from the original utils/serper.go and modified only to
// reside in the top-level package. All functions retain their
// behaviour so that ingest jobs can call SearchESGLinks without
// referencing the old utils directory. See broker_client.go for the
// rate-broker integration used herein.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log" // for progress logs when min-new is reached
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

// SearchItem represents one organic search result returned by Serper.
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

// LoadBlacklist reads a plain text file containing domains to exclude.
func LoadBlacklist(path string) (map[string]struct{}, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	blk := make(map[string]struct{})
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		d := strings.ToLower(strings.TrimSpace(sc.Text()))
		if d == "" || strings.HasPrefix(d, "#") {
			continue
		}
		blk[d] = struct{}{}
	}
	return blk, sc.Err()
}

// hostMatchesDomain returns true when host == domain or is a subdomain.
func hostMatchesDomain(host, domain string) bool {
	host = strings.ToLower(host)
	domain = strings.ToLower(domain)
	return host == domain || strings.HasSuffix(host, "."+domain)
}

// SearchESGLinks performs multiple Serper API calls to find ESG links.
// When minResults > 0, it will stop scheduling NEW query blocks as soon
// as >= minResults unique links have been aggregated (it still finishes
// the block that crossed the threshold).
func SearchESGLinks(company string, cfg *Config, minResults int) ([]SearchItem, error) {
	ctx := context.Background()
	client := &http.Client{Timeout: 20 * time.Second}
	seen := make(map[string]bool)
	var combined []SearchItem

	// Helper: have we hit the requested minimum?
	enough := func() bool {
		return minResults > 0 && len(combined) >= minResults
	}

	// ESG-related query pillars
	pillars := []string{
		"ESG report",
		"sustainability report",
		"CSR report",
		"climate report",
		"DEI report",
		"ESG",
		"sustainability",
		"CSR",
		"climate",
		"DEI",
	}

	// Helper to POST to Serper (via broker if available)
	post := func(body any) (*serperResp, int, []byte, error) {
		b, _ := json.Marshal(body)
		lease, err := acquirePermit(ctx, "serper")
		if err != nil {
			// Fallback to direct Serper if the broker doesn't know this API
			if strings.Contains(err.Error(), "unknown api") {
				key := strings.TrimSpace(cfg.SerperAPIKey)
				if key == "" && len(cfg.SerperKeys) > 0 {
					key = strings.TrimSpace(cfg.SerperKeys[0])
				}
				if key == "" {
					return nil, 0, nil, fmt.Errorf("broker acquire: %w", err)
				}
				req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://google.serper.dev/search", bytes.NewReader(b))
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("X-API-KEY", key)
				var status int
				var raw []byte
				resp, doErr := client.Do(req)
				if resp != nil {
					status = resp.StatusCode
					raw, _ = io.ReadAll(resp.Body)
					resp.Body.Close()
				}
				if doErr != nil {
					return nil, status, raw, doErr
				}
				if status >= 300 {
					return nil, status, raw, fmt.Errorf("serper.dev HTTP %d: %s", status, strings.TrimSpace(string(raw)))
				}
				var out serperResp
				if err := json.Unmarshal(raw, &out); err != nil {
					return nil, status, raw, err
				}
				return &out, status, raw, nil
			}
			return nil, 0, nil, fmt.Errorf("broker acquire: %w", err)
		}

		req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://google.serper.dev/search", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-API-KEY", lease.Key)

		var status int
		var retryAfterMs int
		var raw []byte

		resp, doErr := client.Do(req)
		if resp != nil {
			status = resp.StatusCode
			retryAfterMs = parseRetryAfterMs(resp.Header.Get("Retry-After"), time.Now())
			raw, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
		}
		ok := (doErr == nil && status < 300)
		reportResult(ctx, lease.ID, ok, status, retryAfterMs)

		if doErr != nil {
			return nil, status, raw, doErr
		}
		if status >= 300 {
			return nil, status, raw, fmt.Errorf("serper.dev HTTP %d: %s", status, strings.TrimSpace(string(raw)))
		}
		var out serperResp
		if err := json.Unmarshal(raw, &out); err != nil {
			return nil, status, raw, err
		}
		return &out, status, raw, nil
	}

	// -------- Pillar sweep --------
PILLARS:
	for _, pillar := range pillars {
		qPlain := fmt.Sprintf("%s %s", company, pillar)
		qFull := map[string]any{"q": qPlain, "num": 100}

		respParsed, _, _, err := post(qFull)
		if err != nil && strings.Contains(err.Error(), "Query not allowed") {
			respParsed, _, _, err = post(map[string]any{"q": qPlain})
		}
		if err != nil {
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
		if enough() {
			log.Printf("🎯 min-new reached (%d >= %d) after pillar sweep; stopping new pillar queries", len(combined), minResults)
			break PILLARS
		}
		time.Sleep(200 * time.Millisecond)
	}

	// -------- Filetype sweep --------
	fileTypes := []string{"pdf", "aspx"}
FILETYPES:
	for _, ft := range fileTypes {
		for _, pillar := range pillars {
			qPlain := fmt.Sprintf("%s %s filetype:%s", company, pillar, ft)
			qFull := map[string]any{"q": qPlain, "num": 100}

			respParsed, _, _, err := post(qFull)
			if err != nil && strings.Contains(err.Error(), "Query not allowed") {
				respParsed, _, _, err = post(map[string]any{"q": qPlain})
			}
			if err != nil {
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
			if enough() {
				log.Printf("🎯 min-new reached (%d >= %d) after filetype sweep; stopping new filetype queries", len(combined), minResults)
				break FILETYPES
			}
			time.Sleep(200 * time.Millisecond)
		}
	}

	// -------- Year sweep (present → 1980) --------
	emptyYearStreak := 0
	maxEmptyStreak := 3
	currentYear := time.Now().Year()

YEARS:
	for y := currentYear; y >= 1980 && emptyYearStreak < maxEmptyStreak; y-- {
		foundAny := false
		for _, pillar := range pillars {
			qPlain := fmt.Sprintf("%s %s %d", company, pillar, y)
			qFull := map[string]any{"q": qPlain, "num": 100}

			respParsed, _, _, err := post(qFull)
			if err != nil && strings.Contains(err.Error(), "Query not allowed") {
				respParsed, _, _, err = post(map[string]any{"q": qPlain})
			}
			if err != nil {
				return nil, err
			}
			if len(respParsed.Organic) > 0 {
				foundAny = true
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
			time.Sleep(200 * time.Millisecond)
		}
		if foundAny {
			emptyYearStreak = 0
		} else {
			emptyYearStreak++
		}
		if enough() {
			log.Printf("🎯 min-new reached (%d >= %d) at year %d; stopping new year queries", len(combined), minResults, y)
			break YEARS
		}
	}

	// -------- Extended year sweep (1979 → 1900) if still below target --------
	if minResults > 0 && len(combined) < minResults {
		earliestYear := 1900
		emptyYearStreak = 0
		for y := 1979; y >= earliestYear && emptyYearStreak < maxEmptyStreak; y-- {
			foundAny := false
			for _, pillar := range pillars {
				qPlain := fmt.Sprintf("%s %s %d", company, pillar, y)
				qFull := map[string]any{"q": qPlain, "num": 100}

				respParsed, _, _, err := post(qFull)
				if err != nil && strings.Contains(err.Error(), "Query not allowed") {
					respParsed, _, _, err = post(map[string]any{"q": qPlain})
				}
				if err != nil {
					return nil, err
				}
				if len(respParsed.Organic) > 0 {
					foundAny = true
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
				time.Sleep(200 * time.Millisecond)
			}
			if foundAny {
				emptyYearStreak = 0
			} else {
				emptyYearStreak++
			}
			if enough() {
				log.Printf("🎯 min-new reached (%d >= %d) at extended year %d; stopping new queries", len(combined), minResults, y)
				break // break the extended year for-loop
			}
		}
	}

	return combined, nil
}
