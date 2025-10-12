package main

import (
    "bufio"
    "bytes"
    "context"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "os"
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

// LoadBlacklist reads one domain per line and returns a set.
// Lines can be like:  scribd.com   responsibilityreports.com
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

// hostMatchesDomain matches exact host or any subdomain of domain
// host: www.s2.q4cdn.com   domain: q4cdn.com   => true
func hostMatchesDomain(host, domain string) bool {
	host = strings.ToLower(host)
	domain = strings.ToLower(domain)
	return host == domain || strings.HasSuffix(host, "."+domain)
}

func SearchESGLinks(company string, cfg *Config) ([]SearchItem, error) {
    // Use a context for broker interactions and HTTP calls. The context
    // can be cancelled by the caller if desired, but here a fresh
    // background context is sufficient because SearchESGLinks is short‑lived.
    ctx := context.Background()

    // HTTP client with a reasonable timeout. Each request still uses
    // acquirePermit to enforce rate limits via the broker. We reuse a
    // single client to avoid re‑creating the transport on every request.
    client := &http.Client{Timeout: 20 * time.Second}

    // Track seen URLs across all pillars to deduplicate results in memory.
    seen := make(map[string]bool)
    var combined []SearchItem

    pillars := []string{
        "ESG report",
        "sustainability report",
        "CSR report",
        "climate report",
        "DEI report",
    }

    // post performs a single Serper API call via the broker. It marshals
    // the provided body, acquires a permit for the "serper" API, issues
    // the HTTP request and reports the outcome back to the broker. It
    // returns the parsed Serper response, the HTTP status code and raw
    // body for error messages. On non‑2xx status or request error the
    // returned error contains context but the caller must still decide
    // whether to retry with a simpler payload.
    post := func(body any) (*serperResp, int, []byte, error) {
        b, _ := json.Marshal(body)
        // Acquire a permit from the broker. The broker will tell us when
        // it's safe to call the upstream API.
        lease, err := acquirePermit(ctx, "serper")
        if err != nil {
            return nil, 0, nil, fmt.Errorf("broker acquire: %w", err)
        }
        req, _ := http.NewRequestWithContext(ctx, http.MethodPost, "https://google.serper.dev/search", bytes.NewReader(b))
        req.Header.Set("Content-Type", "application/json")
        // Use the lease key provided by the broker as the API key header.
        req.Header.Set("X-API-KEY", lease.Key)
        // The broker sometimes returns a Host header for APIs that require
        // RapidAPI style calls. Serper does not use this host, but we
        // preserve the field for symmetry and future extensibility.
        // req.Header.Set("X-RapidAPI-Host", lease.Host)

        var status int
        var retryAfterMs int
        var raw []byte
        resp, doErr := client.Do(req)
        if resp != nil {
            status = resp.StatusCode
            // Capture Retry‑After to feed back into the broker rate logic.
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

    for _, pillar := range pillars {
        // start with a plain query (matches playground behavior)
        qPlain := fmt.Sprintf("%s %s", company, pillar)

        // try asking for 100; if Serper rejects it, fall back to minimal payload
        qFull := map[string]any{
            "q":   qPlain, // NO site:, NO date filters, NO extras
            "num": 100,    // ask for top‑100 (may be rejected on some keys)
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
        // tiny pause to be polite (and avoid flaky rejects). Even though
        // the broker manages rate limiting, adding a brief delay reduces
        // jitter and helps distribute load more evenly.
        time.Sleep(200 * time.Millisecond)
    }
    return combined, nil
}
