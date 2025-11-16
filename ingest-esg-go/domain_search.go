package main

// domain_search.go wraps the Serper API used for domain
// discovery. Previously this file relied on Google's Custom Search API
// to locate a company's official website; it now uses the Serper
// API (https://serper.dev).  The function defined below retains the
// name GoogleSearch for backward compatibility but calls the Serper
// endpoint instead.

import (
    "bytes"
    "encoding/json"
    "fmt"
    "io"
    "net/http"
    "strings"
    "time"
    "context"
)

// GoogleSearchItem represents a single search result item.  In the
// original implementation this struct described a result from the
// Google Custom Search API; it is reused here for Serper results.
// Only title, link and snippet fields are populated when decoding
// responses.  Additional JSON fields are ignored.
type GoogleSearchItem struct {
    Title   string `json:"title"`
    Link    string `json:"link"`
    Snippet string `json:"snippet"`
}

// GoogleSearchResponse wraps the items array returned by the search
// provider.  Historically this was the Custom Search API response but
// it now holds Serper results.  An empty Items slice means no
// results were found.
type GoogleSearchResponse struct {
    Items []GoogleSearchItem `json:"items"`
}

// GoogleSearch performs a programmable search using the Serper API.  It
// posts a JSON body containing the query to google.serper.dev/search and
// returns the organic results.  The API key is read from
// cfg.SerperAPIKey; Google API keys and CX values are ignored.  On
// error the function returns a descriptive message.  The caller is
// responsible for handling empty result sets.
func GoogleSearch(query string, cfg *Config) (*GoogleSearchResponse, error) {
    // Validate config
    if cfg == nil {
        return nil, fmt.Errorf("Serper API key is missing in config")
    }
    // Construct the JSON payload. Always request up to 10 results.
    body, _ := json.Marshal(map[string]any{
        "q":   query,
        "num": 10,
    })
    // Attempt to acquire a broker permit for the Serper API. When the broker
    // knows about the "serper" API this will return a lease with a host
    // and key.  If it fails (e.g. unknown API) we fall back to using a
    // single key from config.
    ctx := context.Background()
    lease, err := acquirePermit(ctx, "serper")
    if err == nil {
        // Use the broker lease to call Serper.  The lease provides the host
        // and key; the host is the API domain (e.g. google.serper.dev) and
        // the key is one of the configured API tokens.  We always send a
        // POST request to /search.
        url := fmt.Sprintf("https://%s/search", lease.Host)
        req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
        if reqErr != nil {
            return nil, fmt.Errorf("failed to create Serper request: %w", reqErr)
        }
        req.Header.Set("Content-Type", "application/json")
        req.Header.Set("X-API-KEY", lease.Key)
        client := &http.Client{Timeout: 20 * time.Second}
        fmt.Println("🔍 Serper search query:", query)
        resp, doErr := client.Do(req)
        var status int
        var retryAfterMs int
        if resp != nil {
            status = resp.StatusCode
            retryAfterMs = parseRetryAfterMs(resp.Header.Get("Retry-After"), time.Now())
        }
        ok := (doErr == nil && status < 300)
        // Report the result to the broker.  This allows the broker to update its
        // internal rate calculations and freeze keys if necessary.
        reportResult(ctx, lease.ID, ok, status, retryAfterMs)
        if doErr != nil {
            return nil, fmt.Errorf("failed to call Serper API: %w", doErr)
        }
        defer resp.Body.Close()
        if status < 200 || status >= 300 {
            data, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
            return nil, fmt.Errorf("Serper API HTTP %d: %s", status, strings.TrimSpace(string(data)))
        }
        // Decode the Serper response.
        var raw struct {
            Organic []struct {
                Title   string `json:"title"`
                Link    string `json:"link"`
                Snippet string `json:"snippet"`
            } `json:"organic"`
        }
        if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
            return nil, fmt.Errorf("failed to parse Serper response: %w", err)
        }
        out := &GoogleSearchResponse{Items: make([]GoogleSearchItem, 0, len(raw.Organic))}
        for _, item := range raw.Organic {
            out.Items = append(out.Items, GoogleSearchItem{
                Title:   strings.TrimSpace(item.Title),
                Link:    strings.TrimSpace(item.Link),
                Snippet: strings.TrimSpace(item.Snippet),
            })
        }
        return out, nil
    }
    // Broker acquisition failed.  Fallback to direct usage using the first
    // available Serper key from config. Prefer the singular SerperAPIKey; if
    // empty, fall back to the first entry in SerperKeys.
    key := strings.TrimSpace(cfg.SerperAPIKey)
    if key == "" && len(cfg.SerperKeys) > 0 {
        key = strings.TrimSpace(cfg.SerperKeys[0])
    }
    if key == "" {
        return nil, fmt.Errorf("Serper API key is missing in config and broker acquire failed: %w", err)
    }
    req, reqErr := http.NewRequest(http.MethodPost, "https://google.serper.dev/search", bytes.NewReader(body))
    if reqErr != nil {
        return nil, fmt.Errorf("failed to create Serper request: %w", reqErr)
    }
    req.Header.Set("Content-Type", "application/json")
    req.Header.Set("X-API-KEY", key)
    client := &http.Client{Timeout: 20 * time.Second}
    fmt.Println("🔍 Serper search query:", query)
    resp, doErr := client.Do(req)
    if doErr != nil {
        return nil, fmt.Errorf("failed to call Serper API: %w", doErr)
    }
    defer resp.Body.Close()
    if resp.StatusCode < 200 || resp.StatusCode >= 300 {
        data, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
        return nil, fmt.Errorf("Serper API HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
    }
    var raw struct {
        Organic []struct {
            Title   string `json:"title"`
            Link    string `json:"link"`
            Snippet string `json:"snippet"`
        } `json:"organic"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
        return nil, fmt.Errorf("failed to parse Serper response: %w", err)
    }
    out := &GoogleSearchResponse{Items: make([]GoogleSearchItem, 0, len(raw.Organic))}
    for _, item := range raw.Organic {
        out.Items = append(out.Items, GoogleSearchItem{
            Title:   strings.TrimSpace(item.Title),
            Link:    strings.TrimSpace(item.Link),
            Snippet: strings.TrimSpace(item.Snippet),
        })
    }
    return out, nil
}