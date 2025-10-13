package main

// serper.go implements ESG link search via the Serper API. It has
// been copied from the original utils/serper.go and modified only to
// reside in the top‑level package. All functions retain their
// behaviour so that ingest jobs can call SearchESGLinks without
// referencing the old utils directory. See broker_client.go for the
// rate‑broker integration used herein.

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

// SearchItem represents one organic search result returned by Serper.
// Only the title and link are used for ESG ingest. Additional fields
// are ignored when decoding the API response.
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

// LoadBlacklist reads a plain text file containing domains to exclude
// when scraping ESG links. Lines starting with # or blank lines are
// ignored. Domains are lower‑cased. Errors while opening or scanning
// the file are returned to the caller.
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

// hostMatchesDomain returns true when the given host exactly matches
// domain or is a subdomain of it. It performs a case‑insensitive
// comparison. This helper is used during blacklist filtering.
func hostMatchesDomain(host, domain string) bool {
    host = strings.ToLower(host)
    domain = strings.ToLower(domain)
    return host == domain || strings.HasSuffix(host, "."+domain)
}

// SearchESGLinks performs multiple Serper API calls to find links
// related to a company's ESG reporting. It iterates over a set of
// pillar keywords (ESG report, sustainability report, CSR report,
// climate report, DEI report) and aggregates unique results. Calls
// to the upstream API are mediated by the rate broker via
// acquirePermit/reportResult. On non‑fatal errors the function
// returns the error to the caller without aggregating partial
// results. The caller should handle such errors appropriately.
func SearchESGLinks(company string, cfg *Config) ([]SearchItem, error) {
    ctx := context.Background()
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
    // Helper to issue one Serper API request. It first attempts to
    // acquire a permit from the broker and call the API using the
    // returned key. If the broker returns an "unknown api" error it
    // falls back to calling Serper directly using the API key from
    // config. The function returns the parsed response, HTTP status
    // code, raw body and any error encountered.
    post := func(body any) (*serperResp, int, []byte, error) {
        b, _ := json.Marshal(body)
        // Attempt to acquire a permit from the rate broker.
        lease, err := acquirePermit(ctx, "serper")
        if err != nil {
            // If the broker doesn't recognise the API name, fall back to a
            // direct Serper API call using our own key. The broker wraps its
            // error strings with context, so perform a substring match.
            if strings.Contains(err.Error(), "unknown api") {
                // Select API key from config. Prefer SerperAPIKey, then
                // fallback to the first entry in SerperKeys.
                key := strings.TrimSpace(cfg.SerperAPIKey)
                if key == "" && len(cfg.SerperKeys) > 0 {
                    key = strings.TrimSpace(cfg.SerperKeys[0])
                }
                if key == "" {
                    // Without any key we cannot call the API.
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
            // Propagate other errors from the broker.
            return nil, 0, nil, fmt.Errorf("broker acquire: %w", err)
        }
        // With a broker lease, call the API using the provided key.
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
        // Always report the result back to the broker.
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
        qPlain := fmt.Sprintf("%s %s", company, pillar)
        qFull := map[string]any{
            "q":   qPlain,
            "num": 100,
        }
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
        time.Sleep(200 * time.Millisecond)
    }
    return combined, nil
}