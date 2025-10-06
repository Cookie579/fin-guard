// httpsec.go
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	secCompanyTickersURLs = []string{
		"https://www.sec.gov/files/company_tickers.json",
		"https://data.sec.gov/files/company_tickers.json",
	}
)

// shared SEC HTTP client (keep-alive, timeouts)
var secHTTP = &http.Client{
	Transport: &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		IdleConnTimeout:       30 * time.Second,
		MaxIdleConns:          50,
		MaxConnsPerHost:       10,
		Proxy:                 http.ProxyFromEnvironment,
	},
	Timeout: 20 * time.Second,
}

// SECUserAgent returns a UA that complies with SEC Fair Access.
// Set via env SEC_HTTP_UA (kept in a Kubernetes Secret).
func SECUserAgent() string {
	ua := strings.TrimSpace(os.Getenv("SEC_HTTP_UA"))
	if ua == "" {
		// A sensible default if the Secret isn't provided (still contains contact)
		ua = "FinGuardSEC/1.0 (contact: sec-contact@yourdomain.tld)"
	}
	return ua
}

// doSECGet performs an HTTP GET with the SEC UA and returns body bytes.
// Retries on 429/403 with exponential backoff and respects Retry-After if present.
func doSECGet(ctx context.Context, url string) ([]byte, error) {
	ua := SECUserAgent()
	var lastErr error

	backoff := 500 * time.Millisecond
	for attempt := 0; attempt < 5; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", ua)
		req.Header.Set("Accept", "application/json")
		req.Header.Set("Connection", "keep-alive")
		req.Header.Set("Accept-Language", "en-US,en;q=0.8")
		req.Header.Set("Referer", "https://www.sec.gov/")

		resp, err := secHTTP.Do(req)
		if err != nil {
			lastErr = err
		} else {
			defer resp.Body.Close()

			// Happy path
			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				b, rerr := io.ReadAll(resp.Body)
				if rerr != nil {
					return nil, rerr
				}
				return b, nil
			}

			// Handle throttling/soft blocks
			if resp.StatusCode == 429 || resp.StatusCode == 403 || (resp.StatusCode >= 500 && resp.StatusCode <= 599) {
				// Respect Retry-After if present
				if ra := resp.Header.Get("Retry-After"); ra != "" {
					if secs, perr := strconv.Atoi(strings.TrimSpace(ra)); perr == nil && secs > 0 && secs <= 30 {
						time.Sleep(time.Duration(secs) * time.Second)
					} else {
						time.Sleep(backoff)
						backoff *= 2
						if backoff > 8*time.Second {
							backoff = 8 * time.Second
						}
					}
				} else {
					time.Sleep(backoff)
					backoff *= 2
					if backoff > 8*time.Second {
						backoff = 8 * time.Second
					}
				}
				lastErr = fmt.Errorf("unexpected status %d from SEC", resp.StatusCode)
				continue
			}

			// Hard error
			b, _ := io.ReadAll(resp.Body)
			return nil, fmt.Errorf("SEC GET %s -> %d: %s", url, resp.StatusCode, strings.TrimSpace(string(b)))
		}
	}

	if lastErr == nil {
		lastErr = errors.New("SEC GET failed after retries")
	}
	return nil, lastErr
}

// FetchCompanyTickersJSON tries multiple SEC hosts with backoff.
func FetchCompanyTickersJSON(ctx context.Context) ([]byte, error) {
	var last error
	for _, u := range secCompanyTickersURLs {
		body, err := doSECGet(ctx, u)
		if err == nil {
			return body, nil
		}
		last = err
	}
	return nil, last
}
