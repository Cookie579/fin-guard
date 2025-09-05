// broker_client.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultBrokerURL = "http://rate-broker.default.svc:8080"

func brokerURL() string {
	// If you later add config.json support, read from there.
	// For now, rely on the ClusterIP service DNS name:
	// return defaultBrokerURL
	if v := os.Getenv("RATE_BROKER_URL"); v != "" {
		return v
	}
	// If we're inside k8s, the ClusterIP name is fine; otherwise prefer localhost
	if _, err := os.Stat("/var/run/secrets/kubernetes.io"); err == nil {
		return defaultBrokerURL
	}
	return "http://localhost:8080"
}

type brokerAcquireReq struct {
	API    string `json:"api"`
	Weight int    `json:"weight"`
}
type brokerAcquireResp struct {
	LeaseID string `json:"lease_id"`
	Host    string `json:"host"`
	Key     string `json:"key"`
	WaitMs  int    `json:"wait_ms"`
}

type brokerReportReq struct {
	LeaseID      string `json:"lease_id"`
	OK           bool   `json:"ok"`
	Status       int    `json:"status,omitempty"`
	RetryAfterMs int    `json:"retry_after_ms,omitempty"`
}

type BrokerLease struct {
	ID   string
	Host string
	Key  string
}

// acquirePermit loops until the broker grants a permit (or ctx is done).
func acquirePermit(ctx context.Context, api string) (BrokerLease, error) {
	url := brokerURL() + "/v1/acquire"
	req := brokerAcquireReq{API: api, Weight: 1}

	for {
		var out brokerAcquireResp
		if err := httpJSON(ctx, http.MethodPost, url, req, &out); err != nil {
			return BrokerLease{}, err
		}
		if out.WaitMs > 0 {
			// polite sleep; broker told us when to retry
			t := time.NewTimer(time.Duration(out.WaitMs) * time.Millisecond)
			select {
			case <-ctx.Done():
				t.Stop()
				return BrokerLease{}, ctx.Err()
			case <-t.C:
			}
			continue
		}
		if out.LeaseID == "" || out.Key == "" || out.Host == "" {
			return BrokerLease{}, fmt.Errorf("broker returned empty lease/host/key")
		}
		return BrokerLease{ID: out.LeaseID, Host: out.Host, Key: out.Key}, nil
	}
}

// reportResult always try to report; it's best-effort.
func reportResult(ctx context.Context, leaseID string, ok bool, status int, retryAfterMs int) {
	url := brokerURL() + "/v1/report"
	_ = httpJSONNoResp(ctx, http.MethodPost, url, brokerReportReq{
		LeaseID:      leaseID,
		OK:           ok,
		Status:       status,
		RetryAfterMs: retryAfterMs,
	})
}

// helper: POST JSON and decode response (if v != nil)
func httpJSON[T any](ctx context.Context, method, url string, in any, out *T) error {
	b, err := json.Marshal(in)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("broker %s %s -> HTTP %d: %s", method, url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

// non-generic convenience for "no response body"
func httpJSONNoResp(ctx context.Context, method, url string, in any) error {
	return httpJSON[struct{}](ctx, method, url, in, nil)
}

// parseRetryAfter converts Retry-After header into milliseconds.
// Supports "seconds" or HTTP-date per RFC7231.
func parseRetryAfterMs(h string, now time.Time) int {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0
	}
	if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
		return secs * 1000
	}
	// Try HTTP-date
	if t, err := http.ParseTime(h); err == nil {
		d := t.Sub(now)
		if d > 0 {
			return int(d / time.Millisecond)
		}
	}
	return 0
}
