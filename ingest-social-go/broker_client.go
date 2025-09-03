package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"
)

var brokerURL = getenv("RATE_BROKER_URL", "http://rate-broker.default.svc.cluster.local:8080")

func getenv(k, def string) string {
	v := os.Getenv(k)
	if v == "" {
		return def
	}
	return v
}

type lease struct {
	LeaseID string `json:"lease_id"`
	Host    string `json:"host"`
	Key     string `json:"key"`
	WaitMs  int    `json:"wait_ms"`
}

func httpPost[T any](ctx context.Context, url string, body any) (T, error) {
	var zero T
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return zero, fmt.Errorf("http %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&zero); err != nil {
		return zero, err
	}
	return zero, nil
}

func acquirePermit(ctx context.Context, api string) (lease, error) {
	for {
		l, err := httpPost[lease](ctx, brokerURL+"/v1/acquire", map[string]any{"api": api, "weight": 1})
		if err != nil {
			return lease{}, err
		}
		if l.WaitMs > 0 {
			select {
			case <-ctx.Done():
				return lease{}, ctx.Err()
			case <-time.After(time.Duration(l.WaitMs) * time.Millisecond):
			}
			continue
		}
		return l, nil
	}
}

func reportResult(ctx context.Context, leaseID string, ok bool, status int, retryAfter string) {
	req := map[string]any{"lease_id": leaseID, "ok": ok}
	if !ok {
		req["status"] = status
		if retryAfter != "" {
			// Retry-After can be seconds or HTTP-date
			if secs, err := strconv.Atoi(retryAfter); err == nil && secs > 0 {
				req["retry_after_ms"] = secs * 1000
			} else if t, err := http.ParseTime(retryAfter); err == nil {
				d := time.Until(t)
				if d > 0 {
					req["retry_after_ms"] = int(d / time.Millisecond)
				}
			}
		}
	}
	_, _ = httpPost[struct{}](ctx, brokerURL+"/v1/report", req) // best-effort
}
