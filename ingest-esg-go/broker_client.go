// broker_client.go
//
// This file provides a thin wrapper around the rate‑broker service used to
// throttle outbound API calls. The broker exposes two endpoints: `/v1/acquire`
// to request a permit for a given API and `/v1/report` to report the
// outcome of a request. This implementation mirrors the logic used by
// ingest‑social‑go so that ESG ingest jobs conform to the same rate limiting
// contract. See runner_esg.go for usage.

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

// defaultBrokerURL defines the fallback URL of the rate broker service when
// running inside a Kubernetes cluster. If RATE_BROKER_URL is not set and the
// container is running inside a cluster, the broker will be reached via
// service DNS. Outside of a cluster the localhost URL is used instead.
const defaultBrokerURL = "http://rate-broker.default.svc:8080"

// brokerURL resolves the broker's base URL from the environment. If the
// RATE_BROKER_URL environment variable is provided it is returned verbatim.
// Otherwise the function returns the in‑cluster service name when a
// Kubernetes service account token is present, or localhost when running
// outside of the cluster.
func brokerURL() string {
    if v := os.Getenv("RATE_BROKER_URL"); v != "" {
        return v
    }
    // If the service account directory exists we assume we're inside a cluster.
    if _, err := os.Stat("/var/run/secrets/kubernetes.io"); err == nil {
        return defaultBrokerURL
    }
    return "http://localhost:8080"
}

// brokerAcquireReq represents the JSON payload sent to the broker's acquire
// endpoint. Each request includes the API name and the weight of the call.
type brokerAcquireReq struct {
    API    string `json:"api"`
    Weight int    `json:"weight"`
}

// brokerAcquireResp is the response from the broker's acquire endpoint. The
// broker will either return a lease immediately or instruct the client to
// wait for a specified number of milliseconds.
type brokerAcquireResp struct {
    LeaseID string `json:"lease_id"`
    Host    string `json:"host"`
    Key     string `json:"key"`
    WaitMs  int    `json:"wait_ms"`
}

// brokerReportReq is the JSON payload sent to the broker's report endpoint.
// The client reports whether the call succeeded, along with the HTTP
// response code and any Retry‑After hint returned by the upstream API.
type brokerReportReq struct {
    LeaseID      string `json:"lease_id"`
    OK           bool   `json:"ok"`
    Status       int    `json:"status,omitempty"`
    RetryAfterMs int    `json:"retry_after_ms,omitempty"`
}

// BrokerLease models the permit returned by the broker. Callers should
// populate request headers using the Host and Key fields and later report
// the outcome via reportResult.
type BrokerLease struct {
    ID   string
    Host string
    Key  string
}

// acquirePermit requests a permit for the given API from the broker. If the
// broker instructs us to wait the function will sleep for the requested
// duration and retry until a lease is granted or the context is cancelled.
func acquirePermit(ctx context.Context, api string) (BrokerLease, error) {
    url := brokerURL() + "/v1/acquire"
    reqBody := brokerAcquireReq{API: api, Weight: 1}
    for {
        var out brokerAcquireResp
        if err := httpJSON(ctx, http.MethodPost, url, reqBody, &out); err != nil {
            return BrokerLease{}, err
        }
        if out.WaitMs > 0 {
            // The broker asked us to wait before retrying. Respect the delay or
            // abort if the context is cancelled.
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

// reportResult reports the outcome of a call back to the broker. This is a
// best‑effort operation; any errors are ignored. If retryAfterMs is
// non‑zero the broker uses it to adjust its internal rate calculations.
func reportResult(ctx context.Context, leaseID string, ok bool, status int, retryAfterMs int) {
    url := brokerURL() + "/v1/report"
    _ = httpJSONNoResp(ctx, http.MethodPost, url, brokerReportReq{
        LeaseID:      leaseID,
        OK:           ok,
        Status:       status,
        RetryAfterMs: retryAfterMs,
    })
}

// httpJSON sends an HTTP request with a JSON body and optionally decodes
// the response into out. If out is nil the response body is ignored.
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

// httpJSONNoResp is a convenience wrapper over httpJSON when no response
// body is expected.
func httpJSONNoResp(ctx context.Context, method, url string, in any) error {
    return httpJSON[struct{}](ctx, method, url, in, nil)
}

// parseRetryAfterMs converts the contents of a Retry‑After header into a
// number of milliseconds. It supports both integer seconds and HTTP
// date formats as specified in RFC 7231. If the header is empty or
// invalid zero is returned.
func parseRetryAfterMs(h string, now time.Time) int {
    h = strings.TrimSpace(h)
    if h == "" {
        return 0
    }
    if secs, err := strconv.Atoi(h); err == nil && secs > 0 {
        return secs * 1000
    }
    // Try parsing as an HTTP date
    if t, err := http.ParseTime(h); err == nil {
        d := t.Sub(now)
        if d > 0 {
            return int(d / time.Millisecond)
        }
    }
    return 0
}