
package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"sync"
)

type youTubeKeyRotator struct {
	mu    sync.Mutex
	keys  []string
	index int
	dead  map[int]bool
}

func newYouTubeKeyRotator(keys []string) *youTubeKeyRotator {
	ks := make([]string, 0, len(keys))
	seen := map[string]struct{}{}
	for _, k := range keys {
		k = strings.TrimSpace(k)
		if k == "" {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		ks = append(ks, k)
	}
	return &youTubeKeyRotator{
		keys: ks,
		dead: make(map[int]bool),
	}
}

func (r *youTubeKeyRotator) size() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.keys)
}

func (r *youTubeKeyRotator) current() (string, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.keys) == 0 {
		return "", false
	}
	// advance until a non-dead key is found; if all dead, return false
	start := r.index
	for {
		if !r.dead[r.index] {
			return r.keys[r.index], true
		}
		r.index = (r.index + 1) % len(r.keys)
		if r.index == start {
			return "", false
		}
	}
}

func (r *youTubeKeyRotator) markDead() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.keys) == 0 {
		return
	}
	r.dead[r.index] = true
	// advance to next non-dead slot if available
	for i := 0; i < len(r.keys); i++ {
		r.index = (r.index + 1) % len(r.keys)
		if !r.dead[r.index] {
			break
		}
	}
}

type ytAPIError struct {
	Error struct {
		Code   int `json:"code"`
		Errors []struct {
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"errors"`
		Message string `json:"message"`
	} `json:"error"`
}

func isYouTubeQuotaError(status int, body []byte) bool {
	if status != http.StatusForbidden && status != http.StatusTooManyRequests {
		// YouTube often uses 403 for quota exceeded; 429 for generic rate limiting
	}
	var e ytAPIError
	if err := json.Unmarshal(body, &e); err == nil {
		for _, it := range e.Error.Errors {
			switch strings.ToLower(it.Reason) {
			case "quotaexceeded", "dailylimitexceeded", "ratelimitexceeded", "usageLimits.userRateLimitExceeded":
				return true
			}
		}
		// Some responses omit errors[] and put the string directly in message
		if strings.Contains(strings.ToLower(e.Error.Message), "quota") {
			return true
		}
	}
	// fallback string search if body isn't structured as expected
	s := strings.ToLower(string(body))
	return strings.Contains(s, "quotaexceeded") ||
		strings.Contains(s, "daily limit") ||
		strings.Contains(s, "rate limit")
}

// quotaAwareTransport injects a YouTube API key into requests and rotates keys on quota errors.
type quotaAwareTransport struct {
	base http.RoundTripper
	rot  *youTubeKeyRotator
}

func newQuotaAwareTransport(base http.RoundTripper, rot *youTubeKeyRotator) *quotaAwareTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &quotaAwareTransport{base: base, rot: rot}
}

func (t *quotaAwareTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Only intercept YouTube Data API calls
	host := strings.ToLower(req.URL.Host)
	if !strings.Contains(host, "googleapis.com") || !strings.Contains(req.URL.Path, "/youtube/v3/") {
		return t.base.RoundTrip(req)
	}
	max := t.rot.size()
	if max == 0 {
		return t.base.RoundTrip(req)
	}
	for i := 0; i < max; i++ {
		key, ok := t.rot.current()
		if !ok {
			break
		}
		// clone request to avoid mutating caller
		r2 := req.Clone(req.Context())
		q := r2.URL.Query()
		q.Set("key", key)
		r2.URL.RawQuery = q.Encode()

		resp, err := t.base.RoundTrip(r2)
		if err != nil {
			return resp, err
		}
		// Read body to inspect for quota errors
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if isYouTubeQuotaError(resp.StatusCode, b) {
			// rotate and try again with next key
			t.rot.markDead()
			// continue loop
			continue
		}
		// Restore body for caller
		resp.Body = io.NopCloser(bytes.NewReader(b))
		return resp, nil
	}
	// If all keys exhausted, attempt once with original request (no key rewrite)
	return t.base.RoundTrip(req)
}
