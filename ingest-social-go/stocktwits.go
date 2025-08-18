//go:build ignore
// +build ignore

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Cookie579/fin-guard/config"
)

/*────────────────── compile-time knobs ──────────────────*/

// flip to true if you want an ingest timestamp in every row
const keepRetrievedAt = false

// ───────── logging helpers ─────────
var stderr = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)

func errorf(format string, a ...any) { stderr.Printf("ERROR: "+format, a...) }
func warnf(format string, a ...any)  { stderr.Printf("WARN:  "+format, a...) }

func trimBody(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

/*────────────────── payload slimming ──────────────────*/

func slimOne(src map[string]any) map[string]any {
	dst := map[string]any{
		"id":         src["id"],
		"created_at": src["created_at"],
		"body":       src["body"],
	}

	// symbols  → []string
	if arr, ok := src["symbols"].([]any); ok {
		var syms []string
		for _, it := range arr {
			if m, ok := it.(map[string]any); ok {
				if s, ok := m["symbol"].(string); ok {
					syms = append(syms, s)
				}
			}
		}
		if len(syms) > 0 {
			dst["symbols"] = syms
		}
	}

	// sentiment
	if ent, ok := src["entities"].(map[string]any); ok {
		if sent, ok := ent["sentiment"].(map[string]any); ok {
			if basic, ok := sent["basic"]; ok {
				dst["sentiment"] = basic
			}
		}
	}

	// likes
	if lk, ok := src["likes"].(map[string]any); ok {
		dst["likes"] = lk["total"]
	}

	// reshares
	if rs, ok := src["reshares"].(map[string]any); ok {
		dst["reshares"] = rs["reshared_count"]
	}

	// minimal user
	if u, ok := src["user"].(map[string]any); ok {
		dst["user"] = map[string]any{
			"id":       u["id"],
			"username": u["username"],
		}
	}

	// conversation (only if something non-nil)
	if c, ok := src["conversation"].(map[string]any); ok {
		if c["parent_message_id"] != nil || c["in_reply_to_message_id"] != nil {
			dst["conversation"] = map[string]any{
				"parent_message_id":      c["parent_message_id"],
				"in_reply_to_message_id": c["in_reply_to_message_id"],
			}
		}
	}

	// quoted / reshared (recursively slimmed)
	if rm, ok := src["reshare_message"].(map[string]any); ok {
		if msg, ok := rm["message"].(map[string]any); ok {
			dst["reshare_message"] = map[string]any{
				"message": slimOne(msg),
			}
		}
	}

	return dst
}

func slimPayload(orig json.RawMessage) json.RawMessage {
	var top map[string]any
	_ = json.Unmarshal(orig, &top)
	b, _ := json.Marshal(slimOne(top))
	return b
}

/*────────────────── data structures ──────────────────*/

// persisted structure
type stateRec struct {
	LatestID int   `json:"latest_id"`
	OldestID int   `json:"oldest_id"`
	IdsSeen  []int `json:"ids_seen,omitempty"`

	// in-memory only
	seen map[int]struct{} `json:"-"`
}

// one stored line
type RawPost struct {
	ID      string          `json:"id"`
	Ticker  string          `json:"ticker"`
	Source  string          `json:"source"` // "stocktwits"
	Payload json.RawMessage `json:"payload"`
	// optional
	RetrievedAt *time.Time `json:"retrieved_at,omitempty"`
}

/*────────────────── key rotation ──────────────────*/

type keyRotator struct {
	keys []string
	mu   sync.Mutex
	idx  int
}

func newRotator(keys []string) *keyRotator { return &keyRotator{keys: keys} }
func (r *keyRotator) Next() string {
	r.mu.Lock()
	k := r.keys[r.idx%len(r.keys)]
	r.idx++
	r.mu.Unlock()
	return k
}

/*────────────────── constants ──────────────────*/

const (
	stateFile  = "data/stocktwits_state.json"
	baseURL    = "https://stocktwits.p.rapidapi.com"
	hostHdr    = "stocktwits.p.rapidapi.com"
	userAgent  = "StockTwitsCollector/0.3 (+github.com/Cookie579)"
	baseDelay  = 3600 * time.Millisecond // ≈1 000 req/h per key
	outputRoot = "data"
)

/*────────────────── I/O helpers ──────────────────*/

var httpClient = &http.Client{Timeout: 45 * time.Second}

func fetch(ctx context.Context, key, path string, qs map[string]string) ([]byte, int, error) {
	u := baseURL + path
	if len(qs) > 0 {
		var q []string
		for k, v := range qs {
			q = append(q, k+"="+v)
		}
		u += "?" + strings.Join(q, "&")
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
	req.Header.Set("X-RapidAPI-Host", hostHdr)
	req.Header.Set("X-RapidAPI-Key", key)
	req.Header.Set("User-Agent", userAgent)
	req.Header.Set("Accept", "application/json")

	start := time.Now()
	resp, err := httpClient.Do(req)
	if err != nil {
		errorf("HTTP %s failed: %v", path, err)
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// slow request warning
	if dur := time.Since(start); dur > 2*time.Second {
		warnf("slow %s took %s", path, dur)
	}
	// non-200 log
	if resp.StatusCode != 200 {
		errorf("status %d on %s → %s", resp.StatusCode, path, trimBody(body, 300))
	}
	return body, resp.StatusCode, nil
}

func msgDate(raw json.RawMessage) (y, m, d string) {
	var tmp struct {
		Created string `json:"created_at"`
	}
	_ = json.Unmarshal(raw, &tmp)
	if t, err := time.Parse(time.RFC3339, tmp.Created); err == nil {
		return t.UTC().Format("2006"), t.Format("01"), t.Format("02")
	}
	now := time.Now().UTC()
	return now.Format("2006"), now.Format("01"), now.Format("02")
}

func chooseFileName(dir, tk, y, m, d string) string {
	prefix := fmt.Sprintf("StockTwits_%s_%s_%s_%s_", tk, y, m, d)
	n := 0
	if files, _ := os.ReadDir(dir); len(files) > 0 {
		for _, f := range files {
			if strings.HasPrefix(f.Name(), prefix) {
				n++
			}
		}
	}
	return prefix + strconv.Itoa(n+1) + ".json"
}

func writeBatch(tk, y, m, d string, posts []RawPost) {
	if len(posts) == 0 {
		return
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, p := range posts {
		_ = enc.Encode(p)
	}

	dir := filepath.Join(outputRoot, "raw", "stocktwits", tk, y)
	_ = os.MkdirAll(dir, 0o755)
	file := filepath.Join(dir, chooseFileName(dir, tk, y, m, d))
	_ = os.WriteFile(file, buf.Bytes(), 0o644)

	rel, _ := filepath.Rel(".", file)
	fmt.Printf("📥 %s wrote %d msgs → %s\n", tk, len(posts), rel)
}

func getID(raw json.RawMessage) int {
	var tmp struct {
		ID int `json:"id"`
	}
	_ = json.Unmarshal(raw, &tmp)
	return tmp.ID
}

/*────────────────── state (load / save) ──────────────────*/

func loadState() map[string]*stateRec {
	m := map[string]*stateRec{}
	if b, err := os.ReadFile(stateFile); err == nil {
		_ = json.Unmarshal(b, &m)
	}
	// hydrate in-memory sets
	for _, rec := range m {
		rec.seen = make(map[int]struct{}, len(rec.IdsSeen))
		for _, id := range rec.IdsSeen {
			rec.seen[id] = struct{}{}
		}
	}
	return m
}

// saveState persists a sanitized copy of the in-memory map.
//   - active: the ticker currently being harvested; we do **not**
//     emit its ids_seen slice because Latest/Oldest already gate it.
func saveState(state map[string]*stateRec, active string) {
	out := map[string]*stateRec{}

	for tk, rec := range state {
		// collect ids_seen only for *other* tickers
		var ids []int
		if tk != active {
			ids = make([]int, 0, len(rec.seen))
			for id := range rec.seen {
				ids = append(ids, id)
			}
		}

		out[tk] = &stateRec{
			LatestID: rec.LatestID,
			OldestID: rec.OldestID,
			IdsSeen:  ids, // nil or empty slice omitted thanks to `omitempty`
		}
	}

	_ = os.MkdirAll(filepath.Dir(stateFile), 0o755)
	tmp := stateFile + ".tmp"
	b, _ := json.MarshalIndent(out, "", "  ")
	_ = os.WriteFile(tmp, b, 0o644)
	_ = os.Rename(tmp, stateFile)
}

/*────────────────── back-fill ──────────────────*/

func backfill(ctx context.Context, ticker string, state map[string]*stateRec,
	rot *keyRotator, pause time.Duration) {

	defer func() {
		if r := recover(); r != nil {
			errorf("panic in backfill(%s): %v\n%s", ticker, r, string(debug.Stack()))
		}
	}()

	rec := state[ticker] // pointer to the active-ticker record

	buildPost := func(sym string, id int, raw json.RawMessage) RawPost {
		p := RawPost{
			ID:      strconv.Itoa(id),
			Ticker:  sym,
			Source:  "stocktwits",
			Payload: slimPayload(raw),
		}
		if keepRetrievedAt {
			now := time.Now().UTC()
			p.RetrievedAt = &now
		}
		return p
	}

	handlePage := func(msgs []json.RawMessage) {
		type bucketKey struct{ tk, y, m, d string }
		buckets := map[bucketKey][]RawPost{}

		for _, raw := range msgs {
			id := getID(raw)

			// cheap parse of symbols from slim payload
			var tmp struct {
				Symbols []string `json:"symbols"`
			}
			_ = json.Unmarshal(slimPayload(raw), &tmp)
			if len(tmp.Symbols) == 0 {
				tmp.Symbols = []string{ticker}
			}

			y, m, d := msgDate(raw)

			for _, sym0 := range tmp.Symbols {
				sym := strings.ToUpper(sym0)

				if _, ok := state[sym]; !ok {
					state[sym] = &stateRec{seen: map[int]struct{}{}}
				}
				srec := state[sym]

				// skip duplicates for that ticker
				if _, dup := srec.seen[id]; dup {
					continue
				}
				srec.seen[id] = struct{}{}

				// update Latest / Oldest **only for the active ticker**
				if sym == ticker {
					if rec.LatestID == 0 || id > rec.LatestID {
						rec.LatestID = id
					}
					if rec.OldestID == 0 || id < rec.OldestID {
						rec.OldestID = id
					}
				}

				k := bucketKey{sym, y, m, d}
				buckets[k] = append(buckets[k], buildPost(sym, id, raw))
			}
		}

		for k, list := range buckets {
			writeBatch(k.tk, k.y, k.m, k.d, list)
		}
	}

	/* -------- newer pages -------- */
	if rec.LatestID > 0 {
		for {
			body, code, err := fetch(ctx, rot.Next(),
				"/streams/symbol/"+ticker+".json",
				map[string]string{"since": strconv.Itoa(rec.LatestID)})
			if err != nil || code != 200 {
				errorf("%s fetch failed (code=%d, err=%v)", ticker, code, err)
				time.Sleep(pause) // simple back-off
				continue          // try again instead of hard break
			}

			var r struct {
				Messages []json.RawMessage `json:"messages"`
			}
			_ = json.Unmarshal(body, &r)
			if len(r.Messages) == 0 {
				break
			}
			handlePage(r.Messages)
			saveState(state, ticker) // <<<<<<<<<<
			time.Sleep(pause)
		}
	}

	/* -------- older pages -------- */
	for {
		qs := map[string]string{}
		if rec.OldestID > 0 {
			qs["max"] = strconv.Itoa(rec.OldestID - 1)
		}
		body, code, err := fetch(ctx, rot.Next(),
			"/streams/symbol/"+ticker+".json", qs)
		if err != nil || code != 200 {
			errorf("%s fetch failed (code=%d, err=%v)", ticker, code, err)
			time.Sleep(pause) // simple back-off
			continue          // try again instead of hard break
		}

		var r struct {
			Messages []json.RawMessage `json:"messages"`
		}
		_ = json.Unmarshal(body, &r)
		if len(r.Messages) == 0 {
			break
		}
		handlePage(r.Messages)
		saveState(state, ticker) // <<<<<<<<<<
		time.Sleep(pause)
	}
}

/*────────────────── exported entry-point ──────────────────*/

func StartStockTwitsCollector(ctx context.Context, tickers []string, cfg config.Config) error {
	if len(cfg.RapidAPIKeys) == 0 {
		return fmt.Errorf("no RapidAPI key provided")
	}
	rot := newRotator(cfg.RapidAPIKeys)
	pause := baseDelay / time.Duration(len(cfg.RapidAPIKeys))

	state := loadState()
	for _, tk := range tickers {
		tk = strings.ToUpper(strings.TrimSpace(tk))
		if tk == "" {
			continue
		}
		if _, ok := state[tk]; !ok {
			state[tk] = &stateRec{seen: map[int]struct{}{}}
		}
		backfill(ctx, tk, state, rot, pause)
	}
	return nil
}
