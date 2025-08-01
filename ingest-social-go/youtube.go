//go:build youtube
// +build youtube

// ingest-social-go/youtube.go
//
//   TICKER_LIST=AAPL,MSFT go run -tags youtube . -videos 200 -pages -1
//
// Reads google_api_keys (or google_api_key) from ../config.json

package main

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/Cookie579/fin-guard/config"
)

/* ────────────────────────────── CLI flags ────────────────────────────── */

var (
	wantVideos = flag.Int("videos", 50, "max videos per ticker run")
	pagesFlag  = flag.Int("pages", 0, "extra 100-comment pages (0 = none, –1 = all)")
	delayMS    = flag.Int("delay", 100, "ms between YouTube API calls / retries")
)

/* ────────────────────────────── helpers ────────────────────────────── */

func searchQueries(tk string) []string {
	return []string{
		fmt.Sprintf("$%s stock", tk),
		fmt.Sprintf("%s earnings", tk),
		fmt.Sprintf("%s stock analysis", tk),
		fmt.Sprintf("%s price prediction", tk),
		fmt.Sprintf("%s news", tk),
		fmt.Sprintf("%s stock update", tk),
		fmt.Sprintf("%s investor day", tk),
		fmt.Sprintf("%s quarterly results", tk),
		fmt.Sprintf("%s stock crash", tk),
		fmt.Sprintf("%s vs", tk),
	}
}

/* ───────────────────────── YouTube API models ───────────────────────── */

type searchResp struct {
	NextPageToken string `json:"nextPageToken"`
	Items         []struct {
		ID struct {
			VideoID string `json:"videoId"`
		} `json:"id"`
	} `json:"items"`
}

type threadResp struct {
	NextPageToken string `json:"nextPageToken"`
	Items         []struct {
		Snippet struct {
			TopLevelComment struct {
				ID      string `json:"id"`
				Snippet struct {
					TextOriginal      string `json:"textOriginal"`
					AuthorDisplayName string `json:"authorDisplayName"`
					LikeCount         int    `json:"likeCount"`
					PublishedAt       string `json:"publishedAt"`
				} `json:"snippet"`
			} `json:"topLevelComment"`
		} `json:"snippet"`
	} `json:"items"`
}

type videosResp struct {
	Items []struct {
		ID      string `json:"id"`
		Snippet struct {
			Title        string `json:"title"`
			ChannelID    string `json:"channelId"`
			ChannelTitle string `json:"channelTitle"`
			PublishedAt  string `json:"publishedAt"`
		} `json:"snippet"`
		Statistics struct {
			ViewCount    string `json:"viewCount"`
			LikeCount    string `json:"likeCount"`
			CommentCount string `json:"commentCount"`
		} `json:"statistics"`
		ContentDetails struct {
			Duration string `json:"duration"`
		} `json:"contentDetails"`
	} `json:"items"`
}

type videoMeta struct {
	VideoID      string    `json:"video_id"`
	Title        string    `json:"title"`
	ChannelID    string    `json:"channel_id"`
	ChannelTitle string    `json:"channel_title"`
	PublishedAt  string    `json:"published_at"`
	Duration     string    `json:"duration"`
	ViewCount    int64     `json:"view_count"`
	LikeCount    int64     `json:"like_count"`
	CommentCount int64     `json:"comment_count"`
	RetrievedAt  time.Time `json:"retrieved_at"`
	Ticker       string    `json:"ticker"`
}

/* ───────────────────────── constants & vars ───────────────────────── */

const (
	baseURL       = "https://youtube.googleapis.com/youtube/v3"
	ua            = "Mozilla/5.0 (compatible; fin-guard-bot/1.0)"
	ytStateFile   = "data/youtube_state.json"
	timeLayoutISO = "2006-01-02T15:04:05Z"
)

/* ─────────────────────── quota-aware fetch logic ────────────────────── */

func isQuota403(body []byte) bool {
	var e struct {
		Error struct {
			Code   int `json:"code"`
			Errors []struct {
				Reason string `json:"reason"`
			} `json:"errors"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) != nil || e.Error.Code != 403 {
		return false
	}
	for _, er := range e.Error.Errors {
		r := strings.ToLower(er.Reason)
		if strings.Contains(r, "quota") ||
			strings.Contains(r, "ratelimit") ||
			strings.Contains(r, "dailylimit") {
			return true
		}
	}
	return false
}

func getJSONWithRotation(
	ctx context.Context,
	cli *http.Client,
	keys []string,
	buildURL func(k string) string,
	dst any,
	delay time.Duration,
) error {
	for i := 0; i < len(keys); i++ {
		u := buildURL(keys[i])

		req, _ := http.NewRequestWithContext(ctx, "GET", u, nil)
		req.Header.Set("User-Agent", ua)

		resp, err := cli.Do(req)
		if err != nil {
			return err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()

		if resp.StatusCode == 200 {
			return json.Unmarshal(body, dst)
		}
		if resp.StatusCode == 403 && isQuota403(body) {
			time.Sleep(delay)
			continue // try next key
		}
		return fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return fmt.Errorf("all API keys exhausted due to quota")
}

/* ──────────────── persistent per-ticker “seen” state ────────────────
   JSON on disk: { "AAPL": ["id1","id2"], "MSFT": ["idX", ...] }
*/

func loadYTState() map[string]map[string]struct{} {
	out := map[string]map[string]struct{}{}

	b, err := os.ReadFile(ytStateFile)
	if err != nil {
		return out // new install
	}

	var tmp map[string][]string
	if json.Unmarshal(b, &tmp) != nil {
		return out // corrupt → start fresh
	}

	for tk, arr := range tmp {
		set := make(map[string]struct{}, len(arr))
		for _, id := range arr {
			set[id] = struct{}{}
		}
		out[tk] = set
	}
	return out
}

func saveYTState(st map[string]map[string]struct{}) {
	out := map[string][]string{}
	for tk, set := range st {
		for id := range set {
			out[tk] = append(out[tk], id)
		}
	}

	_ = os.MkdirAll(filepath.Dir(ytStateFile), 0o755)
	b, _ := json.MarshalIndent(out, "", "  ")
	_ = os.WriteFile(ytStateFile+".tmp", b, 0o644)
	_ = os.Rename(ytStateFile+".tmp", ytStateFile)
}

/* ───────────────────── duplicate-text helper ───────────────────── */

type dup struct{ seen map[string]struct{} }

func newDup() *dup { return &dup{seen: map[string]struct{}{}} }
func (d *dup) fresh(s string) bool {
	h := sha1.Sum([]byte(s))
	k := hex.EncodeToString(h[:])
	if _, ok := d.seen[k]; ok {
		return false
	}
	d.seen[k] = struct{}{}
	return true
}

/* ───────────────────────── video search ───────────────────────── */

func collectVideos(
	ctx context.Context,
	cli *http.Client,
	keys []string,
	tk string,
	want int,
	delay time.Duration,
	seen map[string]struct{},
) []string {

	dedup := map[string]struct{}{}
	var vids []string

	for _, term := range searchQueries(tk) {
		page := ""
		for len(vids) < want {
			q := url.Values{
				"part":       {"id"},
				"type":       {"video"},
				"order":      {"date"},
				"maxResults": {"50"},
				"q":          {term},
			}
			if page != "" {
				q.Set("pageToken", page)
			}

			var sr searchResp
			err := getJSONWithRotation(ctx, cli, keys, func(k string) string {
				q.Set("key", k)
				return baseURL + "/search?" + q.Encode()
			}, &sr, delay)
			if err != nil {
				fmt.Println("search err:", err)
				break
			}

			for _, it := range sr.Items {
				vid := it.ID.VideoID
				if vid == "" {
					continue
				}
				if _, dup := dedup[vid]; dup {
					continue
				}
				if _, done := seen[vid]; done {
					continue // already harvested for *this* ticker
				}
				dedup[vid] = struct{}{}
				vids = append(vids, vid)
				if len(vids) >= want {
					break
				}
			}

			if sr.NextPageToken == "" {
				break
			}
			page = sr.NextPageToken
			time.Sleep(delay)
		}
		if len(vids) >= want {
			break
		}
	}
	fmt.Printf("  • %d fresh videos\n", len(vids))
	return vids
}

/* ─────────────────────── file-layout helpers ─────────────────────── */

func chooseVideoFileName(dir, tk, y, m, d string) string {
	prefix := fmt.Sprintf("YouTube_%s_%s_%s_%s_", tk, y, m, d)
	n := 0
	files, _ := os.ReadDir(dir)
	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix) && strings.HasSuffix(f.Name(), "V.json") {
			n++
		}
	}
	return prefix + strconv.Itoa(n+1) + "V.json"
}

/* ─────────────────────── dump helpers ─────────────────────── */

func dumpCommentsAndMeta(tk string, comments []map[string]any, meta *videoMeta) error {
	pub, _ := time.Parse(timeLayoutISO, meta.PublishedAt)
	y, m, d := pub.Format("2006"), pub.Format("01"), pub.Format("02")

	dir := filepath.Join("data", "raw", "youtube", tk, "videos", y)
	_ = os.MkdirAll(dir, 0o755)

	commentsFile := filepath.Join(dir, chooseVideoFileName(dir, tk, y, m, d))

	// comments
	fc, err := os.Create(commentsFile)
	if err != nil {
		return err
	}
	w := bufio.NewWriter(fc)
	for _, row := range comments {
		_ = json.NewEncoder(w).Encode(row)
	}
	w.Flush()
	_ = fc.Close()

	// metadata
	metaFile := strings.TrimSuffix(commentsFile, ".json") + ".metadata.json"
	fm, err := os.Create(metaFile)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(fm)
	enc.SetIndent("", "  ")
	_ = enc.Encode(meta)
	_ = fm.Close()

	fmt.Printf("    ↳ stored %d comments → %s\n", len(comments), commentsFile)
	return nil
}

/* ─────────────────────── video harvest ─────────────────────── */

func fetchVideoMeta(ctx context.Context, cli *http.Client, keys []string, vid, tk string, delay time.Duration) (*videoMeta, error) {
	var vr videosResp
	err := getJSONWithRotation(ctx, cli, keys, func(k string) string {
		q := url.Values{
			"part": {"snippet,statistics,contentDetails"},
			"id":   {vid},
			"key":  {k},
		}
		return baseURL + "/videos?" + q.Encode()
	}, &vr, delay)
	if err != nil {
		return nil, err
	}
	if len(vr.Items) == 0 {
		return nil, fmt.Errorf("no meta for %s", vid)
	}
	it := vr.Items[0]
	parseI64 := func(s string) int64 { n, _ := strconv.ParseInt(s, 10, 64); return n }
	return &videoMeta{
		VideoID:      it.ID,
		Title:        it.Snippet.Title,
		ChannelID:    it.Snippet.ChannelID,
		ChannelTitle: it.Snippet.ChannelTitle,
		PublishedAt:  it.Snippet.PublishedAt,
		Duration:     it.ContentDetails.Duration,
		ViewCount:    parseI64(it.Statistics.ViewCount),
		LikeCount:    parseI64(it.Statistics.LikeCount),
		CommentCount: parseI64(it.Statistics.CommentCount),
		RetrievedAt:  time.Now().UTC(),
		Ticker:       tk,
	}, nil
}

func harvest(ctx context.Context, cli *http.Client, keys []string, tk, vid string, extraPages int, delay time.Duration) bool {
	meta, err := fetchVideoMeta(ctx, cli, keys, vid, tk, delay)
	if err != nil || meta.CommentCount == 0 {
		return false
	}

	page, tok := 0, ""
	dups := newDup()
	var comments []map[string]any

	for {
		var tr threadResp
		err := getJSONWithRotation(ctx, cli, keys, func(k string) string {
			q := url.Values{
				"part":       {"snippet"},
				"videoId":    {vid},
				"maxResults": {"100"},
				"key":        {k},
			}
			if tok != "" {
				q.Set("pageToken", tok)
			}
			return baseURL + "/commentThreads?" + q.Encode()
		}, &tr, delay)
		if err != nil {
			fmt.Println("comment err:", err)
			return false
		}

		for _, it := range tr.Items {
			s := it.Snippet.TopLevelComment.Snippet
			if !dups.fresh(s.TextOriginal) {
				continue
			}
			comments = append(comments, map[string]any{
				"id":        it.Snippet.TopLevelComment.ID,
				"text":      s.TextOriginal,
				"author":    s.AuthorDisplayName,
				"likes":     s.LikeCount,
				"published": s.PublishedAt,
				"ticker":    tk,
				"video":     vid,
				"retrieved": time.Now().UTC().Format(timeLayoutISO),
			})
		}

		if tr.NextPageToken == "" || (extraPages >= 0 && page >= extraPages) {
			break
		}
		tok = tr.NextPageToken
		page++
		time.Sleep(delay)
	}

	if len(comments) == 0 {
		return false
	}
	if dumpCommentsAndMeta(tk, comments, meta) != nil {
		return false
	}
	return true
}

/* ─────────────────────────────── main ─────────────────────────────── */

func main() {
	flag.Parse()

	cfg, err := config.Load("../config.json")
	if err != nil {
		fmt.Println("config:", err)
		return
	}

	keys := cfg.GoogleAPIKeys
	if len(keys) == 0 && strings.TrimSpace(cfg.GoogleAPIKey) != "" {
		keys = []string{strings.TrimSpace(cfg.GoogleAPIKey)}
	}
	if len(keys) == 0 {
		fmt.Println("no google api keys configured")
		return
	}

	raw := strings.ToUpper(os.Getenv("TICKER_LIST"))
	if raw == "" {
		raw = "PLTR"
	}
	tickers := strings.Split(raw, ",")

	cli := &http.Client{Timeout: 20 * time.Second}
	ctx := context.Background()
	delay := time.Duration(*delayMS) * time.Millisecond

	state := loadYTState()
	defer saveYTState(state)

	for _, tk := range tickers {
		tk = strings.TrimSpace(tk)
		if tk == "" {
			continue
		}
		fmt.Println("▶", tk)

		if state[tk] == nil {
			state[tk] = map[string]struct{}{}
		}

		for _, vid := range collectVideos(ctx, cli, keys, tk, *wantVideos, delay, state[tk]) {

			state[tk][vid] = struct{}{}
			saveYTState(state) // incremental flush

			if harvest(ctx, cli, keys, tk, vid, *pagesFlag, delay) {
				state[tk][vid] = struct{}{}
				saveYTState(state) // incremental save
			}
		}
	}
}
