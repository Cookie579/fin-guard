package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"html"
	"regexp"
	"strconv"
)

// ---- Tunables (safe defaults) ----
const (
	// How many distinct channels (max) to crawl via uploads playlists per run.
	maxUploadsChannelsPerRun = 6
	// How many pages of playlistItems to fetch per channel (50 videos/page).
	maxUploadsPagesPerChannel = 1
	// Batch size for checking comment counts via videos.list before harvesting.
	commentCountBatchSize = 50
)

// --- YouTube text cleaner (keeps emojis, strips HTML, normalizes) ---
var brRe = regexp.MustCompile(`(?i)<br\s*/?>`)
var tagRe = regexp.MustCompile(`<[^>]+>`)

func stripZeroWidth(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '\u200B', '\u200C', '\u200D', '\uFEFF':
			return -1
		}
		return r
	}, s)
}

func cleanYTText(s string) string {
	if s == "" {
		return s
	}
	s = brRe.ReplaceAllString(s, "\n")
	s = html.UnescapeString(s)
	s = tagRe.ReplaceAllString(s, "")
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = stripZeroWidth(s)
	s = strings.TrimSpace(s)
	return s
}

// searchQueries returns a slice of search terms for harvesting YouTube videos.
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

// ---------------- Small helpers ----------------

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// fetchVideoCommentCounts returns the commentCount for up to 50 video IDs.
func fetchVideoCommentCounts(ctx context.Context, client *http.Client, apiKey string, ids []string) (map[string]int, error) {
	m := make(map[string]int, len(ids))
	if len(ids) == 0 {
		return m, nil
	}
	q := url.Values{}
	q.Set("part", "statistics")
	q.Set("id", strings.Join(ids, ","))
	q.Set("key", apiKey)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.googleapis.com/youtube/v3/videos?"+q.Encode(), nil)
	req.Header.Set("User-Agent", "ingest-social-go/1.0")

	lease, err := acquirePermit(ctx, "youtube")
	if err != nil {
		return nil, err
	}
	var status int
	var retryAfterMs int
	var body []byte
	resp, doErr := client.Do(req)
	if resp != nil {
		status = resp.StatusCode
		retryAfterMs = parseRetryAfterMs(resp.Header.Get("Retry-After"), time.Now())
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		body = b
	}
	ok := (doErr == nil && status == http.StatusOK)
	reportResult(ctx, lease.ID, ok, status, retryAfterMs)
	if doErr != nil {
		return nil, doErr
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("videos.list HTTP %d: %s", status, string(body))
	}
	var out struct {
		Items []struct {
			ID         string `json:"id"`
			Statistics struct {
				CommentCount string `json:"commentCount"`
                LikeCount    string `json:"likeCount"`
			} `json:"statistics"`
		} `json:"items"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	for _, it := range out.Items {
		n := 0
		if it.Statistics.CommentCount != "" {
			n, _ = strconv.Atoi(it.Statistics.CommentCount)
		}
		m[it.ID] = n
	}
	return m, nil
}

// getUploadsPlaylistIDs maps channelID -> uploadsPlaylistID using channels.list(contentDetails).
func getUploadsPlaylistIDs(ctx context.Context, client *http.Client, apiKey string, channelIDs []string) (map[string]string, error) {
	res := make(map[string]string, len(channelIDs))
	if len(channelIDs) == 0 {
		return res, nil
	}
	for i := 0; i < len(channelIDs); i += 50 {
		end := i + 50
		if end > len(channelIDs) {
			end = len(channelIDs)
		}
		ids := channelIDs[i:end]
		q := url.Values{}
		q.Set("part", "contentDetails")
		q.Set("id", strings.Join(ids, ","))
		q.Set("key", apiKey)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.googleapis.com/youtube/v3/channels?"+q.Encode(), nil)
		req.Header.Set("User-Agent", "ingest-social-go/1.0")

		lease, err := acquirePermit(ctx, "youtube")
		if err != nil {
			return nil, err
		}
		var status int
		var retryAfterMs int
		var body []byte
		resp, doErr := client.Do(req)
		if resp != nil {
			status = resp.StatusCode
			retryAfterMs = parseRetryAfterMs(resp.Header.Get("Retry-After"), time.Now())
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = b
		}
		ok := (doErr == nil && status == http.StatusOK)
		reportResult(ctx, lease.ID, ok, status, retryAfterMs)
		if doErr != nil {
			return nil, doErr
		}
		if status != http.StatusOK {
			return nil, fmt.Errorf("channels.list HTTP %d: %s", status, string(body))
		}
		var out struct {
			Items []struct {
				ID             string `json:"id"`
				ContentDetails struct {
					RelatedPlaylists struct {
						Uploads string `json:"uploads"`
					} `json:"relatedPlaylists"`
				} `json:"contentDetails"`
			} `json:"items"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return nil, err
		}
		for _, it := range out.Items {
			if it.ContentDetails.RelatedPlaylists.Uploads != "" {
				res[it.ID] = it.ContentDetails.RelatedPlaylists.Uploads
			}
		}
	}
	return res, nil
}

// listPlaylistItems yields video entries from a playlist, limited by pages.
func listPlaylistItems(ctx context.Context, client *http.Client, apiKey, playlistID string, publishedAfter int64, maxPages int) ([]struct {
	VideoID      string
	PublishedAt  string
	Title        string
	Description  string
	ChannelID    string
	ChannelTitle string
}, error) {
	var results []struct {
		VideoID      string
		PublishedAt  string
		Title        string
		Description  string
		ChannelID    string
		ChannelTitle string
	}
	if playlistID == "" || maxPages <= 0 {
		return results, nil
	}
	pageToken := ""
	pages := 0
	for {
		q := url.Values{}
		q.Set("part", "contentDetails,snippet")
		q.Set("playlistId", playlistID)
		q.Set("maxResults", "50")
		if pageToken != "" {
			q.Set("pageToken", pageToken)
		}
		// Note: playlistItems.list does not accept publishedAfter filter; we filter client-side.
		q.Set("key", apiKey)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.googleapis.com/youtube/v3/playlistItems?"+q.Encode(), nil)
		req.Header.Set("User-Agent", "ingest-social-go/1.0")

		lease, err := acquirePermit(ctx, "youtube")
		if err != nil {
			return results, err
		}
		var status int
		var retryAfterMs int
		var body []byte
		resp, doErr := client.Do(req)
		if resp != nil {
			status = resp.StatusCode
			retryAfterMs = parseRetryAfterMs(resp.Header.Get("Retry-After"), time.Now())
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			body = b
		}
		ok := (doErr == nil && status == http.StatusOK)
		reportResult(ctx, lease.ID, ok, status, retryAfterMs)
		if doErr != nil {
			return results, doErr
		}
		if status != http.StatusOK {
			return results, fmt.Errorf("playlistItems.list HTTP %d: %s", status, string(body))
		}

		var out struct {
			NextPageToken string `json:"nextPageToken"`
			Items         []struct {
				ContentDetails struct {
					VideoID          string `json:"videoId"`
					VideoPublishedAt string `json:"videoPublishedAt"`
				} `json:"contentDetails"`
				Snippet struct {
					Title        string `json:"title"`
					Description  string `json:"description"`
					ChannelID    string `json:"channelId"`
					ChannelTitle string `json:"channelTitle"`
				} `json:"snippet"`
			} `json:"items"`
		}
		if err := json.Unmarshal(body, &out); err != nil {
			return results, err
		}

		for _, it := range out.Items {
			pub := it.ContentDetails.VideoPublishedAt
			if pub == "" {
				// Fallback: some items may lack contentDetails timestamp; skip cutoff check
			} else if publishedAfter > 0 {
				if t, err := time.Parse(time.RFC3339, pub); err == nil {
					if t.Unix() <= publishedAfter {
						// Too old; skip
						continue
					}
				}
			}
			results = append(results, struct {
				VideoID      string
				PublishedAt  string
				Title        string
				Description  string
				ChannelID    string
				ChannelTitle string
			}{
				VideoID:      it.ContentDetails.VideoID,
				PublishedAt:  it.ContentDetails.VideoPublishedAt,
				Title:        it.Snippet.Title,
				Description:  it.Snippet.Description,
				ChannelID:    it.Snippet.ChannelID,
				ChannelTitle: it.Snippet.ChannelTitle,
			})
		}

		pages++
		if out.NextPageToken == "" || pages >= maxUploadsPagesPerChannel {
			break
		}
		pageToken = out.NextPageToken
		// polite tiny delay
		time.Sleep(50 * time.Millisecond)
	}
	return results, nil
}

// harvestCommentsForVideo fetches commentThreads for a single video ID respecting args.ExtraPages.
func harvestCommentsForVideo(ctx context.Context, client *http.Client, apiKey, source, ticker, ingestDate, jobID, vid string, args *youtubeArgs, pub *KinesisPublisher) error {
	if args.ExtraPages == 0 {
		fmt.Printf("INFO: skipping comments for video %s (extra-pages=0)\n", vid)
		return nil
	}
	commentPage := ""
	pagesLeft := args.ExtraPages
	for {
		if args.ExtraPages >= 0 && pagesLeft == 0 {
			break
		}
		cLease, err := acquirePermit(ctx, "youtube")
		if err != nil {
			return err
		}
		cq := url.Values{}
		cq.Set("videoId", vid)
		cq.Set("part", "snippet")
		cq.Set("maxResults", "100")
		cq.Set("order", "time")
		if commentPage != "" {
			cq.Set("pageToken", commentPage)
		}
		cq.Set("key", apiKey)
		cURL := "https://www.googleapis.com/youtube/v3/commentThreads?" + cq.Encode()
		creq, _ := http.NewRequestWithContext(ctx, http.MethodGet, cURL, nil)
		creq.Header.Set("User-Agent", "ingest-social-go/1.0")
		var cStatus int
		var cRetry int
		var cBody []byte
		cResp, cErr := client.Do(creq)
		if cResp != nil {
			cStatus = cResp.StatusCode
			cRetry = parseRetryAfterMs(cResp.Header.Get("Retry-After"), time.Now())
			b, _ := io.ReadAll(cResp.Body)
			cResp.Body.Close()
			cBody = b
		}
		cOk := (cErr == nil && cStatus == http.StatusOK)
		reportResult(ctx, cLease.ID, cOk, cStatus, cRetry)
		if cErr != nil {
			return cErr
		}
		if cStatus != http.StatusOK {
			break
		}
		var cRespObj struct {
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
		_ = json.Unmarshal(cBody, &cRespObj)
		for _, it := range cRespObj.Items {
			c := it.Snippet.TopLevelComment
			rec := map[string]any{
				"stage":           StageRaw,
				"type":            "youtube_comment",
				"source":          source,
				"ticker":          ticker,
				"ingest_date":     ingestDate,
				"job_id":          jobID,
				"video_id":        vid,
				"comment_id":      c.ID,
				"text":            cleanYTText(c.Snippet.TextOriginal),
				"likes":           c.Snippet.LikeCount,
				"author_username": c.Snippet.AuthorDisplayName,
				"published_at":    c.Snippet.PublishedAt,
			}
			commentRowKey := "com:" + vid + ":" + c.ID
			if err := pub.AddRecord(source, ticker, commentRowKey, rec); err != nil {
				return err
			}
		}
		next := cRespObj.NextPageToken
		if next == "" {
			break
		}
		commentPage = next
		if args.ExtraPages > 0 {
			pagesLeft--
		}
		if args.DelayMS > 0 {
			time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	return nil
}

// ---------------- Main job ----------------

func RunYouTubeJob(ctx context.Context, jobID, ingestDate string, args *youtubeArgs) error {
	if args.Ticker == "" {
		return fmt.Errorf("ticker required")
	}
	ticker := strings.ToUpper(args.Ticker)
	source := "youtube"
	ownerID := jobID

	store, err := NewDynamoStore(ctx)
	if err != nil {
		return fmt.Errorf("create dynamo store: %w", err)
	}
	pub, err := NewKinesisPublisher(ctx, jobID)
	if err != nil {
		return fmt.Errorf("create kinesis publisher: %w", err)
	}
	defer func() { _ = pub.Close(ctx) }()

	emitMeta := func(eventType string, extra map[string]any) {
		rec := map[string]any{
			"stage":  StageEvents,
			"type":   eventType,
			"source": source,
			"ticker": ticker,
			"job_id": jobID,
			"ts":     time.Now().UTC().Format(time.RFC3339),
		}
		for k, v := range extra {
			rec[k] = v
		}
		rowKey := fmt.Sprintf("meta:%s", eventType)
		_ = pub.AddRecord(source, ticker, rowKey, rec)
	}

	emitMeta("job_started", map[string]any{})

	if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			return err
		}
		emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("lock: %v", err)})
		return err
	}
	fmt.Printf("LOCK_ACQUIRED job_id=%s pk=%s#%s\n", jobID, source, ticker)
	defer func() { _ = store.ReleaseLock(context.Background(), source, ticker, ownerID) }()

	hbCtx, hbCancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(time.Duration(LockHeartbeatSeconds) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				_ = store.HeartbeatLock(ctx, source, ticker, ownerID)
			}
		}
	}()
	defer hbCancel()

	apiKey, err := getYouTubeAPIKey()
	if err != nil {
		emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("missing YouTube API key: %v", err)})
		return err
	}

	lastSearchAt, err := store.GetYouTubeState(ctx, ticker)
	if err != nil {
		emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("state load: %v", err)})
		return err
	}
	var cutoff int64
	if lastSearchAt != nil {
		cutoff = lastSearchAt.UTC().Unix()
	}
	var maxTimestamp int64 = cutoff

	processedVideos := 0
	startTime := time.Now()
	client := &http.Client{Timeout: 45 * time.Second}

	seen := make(map[string]struct{})
	terms := searchQueries(args.Ticker)
	done := false

	// comment batching
	pendingCommentIDs := make([]string, 0, commentCountBatchSize)
	pendingSet := make(map[string]struct{})

	// channels discovered this run (to crawl uploads)
	discoveredChannels := make([]string, 0, 32)
	crawledChannel := make(map[string]struct{})

	flushPendingComments := func() error {
		if len(pendingCommentIDs) == 0 {
			return nil
		}
		// Batch-check comment counts
		for i := 0; i < len(pendingCommentIDs); i += commentCountBatchSize {
			batch := pendingCommentIDs[i:minInt(i+commentCountBatchSize, len(pendingCommentIDs))]
			counts, err := fetchVideoCommentCounts(ctx, client, apiKey, batch)
			if err != nil {
				return err
			}
			for _, vid := range batch {
				if counts[vid] == 0 {
					continue // skip zero-comment videos
				}
				if err := harvestCommentsForVideo(ctx, client, apiKey, source, ticker, ingestDate, jobID, vid, args, pub); err != nil {
					return err
				}
				// pacing
				if args.DelayMS > 0 {
					time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
				} else {
					time.Sleep(25 * time.Millisecond)
				}
			}
		}
		// reset
		pendingCommentIDs = pendingCommentIDs[:0]
		pendingSet = make(map[string]struct{})
		return nil
	}

	// Helper to ingest a single video record (from search or uploads crawl).
	ingestVideo := func(vid, publishedAt, title, desc, channelID, channelTitle, searchTerm string) error {
		if vid == "" {
			return nil
		}
		if _, ok := seen[vid]; ok {
			return nil
		}
		// parse publishedAt if available for cutoff & year
		var t time.Time
		if publishedAt != "" {
			if tt, err := time.Parse(time.RFC3339, publishedAt); err == nil {
				t = tt
				if ts := t.Unix(); cutoff > 0 && ts <= cutoff {
					return nil // too old
				}
				if ts := t.Unix(); ts > maxTimestamp {
					maxTimestamp = ts
				}
			}
		}

		seen[vid] = struct{}{}
		year := t.UTC().Year()
		inserted, err := store.InsertYouTubeVideoID(ctx, ticker, year, vid, time.Now().UTC(), &t, channelID, jobID)
		if err != nil {
			return err
		}
		if !inserted {
			return nil // previously processed
		}
		videoRec := map[string]any{
			"stage":         StageRaw,
			"type":          "youtube_video",
			"source":        source,
			"ticker":        ticker,
			"ingest_date":   ingestDate,
			"job_id":        jobID,
			"video_id":      vid,
			"published_at":  publishedAt,
			"title":         title,
			"description":   cleanYTText(desc),
			"channel_id":    channelID,
			"channel_title": channelTitle,
			"search_term":   searchTerm,
		}
		videoRowKey := "vid:" + vid
		if err := pub.AddRecord(source, ticker, videoRowKey, videoRec); err != nil {
			return err
		}

		// queue for comment harvesting (batched)
		if _, exists := pendingSet[vid]; !exists {
			pendingSet[vid] = struct{}{}
			pendingCommentIDs = append(pendingCommentIDs, vid)
		}

		processedVideos++
		return nil
	}

	crawlUploadsForDiscoveredChannels := func() error {
		if len(discoveredChannels) == 0 {
			return nil
		}
		// pick up to maxUploadsChannelsPerRun channels not yet crawled
		toCrawl := make([]string, 0, maxUploadsChannelsPerRun)
		for _, ch := range discoveredChannels {
			if len(toCrawl) >= maxUploadsChannelsPerRun {
				break
			}
			if _, done := crawledChannel[ch]; done {
				continue
			}
			toCrawl = append(toCrawl, ch)
		}
		if len(toCrawl) == 0 {
			return nil
		}
		// resolve uploads playlists
		upMap, err := getUploadsPlaylistIDs(ctx, client, apiKey, toCrawl)
		if err != nil {
			return err
		}
		for _, ch := range toCrawl {
			pl, ok := upMap[ch]
			if !ok || pl == "" {
				crawledChannel[ch] = struct{}{}
				continue
			}
			items, err := listPlaylistItems(ctx, client, apiKey, pl, cutoff, maxUploadsPagesPerChannel)
			if err != nil {
				return err
			}
			for _, it := range items {
				if done {
					break
				}
				// Respect MaxVideos
				maxVideos := args.MaxVideos
				if maxVideos == 0 {
					maxVideos = 10
				}
				if processedVideos >= maxVideos {
					done = true
					break
				}
				_ = ingestVideo(it.VideoID, it.PublishedAt, it.Title, it.Description, it.ChannelID, it.ChannelTitle, "channel_uploads")
				// pacing
				if args.DelayMS > 0 {
					time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
				} else {
					time.Sleep(25 * time.Millisecond)
				}
			}
			crawledChannel[ch] = struct{}{}
			// Flush comments in batches as we go
			if err := flushPendingComments(); err != nil {
				return err
			}
			if done {
				break
			}
		}
		return nil
	}

	// --------- SEARCH LOOP (with early stop & uploads crawl) ---------
	for _, term := range terms {
		if done {
			break
		}
		backfillBefore := ""
		pageToken := ""
		for {
			maxVideos := args.MaxVideos
			if maxVideos == 0 {
				maxVideos = 10
			}
			if processedVideos >= maxVideos {
				done = true
				break
			}
			select {
			case <-ctx.Done():
				emitMeta("job_failed", map[string]any{"error": "terminated"})
				return fmt.Errorf("terminated")
			default:
			}

			lease, err := acquirePermit(ctx, "youtube")
			if err != nil {
				emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("broker acquire: %v", err)})
				return err
			}

			q := url.Values{}
			q.Set("q", term)
			q.Set("part", "id,snippet")
			q.Set("type", "video")
			q.Set("order", "date")
			q.Set("maxResults", "50")
			q.Set("relevanceLanguage", "en")
			if pageToken != "" {
				q.Set("pageToken", pageToken)
			}
			if backfillBefore != "" {
				q.Set("publishedBefore", backfillBefore)
			} else if cutoff > 0 {
				q.Set("publishedAfter", time.Unix(cutoff, 0).UTC().Format(time.RFC3339))
			}
			q.Set("key", apiKey)
			reqURL := "https://www.googleapis.com/youtube/v3/search?" + q.Encode()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
			req.Header.Set("User-Agent", "ingest-social-go/1.0")

			var status int
			var retryAfterMs int
			var body []byte
			resp, doErr := client.Do(req)
			if resp != nil {
				status = resp.StatusCode
				retryAfterMs = parseRetryAfterMs(resp.Header.Get("Retry-After"), time.Now())
				b, _ := io.ReadAll(resp.Body)
				resp.Body.Close()
				body = b
			}
			ok := (doErr == nil && status == http.StatusOK)
			reportResult(ctx, lease.ID, ok, status, retryAfterMs)
			if doErr != nil {
				emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("HTTP error: %v", doErr)})
				return doErr
			}
			if status != http.StatusOK {
				err := fmt.Errorf("HTTP %d: %s", status, string(body))
				emitMeta("job_failed", map[string]any{"error": err.Error()})
				return err
			}

			var searchRes struct {
				NextPageToken string `json:"nextPageToken"`
				Items         []struct {
					ID struct {
						VideoID string `json:"videoId"`
					} `json:"id"`
					Snippet struct {
						PublishedAt  string `json:"publishedAt"`
						Title        string `json:"title"`
						Description  string `json:"description"`
						ChannelID    string `json:"channelId"`
						ChannelTitle string `json:"channelTitle"`
					} `json:"snippet"`
				} `json:"items"`
			}
			if err := json.Unmarshal(body, &searchRes); err != nil {
				emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("unmarshal: %v", err)})
				return err
			}
			if len(searchRes.Items) == 0 {
				break
			}

			pageOlder := false
			oldestSeen := time.Now().UTC()
			for _, item := range searchRes.Items {
				if done {
					break
				}
				maxVideos := args.MaxVideos
				if maxVideos == 0 {
					maxVideos = 10
				}
				if processedVideos >= maxVideos {
					done = true
					break
				}
				vid := item.ID.VideoID
				if vid == "" {
					continue
				}
				// collect channel for uploads crawl
				if item.Snippet.ChannelID != "" {
					discoveredChannels = append(discoveredChannels, item.Snippet.ChannelID)
				}
				// Try ingest
				pubAt := item.Snippet.PublishedAt
				if t, err := time.Parse(time.RFC3339, pubAt); err == nil {
					if t.Before(oldestSeen) { oldestSeen = t }
				}
				if cutoff > 0 {
					if t, err := time.Parse(time.RFC3339, pubAt); err == nil {
						if t.Unix() <= cutoff {
							pageOlder = true
							continue
						}
					}
				}
				if err := ingestVideo(vid, pubAt, item.Snippet.Title, item.Snippet.Description, item.Snippet.ChannelID, item.Snippet.ChannelTitle, term); err != nil {
					emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("ingest: %v", err)})
					return err
				}
			}

			// opportunistic uploads crawl to boost yield (cheap API units)
			if !done {
				if err := crawlUploadsForDiscoveredChannels(); err != nil {
					emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("uploads crawl: %v", err)})
					return err
				}
			}

			if done {
				break
			}
			if pageOlder {
				mv := args.MaxVideos
				if mv == 0 { mv = 10 }
				if processedVideos < mv {
					backfillBefore = oldestSeen.Add(-1 * time.Second).UTC().Format(time.RFC3339)
					pageToken = ""
					pageOlder = false
					continue
				}
				break
			}
			if searchRes.NextPageToken == "" {
				mv := args.MaxVideos
				if mv == 0 { mv = 10 }
				if processedVideos < mv {
					if backfillBefore == "" {
						backfillBefore = oldestSeen.Add(-1 * time.Second).UTC().Format(time.RFC3339)
						pageToken = ""
						continue
					}
				}
				break
			}
			pageToken = searchRes.NextPageToken
			if args.DelayMS > 0 {
				time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
			} else {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	// Final comment flush
	if err := flushPendingComments(); err != nil {
		emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("flush comments: %v", err)})
		return err
	}

	if err := store.UpdateYouTubeState(ctx, ticker); err != nil {
		emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("state update: %v", err)})
		return err
	}
	emitMeta("batch_committed", map[string]any{"records": processedVideos})
	durationMs := time.Since(startTime).Milliseconds()
	emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
	fmt.Printf("JOB_COMPLETED job_id=%s duration_ms=%d\n", jobID, durationMs)
	fmt.Printf("PROCESSED job_id=%s videos=%d\n", jobID, processedVideos)
	return nil
}