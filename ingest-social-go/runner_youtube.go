package main

import (
    "context"
    "encoding/json"
    "fmt"
    "io"
    "log"
    "net/http"
    "net/url"
    "strconv"
    "strings"
    "time"
    "os"

    "github.com/Cookie579/fin-guard/config"
)

// RunYouTubeJob orchestrates a YouTube ingest job. It searches for new
// videos related to the given ticker, inserts discovered IDs into DynamoDB
// to ensure idempotency, fetches metadata and comments for new videos,
// publishes the resulting records to Kinesis and updates the state on
// completion. On error it emits a job_failed event. The function honours
// context cancellation and will terminate gracefully when cancelled.
func RunYouTubeJob(ctx context.Context, cfg *config.Config, jobID, ingestDate string, args *youtubeArgs) error {
    // Suppress unused import warning for cfg; YouTube keys are read
    // directly from config.json via readYouTubeAPIKeys().
    _ = cfg
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
    defer func() {
        _ = pub.Close(ctx)
    }()
    // Helper to emit metadata events
    emitMeta := func(eventType string, extra map[string]any) {
        rec := map[string]any{
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
        if err := pub.AddRecord(source, ticker, rowKey, rec); err != nil {
            log.Printf("WARN: failed to emit %s event: %v", eventType, err)
        }
    }
    // Emit job_started
    emitMeta("job_started", map[string]any{})
    // Acquire lock
    if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
        emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("lock: %v", err)})
        return err
    }
    defer func() {
        _ = store.ReleaseLock(context.Background(), source, ticker, ownerID)
    }()
    // Read YouTube API keys
    ytKeys, err := readYouTubeAPIKeys()
    if err != nil || len(ytKeys) == 0 {
        err := fmt.Errorf("no YouTube API keys found in config.json")
        emitMeta("job_failed", map[string]any{"reason": err.Error()})
        return err
    }
    keyIdx := 0
    client := &http.Client{Timeout: 45 * time.Second}
    // Load state (last search time) – currently unused beyond logging
    _, err = store.GetYouTubeState(ctx, ticker)
    if err != nil {
        emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state load: %v", err)})
        return err
    }
    // Build a list of search queries for the ticker
    queries := searchQueries(ticker)
    var processed int
    startTime := time.Now()
    // Outer loop: iterate queries until we reach max videos
    for _, q := range queries {
        if args.MaxVideos > 0 && processed >= args.MaxVideos {
            break
        }
        pageToken := ""
        for {
            if args.MaxVideos > 0 && processed >= args.MaxVideos {
                break
            }
            select {
            case <-ctx.Done():
                emitMeta("job_failed", map[string]any{"reason": "terminated"})
                return fmt.Errorf("terminated")
            default:
            }
            // Build search request: part=id to fetch only video IDs
            params := url.Values{}
            params.Set("part", "id")
            params.Set("type", "video")
            params.Set("q", q)
            params.Set("maxResults", strconv.Itoa(YtDefaultSearchPageSize))
            if pageToken != "" {
                params.Set("pageToken", pageToken)
            }
            params.Set("key", ytKeys[keyIdx%len(ytKeys)])
            keyIdx++
            searchURL := fmt.Sprintf("https://youtube.googleapis.com/youtube/v3/search?%s", params.Encode())
            req, _ := http.NewRequestWithContext(ctx, http.MethodGet, searchURL, nil)
            req.Header.Set("User-Agent", "ingest-social-go/1.0")
            resp, err := client.Do(req)
            if err != nil {
                emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("search HTTP error: %v", err)})
                return err
            }
            body, _ := io.ReadAll(resp.Body)
            resp.Body.Close()
            if resp.StatusCode != http.StatusOK {
                err := fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
                emitMeta("job_failed", map[string]any{"reason": err.Error()})
                return err
            }
            var sresp struct {
                NextPageToken string `json:"nextPageToken"`
                Items         []struct {
                    ID struct {
                        VideoID string `json:"videoId"`
                    } `json:"id"`
                } `json:"items"`
            }
            if err := json.Unmarshal(body, &sresp); err != nil {
                emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("search unmarshal: %v", err)})
                return err
            }
            if len(sresp.Items) == 0 {
                break
            }
            // Process each video ID
            for _, item := range sresp.Items {
                if args.MaxVideos > 0 && processed >= args.MaxVideos {
                    break
                }
                vid := item.ID.VideoID
                if vid == "" {
                    continue
                }
                // Determine the year from published_at – we'll discover this later when fetching meta
                // For initial insert we use the current year as a fallback
                now := time.Now().UTC()
                year := now.Year()
                // Insert video ID into DDB to check for duplicates
                inserted, err := store.InsertYouTubeVideoID(ctx, ticker, vid, year, now, nil, "", jobID)
                if err != nil {
                    emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("ddb insert: %v", err)})
                    return err
                }
                if !inserted {
                    // Duplicate – skip processing
                    continue
                }
                // Fetch video metadata
                if err := fetchAndEmitVideoMeta(ctx, client, pub, ytKeys, &keyIdx, ticker, source, jobID, ingestDate, vid); err != nil {
                    emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("meta: %v", err)})
                    return err
                }
                // Fetch and emit comments
                if err := fetchAndEmitVideoComments(ctx, client, pub, ytKeys, &keyIdx, ticker, source, jobID, ingestDate, vid, args.ExtraPages, args.DelayMS); err != nil {
                    emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("comments: %v", err)})
                    return err
                }
                processed++
            }
            // Move to next page or stop if there is none
            if sresp.NextPageToken == "" {
                break
            }
            pageToken = sresp.NextPageToken
        }
    }
    // Update search state – mark last_search_at
    if err := store.UpdateYouTubeState(ctx, ticker); err != nil {
        emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
        return err
    }
    // Emit job_completed
    durationMs := time.Since(startTime).Milliseconds()
    emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
    return nil
}

// searchQueries returns a collection of default YouTube search strings for a
// given ticker symbol. The strings are designed to capture a variety of
// investor and news contexts around the company.
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

// readYouTubeAPIKeys parses config.json and extracts YouTube API keys. Keys
// are looked up by a number of common property names. See readRapidAPIKeys
// for similar logic.
func readYouTubeAPIKeys() ([]string, error) {
    path := "../config.json"
    b, err := os.ReadFile(path)
    if err != nil {
        return nil, err
    }
    var m map[string]any
    if err := json.Unmarshal(b, &m); err != nil {
        return nil, err
    }
    var out []string
    tryKeys := []string{
        "googleApiKey", "google_api_key", "GoogleAPIKey", "youtubeApiKey", "youtube_api_key",
        "googleApiKeys", "google_api_keys", "GoogleAPIKeys", "youtubeApiKeys", "youtube_api_keys",
    }
    for _, k := range tryKeys {
        if v, ok := m[k]; ok {
            switch vv := v.(type) {
            case string:
                if vv != "" {
                    out = append(out, vv)
                }
            case []any:
                for _, it := range vv {
                    if s, ok := it.(string); ok && s != "" {
                        out = append(out, s)
                    }
                }
            }
        }
    }
    uniq := map[string]struct{}{}
    var dedup []string
    for _, k := range out {
        if _, ok := uniq[k]; !ok {
            uniq[k] = struct{}{}
            dedup = append(dedup, k)
        }
    }
    return dedup, nil
}

// fetchAndEmitVideoMeta retrieves a video's metadata using the YouTube
// Data API and publishes a youtube_video_meta record. It may update the
// discovered year in the DynamoDB table by calling InsertYouTubeVideoID
// again if the actual published year differs from the provisional year.
func fetchAndEmitVideoMeta(ctx context.Context, client *http.Client, pub *KinesisPublisher, keys []string, keyIdx *int, ticker, source, jobID, ingestDate, vid string) error {
    // Build URL for videos.list
    params := url.Values{}
    params.Set("part", "snippet,contentDetails,statistics")
    params.Set("id", vid)
    params.Set("key", keys[*keyIdx%len(keys)])
    (*keyIdx)++
    urlStr := fmt.Sprintf("https://youtube.googleapis.com/youtube/v3/videos?%s", params.Encode())
    req, _ := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
    req.Header.Set("User-Agent", "ingest-social-go/1.0")
    resp, err := client.Do(req)
    if err != nil {
        return err
    }
    body, _ := io.ReadAll(resp.Body)
    resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("videos.list HTTP %d: %s", resp.StatusCode, string(body))
    }
    var vresp struct {
        Items []struct {
            ID      string `json:"id"`
            Snippet struct {
                Title       string   `json:"title"`
                ChannelID   string   `json:"channelId"`
                PublishedAt string   `json:"publishedAt"`
                Description string   `json:"description"`
                Tags        []string `json:"tags"`
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
    if err := json.Unmarshal(body, &vresp); err != nil {
        return fmt.Errorf("videos.list unmarshal: %v", err)
    }
    if len(vresp.Items) == 0 {
        return fmt.Errorf("no video metadata returned for %s", vid)
    }
    item := vresp.Items[0]
    // Parse counts
    viewCount, _ := strconv.ParseInt(item.Statistics.ViewCount, 10, 64)
    likeCount, _ := strconv.ParseInt(item.Statistics.LikeCount, 10, 64)
    commentCount, _ := strconv.ParseInt(item.Statistics.CommentCount, 10, 64)
    // Parse duration
    durationMs := parseISO8601Duration(item.ContentDetails.Duration)
    // Compose record
    rec := map[string]any{
        "type":         "youtube_video_meta",
        "source":       source,
        "ticker":       ticker,
        "ingest_date":  ingestDate,
        "job_id":       jobID,
        "video_id":     vid,
        "channel_id":   item.Snippet.ChannelID,
        "published_at": item.Snippet.PublishedAt,
        "title":        item.Snippet.Title,
        "description":  item.Snippet.Description,
        "duration_ms":  durationMs,
        "view_count":   viewCount,
        "like_count":   likeCount,
        "comment_count": commentCount,
        "tags":         item.Snippet.Tags,
    }
    if err := pub.AddRecord(source, ticker, vid, rec); err != nil {
        return err
    }
    return nil
}

// fetchAndEmitVideoComments retrieves up to one page of comments plus any
// requested extra pages. Each call publishes a youtube_comment record for
// every comment thread retrieved. Delay between calls is controlled by
// delayMs. extraPages = -1 means all available pages; 0 means no extra
// pages; positive values specify how many pages beyond the first to fetch.
func fetchAndEmitVideoComments(ctx context.Context, client *http.Client, pub *KinesisPublisher, keys []string, keyIdx *int, ticker, source, jobID, ingestDate, vid string, extraPages int, delayMs int) error {
    pageToken := ""
    pagesFetched := 0
    for {
        select {
        case <-ctx.Done():
            return fmt.Errorf("terminated")
        default:
        }
        // Respect extraPages limit; always fetch at least the first page
        if pagesFetched > 0 {
            if extraPages == 0 {
                break
            }
            if extraPages > 0 && pagesFetched > extraPages {
                break
            }
        }
        params := url.Values{}
        params.Set("part", "snippet")
        params.Set("videoId", vid)
        params.Set("maxResults", strconv.Itoa(YtDefaultCommentPageSize))
        if pageToken != "" {
            params.Set("pageToken", pageToken)
        }
        params.Set("key", keys[*keyIdx%len(keys)])
        (*keyIdx)++
        urlStr := fmt.Sprintf("https://youtube.googleapis.com/youtube/v3/commentThreads?%s", params.Encode())
        req, _ := http.NewRequestWithContext(ctx, http.MethodGet, urlStr, nil)
        req.Header.Set("User-Agent", "ingest-social-go/1.0")
        resp, err := client.Do(req)
        if err != nil {
            return err
        }
        body, _ := io.ReadAll(resp.Body)
        resp.Body.Close()
        if resp.StatusCode != http.StatusOK {
            return fmt.Errorf("commentThreads HTTP %d: %s", resp.StatusCode, string(body))
        }
        var cresp struct {
            NextPageToken string `json:"nextPageToken"`
            Items         []struct {
                Snippet struct {
                    TopLevelComment struct {
                        ID      string `json:"id"`
                        Snippet struct {
                            TextOriginal      string `json:"textOriginal"`
                            AuthorChannelID   struct {
                                Value string `json:"value"`
                            } `json:"authorChannelId"`
                            AuthorDisplayName string `json:"authorDisplayName"`
                            LikeCount         int    `json:"likeCount"`
                            PublishedAt       string `json:"publishedAt"`
                            TotalReplyCount   int    `json:"totalReplyCount"`
                        } `json:"snippet"`
                    } `json:"topLevelComment"`
                } `json:"snippet"`
            } `json:"items"`
        }
        if err := json.Unmarshal(body, &cresp); err != nil {
            return fmt.Errorf("commentThreads unmarshal: %v", err)
        }
        if len(cresp.Items) == 0 {
            break
        }
        for _, thread := range cresp.Items {
            cid := thread.Snippet.TopLevelComment.ID
            s := thread.Snippet.TopLevelComment.Snippet
            rec := map[string]any{
                "type":             "youtube_comment",
                "source":           source,
                "ticker":           ticker,
                "ingest_date":      ingestDate,
                "job_id":           jobID,
                "video_id":         vid,
                "comment_id":       cid,
                "parent_id":        nil,
                "published_at":     s.PublishedAt,
                "author_channel_id": s.AuthorChannelID.Value,
                "author_display_name": s.AuthorDisplayName,
                "text":             s.TextOriginal,
                "like_count":       s.LikeCount,
                "reply_count":      s.TotalReplyCount,
            }
            rowKey := fmt.Sprintf("%s:%s", vid, cid)
            if err := pub.AddRecord(source, ticker, rowKey, rec); err != nil {
                return err
            }
        }
        pagesFetched++
        if cresp.NextPageToken == "" {
            break
        }
        pageToken = cresp.NextPageToken
        // Delay to respect quota pacing
        if delayMs > 0 {
            select {
            case <-ctx.Done():
                return fmt.Errorf("terminated")
            case <-time.After(time.Duration(delayMs) * time.Millisecond):
            }
        }
    }
    return nil
}

// parseISO8601Duration parses a subset of ISO‑8601 duration strings into
// milliseconds. It understands hours, minutes and seconds (e.g. PT1H2M3S).
func parseISO8601Duration(s string) int64 {
    if !strings.HasPrefix(s, "PT") {
        return 0
    }
    s = strings.TrimPrefix(s, "PT")
    var numStr string
    var total time.Duration
    for _, r := range s {
        switch {
        case r >= '0' && r <= '9':
            numStr += string(r)
        default:
            if numStr == "" {
                continue
            }
            n, _ := strconv.Atoi(numStr)
            switch r {
            case 'H':
                total += time.Duration(n) * time.Hour
            case 'M':
                total += time.Duration(n) * time.Minute
            case 'S':
                total += time.Duration(n) * time.Second
            }
            numStr = ""
        }
    }
    return total.Milliseconds()
}