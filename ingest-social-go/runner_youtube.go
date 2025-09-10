package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "io"
    "net/http"
    "net/url"
    "time"
    "strings"

    // strconv is imported for completeness but unused in this file
    _ "strconv"
)

// RunYouTubeJob orchestrates a YouTube ingest job using the RapidAPI YouTube
// v3 Lite proxy. It collects video metadata for a given channel ID and
// publishes RAW records to Kinesis. The state table is used to track the
// latest and oldest publishedAt timestamps in Unix seconds. Duplicate
// prevention is handled via InsertYouTubeVideoID. It respects context
// cancellation and job-level limits.
func RunYouTubeJob(ctx context.Context, jobID, ingestDate string, args *youtubeArgs) error {
    // Channel ID is taken from args.Ticker. Do not uppercase as YouTube IDs are case sensitive.
    channelID := args.Ticker
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

    // Metadata emitter for events
    emitMeta := func(eventType string, extra map[string]any) {
        rec := map[string]any{
            "stage":  StageEvents,
            "type":   eventType,
            "source": source,
            "ticker": channelID,
            "job_id": jobID,
            "ts":     time.Now().UTC().Format(time.RFC3339),
        }
        for k, v := range extra {
            rec[k] = v
        }
        rowKey := fmt.Sprintf("meta:%s", eventType)
        _ = pub.AddRecord(source, channelID, rowKey, rec)
    }

    emitMeta("job_started", map[string]any{})

    // Acquire lock
    if err := store.AcquireLock(ctx, source, channelID, ownerID, jobID); err != nil {
        if errors.Is(err, ErrLockNotAcquired) {
            return err
        }
        emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("lock: %v", err)})
        return err
    }
    fmt.Printf("LOCK_ACQUIRED job_id=%s pk=%s#%s\n", jobID, source, strings.ToUpper(channelID))
    defer func() { _ = store.ReleaseLock(context.Background(), source, channelID, ownerID) }()

    // Start heartbeat
    hbCtx, hbCancel := context.WithCancel(ctx)
    go func() {
        t := time.NewTicker(time.Duration(LockHeartbeatSeconds) * time.Second)
        defer t.Stop()
        for {
            select {
            case <-hbCtx.Done():
                return
            case <-t.C:
                _ = store.HeartbeatLock(ctx, source, channelID, ownerID)
            }
        }
    }()
    defer hbCancel()

    // Load existing state (last search time). The DynamoStore.GetYouTubeState
    // returns a pointer to the last_search_at timestamp (if any) and an error.
    // We convert this to a Unix seconds cutoff for de-duplication. The
    // GetYouTubeState signature only returns two values: (*time.Time, error).
    lastSearchAt, err := store.GetYouTubeState(ctx, channelID)
    if err != nil {
        emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state load: %v", err)})
        return err
    }
    var latestID int64
    // If we have a last_search_at value, use it as our cutoff in Unix seconds.
    if lastSearchAt != nil {
        latestID = lastSearchAt.UTC().Unix()
    }
    // We still track the oldestID (oldest timestamp seen) to aid in potential backfill logic.
    var oldestID int64
    processed := 0
    startTime := time.Now()
    client := &http.Client{Timeout: 45 * time.Second}
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

        // Acquire RapidAPI permit for YouTube
        lease, err := acquirePermit(ctx, "youtube")
        if err != nil {
            emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("broker acquire: %v", err)})
            return err
        }

        // Build request
        q := url.Values{}
        q.Set("channelId", channelID)
        q.Set("part", "id,snippet")
        if pageToken != "" {
            q.Set("pageToken", pageToken)
        }
        reqURL := "https://" + lease.Host + "/search?" + q.Encode()
        req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
        req.Header.Set("X-RapidAPI-Host", lease.Host)
        req.Header.Set("X-RapidAPI-Key", lease.Key)
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
            emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", doErr)})
            return doErr
        }
        if status != http.StatusOK {
            err := fmt.Errorf("HTTP %d: %s", status, string(body))
            emitMeta("job_failed", map[string]any{"reason": err.Error()})
            return err
        }

        // Parse response
        var parsed struct {
            NextPageToken string `json:"nextPageToken"`
            Items         []struct {
                ID struct {
                    VideoID string `json:"videoId"`
                } `json:"id"`
                Snippet struct {
                    PublishedAt string `json:"publishedAt"`
                    Title       string `json:"title"`
                    Description string `json:"description"`
                    ChannelID   string `json:"channelId"`
                    ChannelTitle string `json:"channelTitle"`
                } `json:"snippet"`
            } `json:"items"`
        }
        if err := json.Unmarshal(body, &parsed); err != nil {
            emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("unmarshal: %v", err)})
            return err
        }
        if len(parsed.Items) == 0 {
            break
        }

        // Track min timestamp this page
        var minTsThisPage int64 = 0

        for _, item := range parsed.Items {
            if args.MaxVideos > 0 && processed >= args.MaxVideos {
                break
            }
            vid := item.ID.VideoID
            if vid == "" {
                continue
            }
            // parse publishedAt
            t, err := time.Parse(time.RFC3339, item.Snippet.PublishedAt)
            if err != nil {
                continue
            }
            ts := t.Unix()
            if latestID != 0 && ts <= latestID {
                // already processed or older; skip
                continue
            }
            if minTsThisPage == 0 || ts < minTsThisPage {
                minTsThisPage = ts
            }
            // Insert video ID record to dedup
            year := t.UTC().Year()
            _, err = store.InsertYouTubeVideoID(ctx, channelID, year, vid, time.Now().UTC(), &t, item.Snippet.ChannelID, jobID)
            if err != nil {
                emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("ddb insert: %v", err)})
                return err
            }
            // Build raw record
            rec := map[string]any{
                "stage":         StageRaw,
                "type":          "youtube_item",
                "source":        source,
                "ticker":        channelID,
                "ingest_date":   ingestDate,
                "job_id":        jobID,
                "video_id":      vid,
                "published_at":  item.Snippet.PublishedAt,
                "title":         item.Snippet.Title,
                "description":   item.Snippet.Description,
                "channel_id":    item.Snippet.ChannelID,
                "channel_title": item.Snippet.ChannelTitle,
            }
            rowKey := vid
            if err := pub.AddRecord(source, channelID, rowKey, rec); err != nil {
                emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("publish: %v", err)})
                return err
            }
            processed++
            if ts > latestID {
                latestID = ts
            }
        }

        // update oldestID if necessary
        newOldest := oldestID
        if oldestID == 0 && minTsThisPage > 0 {
            newOldest = minTsThisPage
        } else if minTsThisPage > 0 && (oldestID == 0 || minTsThisPage < oldestID) {
            newOldest = minTsThisPage
        }
        // Update the YouTube state. The UpdateYouTubeState method in DynamoStore
        // does not accept a timestamp; it will set last_search_at to now. We do
        // not pass latestID/newOldest here to avoid signature mismatch.
        if err := store.UpdateYouTubeState(ctx, channelID); err != nil {
            emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
            return err
        }
        oldestID = newOldest

        emitMeta("batch_committed", map[string]any{"records": processed})

        if parsed.NextPageToken == "" {
            break
        }
        pageToken = parsed.NextPageToken
        // small jitter to avoid convoy
        time.Sleep(50 * time.Millisecond)
    }

    // job completed event
    durationMs := time.Since(startTime).Milliseconds()
    emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
    return nil
}
