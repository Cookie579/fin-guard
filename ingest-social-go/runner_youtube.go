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

	// strconv is imported for completeness but unused in this file
	"html"
	"regexp"
	_ "strconv"
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

// searchQueries returns a list of search terms to use when harvesting
// YouTube videos for a given ticker. These queries are designed to
// capture a broad range of news, analysis and investor sentiment around
// the stock. They are inspired by the legacy ingest-social-go-local
// implementation. Callers should iterate through the returned terms
// until they have collected enough fresh videos. All terms include
// the ticker without transformation so they remain case sensitive.
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

// RunYouTubeJob orchestrates a YouTube ingest job using the RapidAPI YouTube
// v3 Lite proxy. It collects video metadata and comments for a search
// query derived from the stock ticker. The job avoids duplicates by
// tracking the last_search_at timestamp in DynamoDB and by inserting
// discovered video IDs into a per-ticker/year table. It respects
// context cancellation and job-level limits on videos and comment pages.
func RunYouTubeJob(ctx context.Context, jobID, ingestDate string, args *youtubeArgs) error {
	// The ticker is treated as a search term. We uppercase it for
	// consistent lock and state keys, but preserve the original case
	// in search queries. An empty ticker is a programmer error and
	// should be prevented by CLI parsing.
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

	// Metadata emitter for events. We always emit using the uppercase
	// ticker in the 'ticker' field so that Kinesis consumers can
	// consistently key off this value.
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

	// Acquire a distributed lock to ensure only one job per ticker runs at
	// a time. Locks are keyed by source#ticker (both lowercased in
	// DynamoStore implementation).
	if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			return err
		}
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("lock: %v", err)})
		return err
	}
	fmt.Printf("LOCK_ACQUIRED job_id=%s pk=%s#%s\n", jobID, source, ticker)
	defer func() { _ = store.ReleaseLock(context.Background(), source, ticker, ownerID) }()

	// Start heartbeat to keep the lock alive.
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

	// Load the YouTube Data API (GCP) key from config.json
	apiKey, err := getYouTubeAPIKey()
	if err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("missing YouTube API key: %v", err)})
		return err
	}

	// Determine the cutoff timestamp for fresh videos. The
	// GetYouTubeState method returns the last_search_at as a pointer to
	// time. If absent, nil is returned. We convert this to Unix
	// seconds. Only videos strictly newer than this timestamp are
	// considered.
	lastSearchAt, err := store.GetYouTubeState(ctx, ticker)
	if err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state load: %v", err)})
		return err
	}
	var cutoff int64
	if lastSearchAt != nil {
		cutoff = lastSearchAt.UTC().Unix()
	}
	// Track the maximum publishedAt timestamp we see so we can update
	// state when finished. If no videos are found, we still update
	// last_search_at to now so that subsequent runs don't repeatedly
	// search the same pages.
	var maxTimestamp int64 = cutoff

	processedVideos := 0
	startTime := time.Now()
	client := &http.Client{Timeout: 45 * time.Second}

	// dedup map to avoid processing the same video multiple times when
	// searching with multiple queries. Keyed by video ID.
	seen := make(map[string]struct{})

	// Build the list of search terms. We include the raw ticker as the
	// first term to capture generic mentions. All other terms come
	// from searchQueries. We use the original case for search queries.
	terms := []string{args.Ticker}
	terms = append(terms, searchQueries(args.Ticker)...)

	// Iterate through each search term until we have collected the
	// requested number of videos. We continue on to the next term if
	// there are no more results or we've hit the limit.
	for _, term := range terms {
		if args.MaxVideos > 0 && processedVideos >= args.MaxVideos {
			break
		}
		// Reset page token for each new query.
		pageToken := ""
		for {
			// respect MaxVideos per query
			if args.MaxVideos > 0 && processedVideos >= args.MaxVideos {
				break
			}
			select {
			case <-ctx.Done():
				emitMeta("job_failed", map[string]any{"reason": "terminated"})
				return fmt.Errorf("terminated")
			default:
			}

			// Acquire RapidAPI permit for YouTube for the search call.
			lease, err := acquirePermit(ctx, "youtube")
			if err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("broker acquire: %v", err)})
				return err
			}

			// Build the search request. We search for the term and
			// request id,snippet so we can see the videoId and
			// publishedAt. We sort by date to get the newest videos
			// first. We also set type=video to filter out channels
			// or playlists. pageToken is included when paging.

			q := url.Values{}
			q.Set("q", term)
			q.Set("part", "id,snippet")
			q.Set("type", "video")
			q.Set("order", "date")
			q.Set("maxResults", "50")
			if pageToken != "" {
				q.Set("pageToken", pageToken)
			}
			// Use Google's YouTube Data API endpoint
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
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", doErr)})
				return doErr
			}
			if status != http.StatusOK {
				err := fmt.Errorf("HTTP %d: %s", status, string(body))
				emitMeta("job_failed", map[string]any{"reason": err.Error()})
				return err
			}

			// Parse the search results. We only need id.videoId and
			// snippet.publishedAt plus snippet.title/description and
			// channel info for metadata.
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
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("unmarshal: %v", err)})
				return err
			}
			if len(searchRes.Items) == 0 {
				break
			}

			// For each video in this search page:
			for _, item := range searchRes.Items {
				if args.MaxVideos > 0 && processedVideos >= args.MaxVideos {
					break
				}
				vid := item.ID.VideoID
				if vid == "" {
					continue
				}
				if _, already := seen[vid]; already {
					continue
				}
				// Parse the publishedAt timestamp. If we can't parse it,
				// skip the video.
				t, err := time.Parse(time.RFC3339, item.Snippet.PublishedAt)
				if err != nil {
					continue
				}
				ts := t.Unix()
				// Skip videos at or before the cutoff.
				if cutoff > 0 && ts <= cutoff {
					continue
				}
				seen[vid] = struct{}{}
				if ts > maxTimestamp {
					maxTimestamp = ts
				}

				// Insert the video ID into the video_id table to aid
				// deduplication across runs. We derive the partition
				// year from the publishedAt timestamp. InsertYouTubeVideoID
				// returns a boolean indicating if the insert happened.
				year := t.UTC().Year()
				_, err = store.InsertYouTubeVideoID(ctx, ticker, year, vid, time.Now().UTC(), &t, item.Snippet.ChannelID, jobID)
				if err != nil {
					emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("ddb insert: %v", err)})
					return err
				}

				// Publish the video metadata record to Kinesis. We use a
				// rowKey of "vid:<videoId>" so that retries or
				// duplicates are naturally deduped by downstream
				// consumers.
				videoRec := map[string]any{
					"stage":         StageRaw,
					"type":          "youtube_video",
					"source":        source,
					"ticker":        ticker,
					"ingest_date":   ingestDate,
					"job_id":        jobID,
					"video_id":      vid,
					"published_at":  item.Snippet.PublishedAt,
					"title":         item.Snippet.Title,
					"description":   item.Snippet.Description,
					"channel_id":    item.Snippet.ChannelID,
					"channel_title": item.Snippet.ChannelTitle,
					"search_term":   term,
				}
				videoRowKey := "vid:" + vid
				if err := pub.AddRecord(source, ticker, videoRowKey, videoRec); err != nil {
					emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("publish video: %v", err)})
					return err
				}
				processedVideos++

				// Now fetch comment threads for this video. We loop
				// through pages until we exhaust the API or hit the
				// extra-pages limit. Each page is emitted as a raw
				// record with the entire response body.
				if args.ExtraPages < 0 || args.ExtraPages > 0 {
					commentPage := ""
					pagesLeft := args.ExtraPages
					for {
						if args.ExtraPages >= 0 && pagesLeft == 0 {
							break
						}
						// Acquire a permit for comments call
						cLease, err := acquirePermit(ctx, "youtube")
						if err != nil {
							emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("broker acquire: %v", err)})
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
						// Use Google's YouTube Data API commentThreads endpoint
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
							emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", cErr)})
							return cErr
						}
						if cStatus != http.StatusOK {
							// If comments are disabled or API fails, just stop comment harvest for this video
							break
						}
						// Emit the raw comment page. We include the full response JSON for downstream processing.

						commentRec := map[string]any{
							"stage":         StageRaw,
							"type":          "youtube_comment_page",
							"source":        source,
							"ticker":        ticker,
							"ingest_date":   ingestDate,
							"job_id":        jobID,
							"video_id":      vid,
							"published_at":  item.Snippet.PublishedAt,
							"page_token":    commentPage,
							"response_json": string(cBody),
						}
						commentRowKey := "comments:" + vid
						if commentPage != "" {
							commentRowKey += ":" + commentPage
						}
						if err := pub.AddRecord(source, ticker, commentRowKey, commentRec); err != nil {
							emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("publish comments: %v", err)})
							return err
						}
						// Attempt to parse nextPageToken from the response to continue paginating
						var cr struct {
							NextPageToken string `json:"nextPageToken"`
						}
						_ = json.Unmarshal(cBody, &cr)
						if cr.NextPageToken == "" {
							break
						}
						commentPage = cr.NextPageToken
						if args.ExtraPages > 0 {
							pagesLeft--
						}
						// Sleep a bit before next page to avoid convoy
						if args.DelayMS > 0 {
							time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
						} else {
							time.Sleep(50 * time.Millisecond)
						}
					}
				}
			}

			// If we reached our video limit, stop paging this query
			if args.MaxVideos > 0 && processedVideos >= args.MaxVideos {
				break
			}

			// If there are no more pages, break the query loop
			if searchRes.NextPageToken == "" {
				break
			}
			pageToken = searchRes.NextPageToken
			// Delay between search pages
			if args.DelayMS > 0 {
				time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
			} else {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	// Update the state to reflect the latest ingestion time. We call
	// UpdateYouTubeState which sets last_search_at and updated_at to now.
	if err := store.UpdateYouTubeState(ctx, ticker); err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
		return err
	}

	// Emit batch and job completion events. The records field counts
	// videos only, not comment pages.
	emitMeta("batch_committed", map[string]any{"records": processedVideos})
	durationMs := time.Since(startTime).Milliseconds()
	emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
	return nil
}
