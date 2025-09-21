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
// brRe matches HTML line breaks, tagRe matches any HTML tag. These are used
// by cleanYTText to remove markup from comment and description fields while
// preserving emoji characters.
var brRe = regexp.MustCompile(`(?i)<br\s*/?>`)
var tagRe = regexp.MustCompile(`<[^>]+>`)

// stripZeroWidth removes zero width and byte order mark runes from strings.
func stripZeroWidth(s string) string {
	return strings.Map(func(r rune) rune {
		switch r {
		case '\u200B', '\u200C', '\u200D', '\uFEFF':
			return -1
		}
		return r
	}, s)
}

// cleanYTText normalizes YouTube text by replacing <br> tags with newlines,
// unescaping HTML entities, stripping all remaining tags, removing carriage
// returns and zero width characters, and trimming whitespace.
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

// searchQueries returns a slice of search terms for harvesting YouTube
// videos related to a given stock ticker. These phrases mirror the
// legacy ingest-social-go-local implementation and avoid using the bare
// ticker alone. Each term is case sensitive and includes the ticker.
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

// RunYouTubeJob orchestrates a YouTube ingest job using the Google
// YouTube Data API. It collects recent video metadata and comments
// matching a set of search queries derived from the provided stock ticker.
// The job tracks duplicate videos across runs via DynamoDB and skips
// reprocessing already-seen videos. It respects context cancellation,
// job-level video limits and comment page limits via args.MaxVideos and
// args.ExtraPages. A global rate broker enforces API quotas.
func RunYouTubeJob(ctx context.Context, jobID, ingestDate string, args *youtubeArgs) error {
	// Ensure a non-empty ticker. We upper-case for lock and state keys but
	// preserve the original case for search queries. This mirrors the local
	// behaviour and avoids accidentally lowercasing user-provided tickers.
	if args.Ticker == "" {
		return fmt.Errorf("ticker required")
	}
	ticker := strings.ToUpper(args.Ticker)
	source := "youtube"
	ownerID := jobID

	// Initialise backing store and publisher. Any failure here ends the job
	// before acquiring the lock, to avoid stale locks or half-initialised
	// publishers.
	store, err := NewDynamoStore(ctx)
	if err != nil {
		return fmt.Errorf("create dynamo store: %w", err)
	}
	pub, err := NewKinesisPublisher(ctx, jobID)
	if err != nil {
		return fmt.Errorf("create kinesis publisher: %w", err)
	}
	defer func() { _ = pub.Close(ctx) }()

	// Helper to emit metadata events to Kinesis. All events use the
	// uppercase ticker to ease downstream partitioning.
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

	// Acquire a distributed lock per source#ticker. This prevents more than
	// one concurrent job from processing the same ticker. Locks are keyed
	// lowercased internally by the store implementation.
	if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			return err
		}
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("lock: %v", err)})
		return err
	}
	fmt.Printf("LOCK_ACQUIRED job_id=%s pk=%s#%s\n", jobID, source, ticker)
	defer func() { _ = store.ReleaseLock(context.Background(), source, ticker, ownerID) }()

	// Heartbeat goroutine to keep the lock alive. Cancelled when the job
	// finishes or is cancelled. Any HeartbeatLock error is ignored here; if
	// the lock expires, subsequent operations will fail gracefully.
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

	// Load the YouTube API key from config. A missing key terminates the
	// job immediately.
	apiKey, err := getYouTubeAPIKey()
	if err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("missing YouTube API key: %v", err)})
		return err
	}

	// Determine the cutoff timestamp for fresh videos. We use the last_search_at
	// timestamp stored in DynamoDB; only videos strictly newer than this
	// timestamp are processed. If no prior state exists, cutoff remains zero.
	lastSearchAt, err := store.GetYouTubeState(ctx, ticker)
	if err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state load: %v", err)})
		return err
	}
	var cutoff int64
	if lastSearchAt != nil {
		cutoff = lastSearchAt.UTC().Unix()
	}
	// Track the maximum publishedAt timestamp we encounter during this run
	// so we can update last_search_at even if no videos are returned.
	var maxTimestamp int64 = cutoff

	processedVideos := 0
	startTime := time.Now()
	client := &http.Client{Timeout: 45 * time.Second}

	// Local deduplication: avoids processing the same video twice when
	// searching with multiple queries in this run.
	seen := make(map[string]struct{})

	// Build the list of search terms. Unlike the earlier AWS implementation,
	// we deliberately omit the bare ticker from the list. This ensures we
	// execute the targeted queries inspired by the local scraper and
	// prevents generic results from short-circuiting the search order.
	terms := searchQueries(args.Ticker)

	// done becomes true when we have collected enough videos; it causes
	// all loops to unwind.
	done := false

	// Iterate over each search term. We continue through all queries even
	// after hitting the MaxVideos limit to mirror the local scraper’s
	// behaviour. Exiting early only occurs when done is set.
	for _, term := range terms {
		if done {
			break
		}
		// Reset pageToken for each new term
		pageToken := ""
		for {
			// Respect the MaxVideos limit. A zero value means default to 10.
			maxVideos := args.MaxVideos
			if maxVideos == 0 {
				maxVideos = 10
			}
			if processedVideos >= maxVideos {
				// Set done and break out of paging loop for this term. The
				// outer term loop will also stop on the next iteration.
				done = true
				break
			}

			// Honour context cancellation
			select {
			case <-ctx.Done():
				emitMeta("job_failed", map[string]any{"reason": "terminated"})
				return fmt.Errorf("terminated")
			default:
			}

			// Acquire a permit from the rate broker before making a search call
			lease, err := acquirePermit(ctx, "youtube")
			if err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("broker acquire: %v", err)})
				return err
			}

			// Build the search request for the current term. We use part=id,snippet
			// to retrieve videoId and publishedAt. Results are sorted by date.
			q := url.Values{}
			q.Set("q", term)
			q.Set("part", "id,snippet")
			q.Set("type", "video")
			q.Set("order", "date")
			q.Set("maxResults", "50")
			if pageToken != "" {
				q.Set("pageToken", pageToken)
			}
			// Only request videos published strictly after the cutoff
			if cutoff > 0 {
				q.Set("publishedAfter", time.Unix(cutoff, 0).UTC().Format(time.RFC3339))
			}
			q.Set("key", apiKey)
			reqURL := "https://www.googleapis.com/youtube/v3/search?" + q.Encode()
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
			req.Header.Set("User-Agent", "ingest-social-go/1.0")

			// Perform the HTTP request
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

			// Parse the search results. We only require the videoId and publishedAt
			// plus some snippet metadata.
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
			// If no items returned, no more pages for this query.
			if len(searchRes.Items) == 0 {
				break
			}

			// pageOlder is set if all remaining videos on this page are at or
			// before the cutoff. In that case we break the paging loop.
			pageOlder := false
			for _, item := range searchRes.Items {
				if done {
					break
				}
				// Check again for the global video limit
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
				// Skip if we've already processed this video in this run
				if _, already := seen[vid]; already {
					continue
				}
				// Parse the publishedAt timestamp; skip if unparsable
				t, err := time.Parse(time.RFC3339, item.Snippet.PublishedAt)
				if err != nil {
					continue
				}
				ts := t.Unix()
				// If older than or equal to the cutoff, mark pageOlder and skip
				if cutoff > 0 && ts <= cutoff {
					pageOlder = true
					continue
				}
				// Track video as seen and update maxTimestamp
				seen[vid] = struct{}{}
				if ts > maxTimestamp {
					maxTimestamp = ts
				}
				// Compute partition year for the video
				year := t.UTC().Year()
				// Insert the video ID into DynamoDB. If the insert returns
				// inserted=false it means we saw this video in a previous run and
				// should skip reprocessing it.
				inserted, err := store.InsertYouTubeVideoID(ctx, ticker, year, vid, time.Now().UTC(), &t, item.Snippet.ChannelID, jobID)
				if err != nil {
					emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("ddb insert: %v", err)})
					return err
				}
				// Skip if this video has been processed in a prior run
				if !inserted {
					continue
				}
				// Build and emit the video metadata record. Use a row key of
				// "vid:<videoId>" to allow downstream consumers to deduplicate.
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
					"description":   cleanYTText(item.Snippet.Description),
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

				// Fetch comments for this video. We implement the ExtraPages
				// semantics as follows: ExtraPages = -1 fetches all pages,
				// ExtraPages = 0 fetches none (skip comments), and
				// ExtraPages = N>0 fetches the first page plus N extra.
				// We'll always attempt at least one page when ExtraPages >= 1.
				commentPage := ""
				pagesLeft := args.ExtraPages
				// If ExtraPages is zero, skip comments entirely
				if args.ExtraPages == 0 {
					fmt.Printf("INFO: skipping comments for video %s (extra-pages=0)\n", vid)
				} else {
					if args.ExtraPages > 0 {
						fmt.Printf("INFO: fetching comments for video %s (pages=%d)\n", vid, args.ExtraPages)
					}
					for {
						// Respect ExtraPages when non-negative
						if args.ExtraPages >= 0 && pagesLeft == 0 {
							break
						}
						// Acquire permit for comment API call
						cLease, err := acquirePermit(ctx, "youtube")
						if err != nil {
							emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("broker acquire: %v", err)})
							return err
						}
						// Build request for commentThreads
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
							emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", cErr)})
							return cErr
						}
						if cStatus != http.StatusOK {
							// Stop comment harvest for this video on non-200
							break
						}
						// Parse comment threads
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
						// Emit each top-level comment as a raw record
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
								emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("publish comment: %v", err)})
								return err
							}
						}
						// Determine the next page token
						next := cRespObj.NextPageToken
						if next == "" {
							break
						}
						commentPage = next
						// Decrement pagesLeft if ExtraPages > 0
						if args.ExtraPages > 0 {
							pagesLeft--
						}
						// Delay between comment pages
						if args.DelayMS > 0 {
							time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
						} else {
							time.Sleep(50 * time.Millisecond)
						}
					}
				}
			}

			// Break the paging loop if all videos on this page were too old
			if done || pageOlder {
				break
			}
			// If there is no next page token, stop paging this query
			if searchRes.NextPageToken == "" {
				break
			}
			// Otherwise set pageToken and optionally sleep between search pages
			pageToken = searchRes.NextPageToken
			if args.DelayMS > 0 {
				time.Sleep(time.Duration(args.DelayMS) * time.Millisecond)
			} else {
				time.Sleep(50 * time.Millisecond)
			}
		}
	}

	// At the end of the job (after all terms are processed), update the
	// last_search_at state so subsequent runs only fetch newer videos.
	if err := store.UpdateYouTubeState(ctx, ticker); err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
		return err
	}
	// Emit batch commit and job completion events. processedVideos counts
	// videos only; comment pages are not counted here.
	emitMeta("batch_committed", map[string]any{"records": processedVideos})
	durationMs := time.Since(startTime).Milliseconds()
	emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
	fmt.Printf("JOB_COMPLETED job_id=%s duration_ms=%d\n", jobID, durationMs)
	fmt.Printf("PROCESSED job_id=%s videos=%d\n", jobID, processedVideos)
	return nil
}
