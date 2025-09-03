package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RunStocktwitsJob orchestrates the lifecycle of a single StockTwits ingest
// job. It takes the unique job ID, the ingest date string and the parsed
// command line arguments. The function implements locking, state management,
// message fetching, event emission and graceful termination in response to
// context cancellation. On error the job will emit a job_failed event
// before returning the error.
func RunStocktwitsJob(ctx context.Context, jobID, ingestDate string, args *stocktwitsArgs) error {
	ticker := strings.ToUpper(args.Ticker)
	source := "stocktwits"
	ownerID := jobID

	// Initialise DynamoDB store and Kinesis publisher
	store, err := NewDynamoStore(ctx)
	if err != nil {
		return fmt.Errorf("create dynamo store: %w", err)
	}
	pub, err := NewKinesisPublisher(ctx, jobID)
	if err != nil {
		return fmt.Errorf("create kinesis publisher: %w", err)
	}
	defer func() { _ = pub.Close(ctx) }()

	// Helper to emit a metadata event
	emitMeta := func(eventType string, extra map[string]any) {
		rec := map[string]any{
			"stage":  StageEvents,
			"type":   eventType,
			"source": source,
			"ticker": ticker,
			"job_id": jobID,
			"ts":     time.Now().UTC().Format(time.RFC3339),
		}
		maps.Copy(rec, extra)
		rowKey := fmt.Sprintf("meta:%s", eventType)
		if err := pub.AddRecord(source, ticker, rowKey, rec); err != nil {
			log.Printf("WARN: failed to emit %s event: %v", eventType, err)
		}
	}

	emitMeta("job_started", map[string]any{})

	// Acquire lock
	if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			return err
		}
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("lock: %v", err)})
		return err
	}
	fmt.Printf("LOCK_ACQUIRED job_id=%s pk=%s#%s\n", jobID, source, strings.ToUpper(ticker))
	defer func() { _ = store.ReleaseLock(context.Background(), source, ticker, ownerID) }()

	// Heartbeat
	hbCtx, hbCancel := context.WithCancel(ctx)
	go func() {
		t := time.NewTicker(time.Duration(LockHeartbeatSeconds) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-hbCtx.Done():
				return
			case <-t.C:
				if err := store.HeartbeatLock(ctx, source, ticker, ownerID); err != nil {
					log.Printf("WARN: heartbeat failed for %s: %v", ticker, err)
				}
			}
		}
	}()
	defer hbCancel()

	// Load state
	latestID, existingOldest, err := store.GetStocktwitsState(ctx, ticker)
	if err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state load: %v", err)})
		return err
	}
	if args.SinceID > 0 {
		latestID = args.SinceID
	}
	accOldest := existingOldest // we’ll merge page mins into this

	// RapidAPI host/key
	host, apiKey, err := getRapidAPIHostAndKey()
	if err != nil {
		err = fmt.Errorf("stocktwits: %w", err)
		emitMeta("job_failed", map[string]any{"reason": err.Error()})
		return err
	}
	client := &http.Client{Timeout: 45 * time.Second}
	baseURL := fmt.Sprintf("https://%s", host)

	var processedCount int
	startTime := time.Now()
	pagesFetched := 0
	maxPages := args.MaxPages // 0 means unlimited

	// Small helper to fetch one page with arbitrary query params.
	fetchPage := func(q url.Values) ([]byte, error) {
		endpoint := fmt.Sprintf("/streams/symbol/%s.json", ticker)
		reqURL := baseURL + endpoint
		if len(q) > 0 {
			reqURL += "?" + q.Encode()
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		req.Header.Set("X-RapidAPI-Host", host)
		req.Header.Set("X-RapidAPI-Key", apiKey)
		req.Header.Set("User-Agent", "ingest-social-go/1.0")
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
		}
		return body, nil
	}

	type pageMsg struct {
		ID        int64  `json:"id"`
		CreatedAt string `json:"created_at"`
		Body      string `json:"body"`
		Symbols   []struct {
			Symbol string `json:"symbol"`
		} `json:"symbols"`
		Entities struct {
			Sentiment struct {
				Basic string `json:"basic"`
			} `json:"sentiment"`
		} `json:"entities"`
		Likes struct {
			Total int `json:"total"`
		} `json:"likes"`
		Reshares struct {
			ResharedCount int `json:"reshared_count"`
		} `json:"reshares"`
		User struct {
			ID       int64  `json:"id"`
			Username string `json:"username"`
		} `json:"user"`
	}
	parse := func(body []byte) ([]pageMsg, error) {
		var parsed struct {
			Messages []pageMsg `json:"messages"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			return nil, err
		}
		return parsed.Messages, nil
	}

	// ---------- PASS 1: Forward (since latestID) ----------
	if ctx.Err() == nil {
		q := url.Values{}
		if latestID > 0 {
			q.Set("since", strconv.FormatInt(latestID, 10))
		}
		for {
			if maxPages > 0 && pagesFetched >= maxPages {
				break
			}
			select {
			case <-ctx.Done():
				emitMeta("job_failed", map[string]any{"reason": "terminated"})
				return fmt.Errorf("terminated")
			default:
			}

			body, err := fetchPage(q)
			if err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", err)})
				return err
			}
			msgs, err := parse(body)
			if err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("unmarshal: %v", err)})
				return err
			}
			if len(msgs) == 0 {
				break // no newer messages
			}

			var pageMinID int64
			for _, msg := range msgs {
				if args.UntilID > 0 && msg.ID <= args.UntilID {
					pagesFetched = maxPages // force outer break
					break
				}
				// symbols -> []string
				var syms []string
				for _, s := range msg.Symbols {
					if s.Symbol != "" {
						syms = append(syms, strings.ToUpper(s.Symbol))
					}
				}
				rec := map[string]any{
					"stage":         StageRaw,
					"type":          "stocktwits_message",
					"source":        source,
					"ticker":        ticker,
					"ingest_date":   ingestDate,
					"job_id":        jobID,
					"message_id":    msg.ID,
					"created_at":    msg.CreatedAt,
					"text":          msg.Body,
					"symbols":       syms,
					"sentiment":     msg.Entities.Sentiment.Basic,
					"likes":         msg.Likes.Total,
					"reshares":      msg.Reshares.ResharedCount,
					"user_id":       msg.User.ID,
					"user_username": msg.User.Username,
				}
				rowKey := strconv.FormatInt(msg.ID, 10)
				if err := pub.AddRecord(source, ticker, rowKey, rec); err != nil {
					emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("publish: %v", err)})
					return err
				}
				processedCount++

				if msg.ID > latestID {
					latestID = msg.ID
				}
				if pageMinID == 0 || msg.ID < pageMinID {
					pageMinID = msg.ID
				}
			}
			pagesFetched++

			// Merge oldest and persist
			if pageMinID > 0 && (accOldest == 0 || pageMinID < accOldest) {
				accOldest = pageMinID
			}
			if err := store.UpdateStocktwitsState(ctx, ticker, latestID, accOldest); err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
				return err
			}
			emitMeta("batch_committed", map[string]any{"records": processedCount})

			// Next forward page: keep using the same "since=latestID"
			// (API returns only newer-than latestID; break above when none)
		}
	}

	// ---------- PASS 2: Backfill (older-than oldest) ----------
	// If we still have page budget (or unlimited), walk backwards.
	if ctx.Err() == nil && (maxPages == 0 || pagesFetched < maxPages) {
		// Start from the oldest known; if we don't have one yet, seed with latestID.
		maxID := accOldest
		if maxID == 0 {
			maxID = latestID
		}
		for {
			if maxPages > 0 && pagesFetched >= maxPages {
				break
			}
			select {
			case <-ctx.Done():
				emitMeta("job_failed", map[string]any{"reason": "terminated"})
				return fmt.Errorf("terminated")
			default:
			}
			if maxID <= 1 {
				break // nothing older to request
			}

			q := url.Values{}
			// "max" means return messages with ID <= max; use maxID-1 to avoid duplicating boundary.
			q.Set("max", strconv.FormatInt(maxID-1, 10))

			body, err := fetchPage(q)
			if err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", err)})
				return err
			}
			msgs, err := parse(body)
			if err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("unmarshal: %v", err)})
				return err
			}
			if len(msgs) == 0 {
				break // reached the end of history (or API doesn't have older)
			}

			var pageMinID int64
			for _, msg := range msgs {
				// symbols -> []string
				var syms []string
				for _, s := range msg.Symbols {
					if s.Symbol != "" {
						syms = append(syms, strings.ToUpper(s.Symbol))
					}
				}
				rec := map[string]any{
					"stage":         StageRaw,
					"type":          "stocktwits_message",
					"source":        source,
					"ticker":        ticker,
					"ingest_date":   ingestDate,
					"job_id":        jobID,
					"message_id":    msg.ID,
					"created_at":    msg.CreatedAt,
					"text":          msg.Body,
					"symbols":       syms,
					"sentiment":     msg.Entities.Sentiment.Basic,
					"likes":         msg.Likes.Total,
					"reshares":      msg.Reshares.ResharedCount,
					"user_id":       msg.User.ID,
					"user_username": msg.User.Username,
				}
				rowKey := strconv.FormatInt(msg.ID, 10)
				if err := pub.AddRecord(source, ticker, rowKey, rec); err != nil {
					emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("publish: %v", err)})
					return err
				}
				processedCount++

				if pageMinID == 0 || msg.ID < pageMinID {
					pageMinID = msg.ID
				}
			}
			pagesFetched++

			// Advance the backfill cursor and persist oldest
			if pageMinID > 0 {
				maxID = pageMinID
				if accOldest == 0 || pageMinID < accOldest {
					accOldest = pageMinID
				}
			}
			if err := store.UpdateStocktwitsState(ctx, ticker, latestID, accOldest); err != nil {
				emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
				return err
			}
			emitMeta("batch_committed", map[string]any{"records": processedCount})
		}
	}

	// Completed
	durationMs := time.Since(startTime).Milliseconds()
	emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
	return nil
}
