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

// RunStocktwitsJob orchestrates one StockTwits ingest job.
func RunStocktwitsJob(ctx context.Context, jobID, ingestDate string, args *stocktwitsArgs) error {
	ticker := strings.ToUpper(args.Ticker)
	source := "stocktwits"
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
		maps.Copy(rec, extra)
		rowKey := fmt.Sprintf("meta:%s", eventType)
		if err := pub.AddRecord(source, ticker, rowKey, rec); err != nil {
			log.Printf("WARN: failed to emit %s event: %v", eventType, err)
		}
	}

	emitMeta("job_started", nil)

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

	// Heartbeat the lock
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

	// Load current state
	latestID, existingOldest, err := store.GetStocktwitsState(ctx, ticker)
	if err != nil {
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state load: %v", err)})
		return err
	}
	if args.SinceID > 0 {
		latestID = args.SinceID
	}

	client := &http.Client{Timeout: 45 * time.Second}
	var processedCount int
	startTime := time.Now()
	pagesFetched := 0

	for {
		if args.MaxPages > 0 && pagesFetched >= args.MaxPages {
			break
		}
		select {
		case <-ctx.Done():
			emitMeta("job_failed", map[string]any{"reason": "terminated"})
			return fmt.Errorf("terminated")
		default:
		}

		endpoint := fmt.Sprintf("/streams/symbol/%s.json", ticker)
		q := url.Values{}

		// ── Seed mode: on the very first page, if oldest_id is 0, fetch WITHOUT 'since'
		// to initialize oldest. Otherwise, use 'since=latestID'.
		seedMode := (pagesFetched == 0 && existingOldest == 0)
		if !seedMode && latestID > 0 {
			q.Set("since", strconv.FormatInt(latestID, 10))
		}

		// Acquire broker permit
		lease, err := acquirePermit(ctx, "stocktwits")
		if err != nil {
			emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("broker acquire: %v", err)})
			return err
		}

		// Build request
		reqURL := "https://" + lease.Host + endpoint
		if len(q) > 0 {
			reqURL += "?" + q.Encode()
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		req.Header.Set("X-RapidAPI-Host", lease.Host)
		req.Header.Set("X-RapidAPI-Key", lease.Key)
		req.Header.Set("User-Agent", "ingest-social-go/1.0")

		// Make request
		var (
			status       int
			retryAfterMs int
			body         []byte
		)
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

		// Parse JSON
		var parsed struct {
			Messages []struct {
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
			} `json:"messages"`
		}
		if err := json.Unmarshal(body, &parsed); err != nil {
			emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("unmarshal: %v", err)})
			return err
		}

		// If no messages:
		if len(parsed.Messages) == 0 {
			// Seed oldest to latest if we have a known latest but couldn't fetch any messages.
			if seedMode && latestID > 0 {
				if err := store.UpdateStocktwitsState(ctx, ticker, latestID, latestID); err != nil {
					emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state seed: %v", err)})
					return err
				}
			}
			break
		}

		// Track min eligible ID this page (eligible = strictly newer than latestID when in seed mode)
		minEligible := int64(0)

		for _, msg := range parsed.Messages {
			// If we are seeding and this message is not newer than our latest, skip publishing it.
			if seedMode && latestID > 0 && msg.ID <= latestID {
				continue
			}

			// stop early if we crossed UntilID
			if args.UntilID > 0 && msg.ID <= args.UntilID {
				pagesFetched = args.MaxPages
				break
			}

			// eligible min (earliest "new" message we actually publish)
			if minEligible == 0 || msg.ID < minEligible {
				minEligible = msg.ID
			}

			var syms []string
			for _, s := range msg.Symbols {
				if s.Symbol != "" {
					syms = append(syms, strings.ToUpper(s.Symbol))
				}
			}
			sentiment := msg.Entities.Sentiment.Basic
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
				"sentiment":     sentiment,
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
		}

		pagesFetched++

		// Initialize oldest if needed
		newOldest := existingOldest
		if newOldest == 0 {
			if minEligible > 0 {
				newOldest = minEligible
			} else if latestID > 0 {
				// No eligible messages (all were <= latest) — seed to latest to avoid 0
				newOldest = latestID
			}
		}

		if err := store.UpdateStocktwitsState(ctx, ticker, latestID, newOldest); err != nil {
			emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
			return err
		}
		existingOldest = newOldest

		emitMeta("batch_committed", map[string]any{"records": processedCount})

		// small jitter
		time.Sleep(50 * time.Millisecond)
	}

	durationMs := time.Since(startTime).Milliseconds()
	emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
	return nil
}
