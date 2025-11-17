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

// RunStocktwitsJob orchestrates one StockTwits ingest job (forward + backfill).
func RunStocktwitsJob(ctx context.Context, jobID, ingestDate string, args *stocktwitsArgs) error {
	ticker := strings.ToUpper(args.Ticker)
	source := "STOCKTWITS"
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
			"stage":  stageMetadata,
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

	// Lock
	if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			return err
		}
		emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("lock: %v", err)})
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
		emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("state load: %v", err)})
		return err
	}
	if args.SinceID > 0 {
		latestID = args.SinceID
	}

	client := &http.Client{Timeout: 45 * time.Second}
	startTime := time.Now()
	processedCount := 0
	pagesFetched := 0

	type pageMode int
	const (
		modeForward pageMode = iota
		modeBackfill
	)
	mode := modeForward
	nextMax := int64(0) // for backfill: next request will use max=nextMax

	for {
		if args.MaxPages > 0 && pagesFetched >= args.MaxPages {
			break
		}
		select {
		case <-ctx.Done():
			emitMeta("job_failed", map[string]any{"error": "terminated"})
			return fmt.Errorf("terminated")
		default:
		}

		// Build endpoint + query
		endpoint := fmt.Sprintf("/streams/symbol/%s.json", ticker)
		q := url.Values{}

		// FIRST: forward mode (newer than latestID).
		// If we have no oldest yet and it's the first page, do a seed fetch WITHOUT since to initialize oldest.
		seedMode := (mode == modeForward && pagesFetched == 0 && existingOldest == 0)
		if mode == modeForward && !seedMode && latestID > 0 {
			q.Set("since", strconv.FormatInt(latestID, 10))
		}

		// BACKFILL: walk older pages using max=<id>.
		if mode == modeBackfill {
			if nextMax <= 0 {
				// initialize starting point for backfill
				if existingOldest > 0 {
					nextMax = existingOldest - 1
				} else if latestID > 0 {
					nextMax = latestID - 1
				} else {
					// nothing to backfill from
					break
				}
			}
			q.Set("max", strconv.FormatInt(nextMax, 10))
		}

		// Acquire permit from broker
		lease, err := acquirePermit(ctx, "stocktwits")
		if err != nil {
			emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("broker acquire: %v", err)})
			return err
		}

		// Request
		reqURL := "https://" + lease.Host + endpoint
		if len(q) > 0 {
			reqURL += "?" + q.Encode()
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
		req.Header.Set("X-RapidAPI-Host", lease.Host)
		req.Header.Set("X-RapidAPI-Key", lease.Key)
		req.Header.Set("User-Agent", "ingest-social-go/1.0")

		// Do request
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

		// Parse
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
			emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("unmarshal: %v", err)})
			return err
		}

		if len(parsed.Messages) == 0 {
			// Forward finished? Switch to backfill if pages budget remains.
			if mode == modeForward && (args.MaxPages == 0 || pagesFetched < args.MaxPages) {
				mode = modeBackfill
				// initialize nextMax lazily on next iteration
				continue
			}
			// Backfill finished or no pages left
			break
		}

		// Track min/max IDs for this page
		minIDThisPage := int64(0)
		maxIDThisPage := int64(0)

		for _, msg := range parsed.Messages {
			// If seed fetch returned messages we already have, skip those in forward seed.
			if seedMode && latestID > 0 && msg.ID <= latestID {
				continue
			}
			// Respect UntilID (stop if we reached/lower than it)
			if args.UntilID > 0 && msg.ID <= args.UntilID {
				pagesFetched = args.MaxPages // force loop exit
				break
			}

			if minIDThisPage == 0 || msg.ID < minIDThisPage {
				minIDThisPage = msg.ID
			}
			if msg.ID > maxIDThisPage {
				maxIDThisPage = msg.ID
			}

			// Build a list of unique symbols mentioned in the message. We always
			// include the job's ticker so that a message is emitted for the
			// primary symbol even if StockTwits omits it from the symbols
			// array. Symbols are uppercased for consistency.
			uniqueSyms := make(map[string]struct{})
			// Always include the job ticker
			uniqueSyms[strings.ToUpper(ticker)] = struct{}{}
			for _, s := range msg.Symbols {
				if s.Symbol != "" {
					uniqueSyms[strings.ToUpper(s.Symbol)] = struct{}{}
				}
			}
			// Extract the basic sentiment value (e.g. "Bullish", "Bearish", "None")
			sentiment := msg.Entities.Sentiment.Basic
			// Base record payload shared across all symbols. Note that we
			// temporarily set the ticker field to the job ticker; this will
			// be overridden per symbol below.
			rec := map[string]any{
				"stage":       StageRaw,
				"type":        "stocktwits_message",
				"source":      source,
				"ticker":      ticker,
				"ingest_date": ingestDate,
				"job_id":      jobID,
				"message_id":  msg.ID,
				"created_at":  msg.CreatedAt,
				"text":        msg.Body,
				"symbols": func() []string { // list of all symbols as slice
					syms := make([]string, 0, len(uniqueSyms))
					for sym := range uniqueSyms {
						syms = append(syms, sym)
					}
					return syms
				}(),
				"sentiment":     sentiment,
				"likes":         msg.Likes.Total,
				"reshares":      msg.Reshares.ResharedCount,
				"user_id":       msg.User.ID,
				"user_username": msg.User.Username,
			}
			rowKey := strconv.FormatInt(msg.ID, 10)
			// Publish the message once for each unique symbol. We attempt to
			// insert into the per‑ticker seen table first; only if the
			// insertion succeeds (meaning we have not processed this ID for
			// that ticker before) do we emit a Kinesis record. This avoids
			// duplicate writes across backfill runs and cross‑ticker jobs.
			for sym := range uniqueSyms {
				inserted, serr := store.CheckAndMarkStocktwitsSeen(ctx, sym, msg.ID)
				if serr != nil {
					emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("seen insert: %v", serr)})
					return serr
				}
				if !inserted {
					// Already processed for this ticker; skip publishing but
					// still continue for other symbols.
					continue
				}
				recCopy := maps.Clone(rec)
				recCopy["ticker"] = sym
				if err := pub.AddRecord(source, sym, rowKey, recCopy); err != nil {
					emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("publish: %v", err)})
					return err
				}
				processedCount++
			}
			// Only grow latestID (forward/newer side)
			if msg.ID > latestID {
				latestID = msg.ID
			}
		}

		pagesFetched++

		// State: never clobber oldest with 0; initialize or move older when we saw data
		newOldest := existingOldest
		if minIDThisPage > 0 {
			if newOldest == 0 || minIDThisPage < newOldest {
				newOldest = minIDThisPage
			}
		}
		if seedMode && newOldest == 0 && latestID > 0 {
			// Seed fallback (no eligible msgs after skipping <= latest)
			newOldest = latestID
		}

		if err := store.UpdateStocktwitsState(ctx, ticker, latestID, newOldest); err != nil {
			emitMeta("job_failed", map[string]any{"error": fmt.Sprintf("state update: %v", err)})
			return err
		}
		existingOldest = newOldest

		emitMeta("batch_committed", map[string]any{"records": processedCount})

		// Backfill pagination: next page should walk older than the smallest seen
		if mode == modeBackfill && minIDThisPage > 0 {
			nextMax = minIDThisPage - 1
		}

		// tiny jitter
		time.Sleep(50 * time.Millisecond)
	}

	durationMs := time.Since(startTime).Milliseconds()
	emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
	return nil
}
