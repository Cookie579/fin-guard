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

	emitMeta("job_started", map[string]any{})

	if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
		if errors.Is(err, ErrLockNotAcquired) {
			return err
		}
		emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("lock: %v", err)})
		return err
	}
	fmt.Printf("LOCK_ACQUIRED job_id=%s pk=%s#%s\n", jobID, source, strings.ToUpper(ticker))
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
				if err := store.HeartbeatLock(ctx, source, ticker, ownerID); err != nil {
					log.Printf("WARN: heartbeat failed for %s: %v", ticker, err)
				}
			}
		}
	}()
	defer hbCancel()

	latestID, _, err := store.GetStocktwitsState(ctx, ticker)
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

		// Build endpoint
		endpoint := fmt.Sprintf("/streams/symbol/%s.json", ticker)
		reqURL := endpoint
		if latestID > 0 {
			v := url.Values{}
			v.Set("since", strconv.FormatInt(latestID, 10))
			reqURL += "?" + v.Encode()
		}

		// === Acquire a permit from the broker ===
		lease, err := acquirePermit(ctx, "stocktwits")
		if err != nil {
			emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("broker acquire: %v", err)})
			return err
		}
		fullURL := "https://" + lease.Host + reqURL

		// Request using the host/key returned by the broker
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fullURL, nil)
		req.Header.Set("X-RapidAPI-Host", lease.Host) // e.g., stocktwits.p.rapidapi.com
		req.Header.Set("X-RapidAPI-Key", lease.Key)
		req.Header.Set("User-Agent", "ingest-social-go/1.0")

		resp, err := client.Do(req)
		var body []byte
		if err == nil {
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
		}

		// Always report to broker
		ok := err == nil && resp != nil && resp.StatusCode == http.StatusOK
		status := 0
		retryAfter := ""
		if resp != nil {
			status = resp.StatusCode
			retryAfter = resp.Header.Get("Retry-After")
		}
		reportResult(ctx, lease.LeaseID, ok, status, retryAfter)

		if err != nil {
			emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", err)})
			return err
		}
		if resp.StatusCode == http.StatusTooManyRequests {
			emitMeta("job_failed", map[string]any{"reason": "rate-limited"})
			return fmt.Errorf("rate-limited")
		}
		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
			emitMeta("job_failed", map[string]any{"reason": err.Error()})
			return err
		}

		// Parse payload (unchanged)
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
		if len(parsed.Messages) == 0 {
			break
		}
		for _, msg := range parsed.Messages {
			if args.UntilID > 0 && msg.ID <= args.UntilID {
				pagesFetched = args.MaxPages
				break
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

		if err := store.UpdateStocktwitsState(ctx, ticker, latestID, 0); err != nil {
			emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
			return err
		}
		emitMeta("batch_committed", map[string]any{"records": processedCount})
	}

	durationMs := time.Since(startTime).Milliseconds()
	emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
	return nil
}
