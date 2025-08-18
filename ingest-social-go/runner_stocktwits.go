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

// RunStocktwitsJob orchestrates the lifecycle of a single StockTwits ingest
// job. It takes the loaded configuration, the unique job ID, the ingest
// date string and the parsed command line arguments. The function
// implements locking, state management, message fetching, event emission
// and graceful termination in response to context cancellation. On error
// the job will emit a job_failed event before returning the error.
func RunStocktwitsJob(ctx context.Context, cfg *config.Config, jobID, ingestDate string, args *stocktwitsArgs) error {
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
    defer func() {
        // Always attempt to close the publisher at the end
        _ = pub.Close(ctx)
    }()
    // Helper to emit a metadata event. Metadata events are emitted to
    // Kinesis using a synthetic row key. They include the ingest date and
    // current timestamp.
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
        // Partition key uses a synthetic row key based on the event type so
        // that all metadata for a job goes to the same shard family.
        rowKey := fmt.Sprintf("meta:%s", eventType)
        if err := pub.AddRecord(source, ticker, rowKey, rec); err != nil {
            log.Printf("WARN: failed to emit %s event: %v", eventType, err)
        }
    }
    // Emit job_started event immediately
    emitMeta("job_started", map[string]any{})
    // Acquire lock
    if err := store.AcquireLock(ctx, source, ticker, ownerID, jobID); err != nil {
        emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("lock: %v", err)})
        return err
    }
    defer func() {
        // Always release the lock on exit
        _ = store.ReleaseLock(context.Background(), source, ticker, ownerID)
    }()
    // Load state from DDB
    latestID, _, err := store.GetStocktwitsState(ctx, ticker)
    if err != nil {
        emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state load: %v", err)})
        return err
    }
    // Honour --since-id flag if provided
    if args.SinceID > 0 {
        latestID = args.SinceID
    }
    // Determine API keys to use. Attempt to find keys in config.json. If
    // none are present the job cannot proceed and fails. See readAPIKeys
    // below for details on how keys are discovered.
    rapidKeys, err := readRapidAPIKeys()
    if err != nil || len(rapidKeys) == 0 {
        err := fmt.Errorf("no RapidAPI keys found in config.json")
        emitMeta("job_failed", map[string]any{"reason": err.Error()})
        return err
    }
    keyIdx := 0
    client := &http.Client{Timeout: 45 * time.Second}
    // Build base URL for StockTwits API
    baseURL := "https://stocktwits.p.rapidapi.com"
    // Track number of processed records to emit batch_committed events
    var processedCount int
    // Record the start time to compute job duration on completion
    startTime := time.Now()
    // Begin page loop
    pagesFetched := 0
    for {
        // Respect --max-pages if it is positive (0 means unlimited)
        if args.MaxPages > 0 && pagesFetched >= args.MaxPages {
            break
        }
        select {
        case <-ctx.Done():
            emitMeta("job_failed", map[string]any{"reason": "terminated"})
            return fmt.Errorf("terminated")
        default:
        }
        // Build request URL with since parameter
        endpoint := fmt.Sprintf("/streams/symbol/%s.json", ticker)
        v := url.Values{}
        if latestID > 0 {
            v.Set("since", strconv.FormatInt(latestID, 10))
        }
        reqURL := baseURL + endpoint
        if len(v) > 0 {
            reqURL += "?" + v.Encode()
        }
        // Create and send request using the current API key
        req, _ := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
        req.Header.Set("X-RapidAPI-Host", "stocktwits.p.rapidapi.com")
        req.Header.Set("X-RapidAPI-Key", rapidKeys[keyIdx%len(rapidKeys)])
        req.Header.Set("User-Agent", "ingest-social-go/1.0")
        keyIdx++
        resp, err := client.Do(req)
        if err != nil {
            emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("HTTP error: %v", err)})
            return err
        }
        body, _ := io.ReadAll(resp.Body)
        resp.Body.Close()
        if resp.StatusCode != http.StatusOK {
            // Non‑200 responses may indicate invalid parameters or quota
            err := fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
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
        // If no messages returned we are up‑to‑date
        if len(parsed.Messages) == 0 {
            break
        }
        // Process each message
        for _, msg := range parsed.Messages {
            // Stop if until-id flag dictates we should not process further
            if args.UntilID > 0 && msg.ID <= args.UntilID {
                // break out of outer loops
                pagesFetched = args.MaxPages // set to end
                break
            }
            // Convert symbols to []string
            var syms []string
            for _, s := range msg.Symbols {
                if s.Symbol != "" {
                    syms = append(syms, strings.ToUpper(s.Symbol))
                }
            }
            // Sentiment is optional
            sentiment := msg.Entities.Sentiment.Basic
            rec := map[string]any{
                "type":         "stocktwits_message",
                "source":       source,
                "ticker":       ticker,
                "ingest_date":  ingestDate,
                "job_id":       jobID,
                "message_id":   msg.ID,
                "created_at":   msg.CreatedAt,
                "text":         msg.Body,
                "symbols":      syms,
                "sentiment":    sentiment,
                "likes":        msg.Likes.Total,
                "reshares":     msg.Reshares.ResharedCount,
                "user_id":      msg.User.ID,
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
        // Update state after each page
        if err := store.UpdateStocktwitsState(ctx, ticker, latestID, 0); err != nil {
            emitMeta("job_failed", map[string]any{"reason": fmt.Sprintf("state update: %v", err)})
            return err
        }
        // Emit batch_committed event with number of records processed so far
        emitMeta("batch_committed", map[string]any{"records": processedCount})
    }
    // On normal completion emit job_completed event with duration
    durationMs := time.Since(startTime).Milliseconds()
    emitMeta("job_completed", map[string]any{"duration_ms": durationMs})
    return nil
}

// readRapidAPIKeys parses the config.json file in the parent directory and
// attempts to extract a list of RapidAPI keys. Keys are looked for under
// several commonly used property names. If no keys are found the
// returned slice will be empty. An error is only returned if the file
// cannot be read or parsed.
func readRapidAPIKeys() ([]string, error) {
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
        "rapidApiKey", "rapid_api_key", "RapidAPIKey", "rapidAPIKey", "stocktwitsApiKey", "stockTwitsApiKey",
        "stocktwits_keys", "stockTwitsKeys", "StockTwitsKeys",
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
    // Deduplicate keys
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