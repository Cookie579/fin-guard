package main

import (
    "context"
    "encoding/json"
    "errors"
    "fmt"
    "hash/fnv"
    "math"
    "math/rand"
    "os"
    "strings"
    "sync"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/kinesis"
    "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

// KinesisPublisher batches and publishes records to an AWS Kinesis stream
// according to the limits specified in constants.go. Each publisher is
// associated with a single job and maintains its own buffers. Call Close()
// at the end of a job to ensure any buffered records are flushed.
type KinesisPublisher struct {
    client     *kinesis.Client
    streamName string
    jobID      string

    mu         sync.Mutex
    batch      []types.PutRecordsRequestEntry
    batchBytes int
    ctx        context.Context
    cancel     context.CancelFunc
    wg         sync.WaitGroup
}

// NewKinesisPublisher constructs a publisher for the configured stream. It
// starts an internal goroutine that flushes the batch periodically based on
// KinFlushEveryMs. The returned publisher must be closed when no longer
// needed to ensure the goroutine terminates and any buffered records are
// sent.
func NewKinesisPublisher(ctx context.Context, jobID string) (*KinesisPublisher, error) {
    // Load AWS default config using the specified region. This will pick up
    // credentials from the environment, shared config files, EC2/ECS role
    // providers, etc. It is important to set the region here rather than
    // rely on ~/.aws/config because constants.go is authoritative.
    cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(AwsRegion))
    if err != nil {
        return nil, fmt.Errorf("load AWS config: %w", err)
    }
    client := kinesis.NewFromConfig(cfg)
    // Derive a cancellable context for the publisher's background goroutine.
    pubCtx, cancel := context.WithCancel(ctx)
    p := &KinesisPublisher{
        client:     client,
        streamName: KinesisStreamName,
        jobID:      jobID,
        ctx:        pubCtx,
        cancel:     cancel,
    }
    // Start a goroutine that flushes the batch at a fixed interval. Using
    // a dedicated goroutine avoids the need for AddRecord callers to keep
    // track of time themselves and spreads network I/O across the job.
    p.wg.Add(1)
    go func() {
        ticker := time.NewTicker(time.Duration(KinFlushEveryMs) * time.Millisecond)
        defer func() {
            ticker.Stop()
            p.wg.Done()
        }()
        for {
            select {
            case <-pubCtx.Done():
                return
            case <-ticker.C:
                // Perform a timed flush; ignore returned error here. A
                // subsequent explicit flush on Close() will surface any
                // outstanding error.
                _ = p.flush(pubCtx)
            }
        }
    }()
    return p, nil
}

// computeSuffix returns a hexadecimal suffix computed from jobID and rowKey.
// It uses FNV‑1a to hash the composite string and then mods the result by
// ShardsMod to spread records evenly across shards.
func computeSuffix(jobID, rowKey string) string {
    h := fnv.New32a()
    // Ignore errors from Write as per documentation; Write on hash
    // never returns an error.
    _, _ = h.Write([]byte(jobID))
    _, _ = h.Write([]byte(":"))
    _, _ = h.Write([]byte(rowKey))
    sum := h.Sum32() % uint32(ShardsMod)
    return fmt.Sprintf("%x", sum)
}

// buildPartitionKey constructs the final Kinesis partition key given the
// source, ticker and rowKey. The prefix distinguishes StockTwits and
// YouTube records and includes the upper‑case ticker. The suffix is the
// result of computeSuffix(jobID,rowKey).
func (p *KinesisPublisher) buildPartitionKey(source, ticker, rowKey string) string {
    suffix := computeSuffix(p.jobID, rowKey)
    switch source {
    case "stocktwits":
        return fmt.Sprintf("st|%s|%s", strings.ToUpper(ticker), suffix)
    case "youtube":
        return fmt.Sprintf("yt|%s|%s", strings.ToUpper(ticker), suffix)
    default:
        // default to generic prefix if unknown source
        return fmt.Sprintf("src|%s|%s", strings.ToUpper(ticker), suffix)
    }
}

// AddRecord marshals the provided payload into JSON, computes an appropriate
// partition key and queues the record for sending. If the payload
// serialisation produces more than 900 KB of JSON then it is truncated to
// comply with the PutRecords limit and a "truncated": true field is added
// to the output. This method is safe for concurrent use.
func (p *KinesisPublisher) AddRecord(source, ticker, rowKey string, payload map[string]any) error {
    if payload == nil {
        return errors.New("nil payload")
    }
    // Marshal to JSON to determine size. Add truncated marker if necessary.
    b, err := json.Marshal(payload)
    if err != nil {
        return fmt.Errorf("marshal payload: %w", err)
    }
    // 900k is the safe upper limit for Kinesis records (1 MB minus
    // overhead). If the payload is too large we attempt to truncate
    // textual fields. Currently we only look for a top‑level "description"
    // field, but this can be extended easily.
    if len(b) > 900_000 {
        if desc, ok := payload["description"].(string); ok && len(desc) > 0 {
            // Trim the description to leave room for other fields. Keep
            // approximately 800 KB of description content. This value is
            // arbitrary but guarantees we stay under 900 KB when re‑marshalling.
            maxDesc := 800_000
            if len(desc) > maxDesc {
                payload["description"] = desc[:maxDesc]
            }
            payload["truncated"] = true
            b, err = json.Marshal(payload)
            if err != nil {
                return fmt.Errorf("marshal truncated payload: %w", err)
            }
        }
        // If still too large, drop the record and return an error. This
        // prevents PutRecords from failing due to oversize entries.
        if len(b) > 900_000 {
            return fmt.Errorf("payload too large after truncation (%d bytes)", len(b))
        }
    }
    pk := p.buildPartitionKey(source, ticker, rowKey)
    entry := types.PutRecordsRequestEntry{
        Data:         b,
        PartitionKey: aws.String(pk),
    }
    // Acquire lock and update batch state
    p.mu.Lock()
    p.batch = append(p.batch, entry)
    p.batchBytes += len(b) + len(pk)
    shouldFlush := len(p.batch) >= KinMaxRecords || p.batchBytes >= KinMaxBytes
    p.mu.Unlock()
    if shouldFlush {
        return p.flush(p.ctx)
    }
    return nil
}

// flush attempts to send all buffered records to Kinesis. If some records
// fail to be written the function retries up to 5 times with exponential
// backoff and jitter. Failures after all attempts are logged to stderr
// via the standard log package. flush returns nil if there were no
// buffered records or all records eventually succeeded.
func (p *KinesisPublisher) flush(ctx context.Context) error {
    p.mu.Lock()
    // Copy current batch and reset internal buffers. We copy to avoid
    // blocking AddRecord while interacting with the network. New records
    // can be queued while this flush is in progress.
    if len(p.batch) == 0 {
        p.mu.Unlock()
        return nil
    }
    batch := make([]types.PutRecordsRequestEntry, len(p.batch))
    copy(batch, p.batch)
    p.batch = nil
    p.batchBytes = 0
    p.mu.Unlock()

    // Define a helper to perform a single PutRecords call.
    put := func(entries []types.PutRecordsRequestEntry) (*kinesis.PutRecordsOutput, error) {
        req := &kinesis.PutRecordsInput{
            Records:    entries,
            StreamName: aws.String(p.streamName),
        }
        return p.client.PutRecords(ctx, req)
    }

    remaining := batch
    var attempt int
    for len(remaining) > 0 && attempt < 5 {
        attempt++
        resp, err := put(remaining)
        if err != nil {
            // Network or service error. Sleep with backoff and try again.
            jitter := time.Duration(rand.Intn(100)) * time.Millisecond
            backoff := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
            time.Sleep(backoff + jitter)
            continue
        }
        // Inspect individual record errors and collect failures.
        var failed []types.PutRecordsRequestEntry
        for i, rec := range resp.Records {
            if rec.ErrorCode != nil && *rec.ErrorCode != "" {
                // preserve the original entry corresponding to this response
                failed = append(failed, remaining[i])
            }
        }
        if len(failed) == 0 {
            break // success
        }
        // Prepare for retry with only the failed records.
        remaining = failed
        jitter := time.Duration(rand.Intn(100)) * time.Millisecond
        backoff := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
        time.Sleep(backoff + jitter)
    }
    if len(remaining) > 0 {
        // Drop remaining failed records after exhausting retries. Log to stderr.
        fmt.Fprintf(os.Stderr, "WARN: dropping %d records after retries\n", len(remaining))
    }
    return nil
}

// Close stops the background flusher, flushes any remaining records and waits
// for the internal goroutine to exit. This method should be called once
// when the job has finished emitting events.
func (p *KinesisPublisher) Close(ctx context.Context) error {
    // Cancel the flusher context to stop the ticker goroutine. This causes
    // the goroutine to exit after the next tick or immediately if the
    // context is done.
    p.cancel()
    // Perform a final flush of any buffered records.
    if err := p.flush(ctx); err != nil {
        return err
    }
    // Wait for the background goroutine to exit.
    p.wg.Wait()
    return nil
}