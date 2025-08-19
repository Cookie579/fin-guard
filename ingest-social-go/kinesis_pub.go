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
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(AwsRegion))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	client := kinesis.NewFromConfig(cfg)
	pubCtx, cancel := context.WithCancel(ctx)
	p := &KinesisPublisher{
		client:     client,
		streamName: KinesisStreamName,
		jobID:      jobID,
		ctx:        pubCtx,
		cancel:     cancel,
	}
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
				_ = p.flush(pubCtx)
			}
		}
	}()
	return p, nil
}

// computeSuffix returns a hexadecimal suffix computed from jobID and rowKey.
func computeSuffix(jobID, rowKey string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(jobID))
	_, _ = h.Write([]byte(":"))
	_, _ = h.Write([]byte(rowKey))
	sum := h.Sum32() % uint32(ShardsMod)
	return fmt.Sprintf("%x", sum)
}

// buildPartitionKey constructs the final Kinesis partition key.
func (p *KinesisPublisher) buildPartitionKey(source, ticker, rowKey string) string {
	suffix := computeSuffix(p.jobID, rowKey)
	switch source {
	case "stocktwits":
		return fmt.Sprintf("st|%s|%s", strings.ToUpper(ticker), suffix)
	case "youtube":
		return fmt.Sprintf("yt|%s|%s", strings.ToUpper(ticker), suffix)
	default:
		return fmt.Sprintf("src|%s|%s", strings.ToUpper(ticker), suffix)
	}
}

// AddRecord marshals payload, enforces size, and queues it.
func (p *KinesisPublisher) AddRecord(source, ticker, rowKey string, payload map[string]any) error {
	if payload == nil {
		return errors.New("nil payload")
	}
	b, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	if len(b) > 900_000 {
		if desc, ok := payload["description"].(string); ok && len(desc) > 0 {
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
		if len(b) > 900_000 {
			return fmt.Errorf("payload too large after truncation (%d bytes)", len(b))
		}
	}
	pk := p.buildPartitionKey(source, ticker, rowKey)
	entry := types.PutRecordsRequestEntry{
		Data:         b,
		PartitionKey: aws.String(pk),
	}
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

// flush sends buffered records with retries and per-record logging.
func (p *KinesisPublisher) flush(ctx context.Context) error {
	p.mu.Lock()
	if len(p.batch) == 0 {
		p.mu.Unlock()
		return nil
	}
	batch := make([]types.PutRecordsRequestEntry, len(p.batch))
	copy(batch, p.batch)
	p.batch = nil
	p.batchBytes = 0
	p.mu.Unlock()

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
			// call-level error
			fmt.Fprintf(os.Stderr, "PutRecords call error (attempt %d): %v\n", attempt, err)
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond
			backoff := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
			time.Sleep(backoff + jitter)
			continue
		}

		if resp.FailedRecordCount != nil && *resp.FailedRecordCount > 0 {
			fmt.Fprintf(os.Stderr, "PutRecords failed count: %d of %d on attempt %d\n",
				*resp.FailedRecordCount, len(remaining), attempt)
		}

		var failed []types.PutRecordsRequestEntry
		for i, rec := range resp.Records {
			if rec.ErrorCode != nil && *rec.ErrorCode != "" {
				fmt.Fprintf(os.Stderr, "PutRecords failed[idx=%d attempt=%d]: %s - %s\n",
					i, attempt, aws.ToString(rec.ErrorCode), aws.ToString(rec.ErrorMessage))
				if i < len(remaining) {
					pk := ""
					if remaining[i].PartitionKey != nil {
						pk = *remaining[i].PartitionKey
					}
					fmt.Fprintf(os.Stderr, "  offending record: size=%d pk=%s\n",
						len(remaining[i].Data), pk)
					failed = append(failed, remaining[i])
				} else {
					fmt.Fprintln(os.Stderr, "  WARN: response index exceeded request slice")
				}
			}
		}

		if len(failed) == 0 {
			// *** FIX: mark as success so post-loop drop warning doesn't trigger
			remaining = nil
			break
		}

		remaining = failed
		jitter := time.Duration(rand.Intn(100)) * time.Millisecond
		backoff := time.Duration(math.Pow(2, float64(attempt))) * 100 * time.Millisecond
		time.Sleep(backoff + jitter)
	}

	if len(remaining) > 0 {
		fmt.Fprintf(os.Stderr, "WARN: dropping %d records after retries\n", len(remaining))
	}
	return nil
}

// Close stops the flusher, flushes remaining records, waits for exit.
func (p *KinesisPublisher) Close(ctx context.Context) error {
	p.cancel()
	if err := p.flush(ctx); err != nil {
		return err
	}
	p.wg.Wait()
	return nil
}
