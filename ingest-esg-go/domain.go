package main

// domain.go contains functionality for discovering a company's official
// website using the Serper search API and persisting the result to S3
// and Kinesis.  It mirrors the original utils/domain.go implementation
// but now uses Serper instead of Google Custom Search.  See
// FindDomain for the primary entry point.

import (
    "context"
    "encoding/json"
    "fmt"
    "net/url"
    "strings"
    "time"
)

// DOMAIN_BUCKET defines the S3 bucket used to store domain discovery
// results. Changing this constant requires coordination with other
// components that consume these files.
const DOMAIN_BUCKET = "fin-processed-esg-reports"

// domainS3Key constructs the S3 key for a domain discovery result. The
// key layout is company_domains/<TICKER>-domain-YYYY-MM-DD.json. The
// ticker is upper cased and whitespace trimmed.
func domainS3Key(ticker string, t time.Time) string {
    tk := strings.ToUpper(strings.TrimSpace(ticker))
    return fmt.Sprintf(
        "company_domains/%s-domain-%04d-%02d-%02d.json",
        tk, t.Year(), int(t.Month()), t.Day(),
    )
}

// FindDomain discovers the official domain for a company.  It first
// consults a DynamoDB cache (via DomainDDBStore) and, if a cached
// domain exists, writes that value to S3 (if possible) and emits
// it to Kinesis.  When no cached domain is found the function
// attempts to check whether today's domain file already exists in
// S3; errors in this check are treated as "not exists" and do not
// halt execution.  A Google Custom Search is performed only when
// neither the cache nor the existing S3 record provide a result.  In
// all cases the discovered or cached domain is persisted to
// DynamoDB (if available) and returned to the caller.  The
// 'skipped' return value is true only when an S3 record already
// exists for today and no work was necessary.
func FindDomain(c Company, cfg *Config) (*DomainEntry, bool, error) {
    ctx := context.Background()
    now := time.Now().UTC()
    key := domainS3Key(c.Ticker, now)
    // Attempt to load from DynamoDB cache
    if store, err := NewDomainDDBStore(ctx); err == nil {
        if cached, err2 := store.GetDomain(ctx, c.Ticker); err2 == nil && cached != "" {
            entry := &DomainEntry{
                Company:  c.Name,
                Ticker:   c.Ticker,
                Domain:   cached,
                Source:   "DOMAIN",
                IngestTS: now.Format(time.RFC3339),
            }
            // Try to write S3 record; log errors but ignore failures
            if err := PutDomainToS3(DOMAIN_BUCKET, key, entry); err != nil {
                fmt.Printf("⚠️ S3 put (cached domain) failed: %v\n", err)
            } else {
                fmt.Printf("✅ uploaded s3://%s/%s\n", DOMAIN_BUCKET, key)
            }
            if cfg.ReferenceStream != "" {
                if err := SendDomainToStream(cfg.ReferenceStream, entry); err != nil {
                    fmt.Printf("⚠️ Kinesis push failed: %v\n", err)
                }
            }
            return entry, false, nil
        }
        // store available; discovered domains will be persisted below
    }
    // Check if today's domain record already exists in S3.  If the
    // check fails (e.g. missing credentials) we treat it as not
    // existing and proceed.
    exists := false
    if ok, err := CheckIfExists(DOMAIN_BUCKET, key); err == nil {
        exists = ok
    } else {
        fmt.Printf("⚠️ S3 head failed: %v (continuing)\n", err)
    }
    if exists {
        fmt.Printf("📁 %s exists — skip\n", key)
        return nil, true, nil
    }
    // Perform a Serper search to find the domain.  The query asks for
    // the company's official site and relies on cfg.SerperAPIKey.
    query := fmt.Sprintf("%s official site", c.Name)
    results, err := GoogleSearch(query, cfg)
    if err != nil {
        return nil, false, fmt.Errorf("serper: %w", err)
    }
    var host string
    for _, it := range results.Items {
        if u, e := url.Parse(it.Link); e == nil && u.Host != "" {
            host = u.Host
            break
        }
    }
    if host == "" {
        return nil, false, fmt.Errorf("no valid domain found")
    }
    entry := &DomainEntry{
        Company:  c.Name,
        Ticker:   c.Ticker,
        Domain:   host,
        Source:   "DOMAIN",
        IngestTS: now.Format(time.RFC3339),
    }
    // Persist to S3; ignore errors
    if err := PutDomainToS3(DOMAIN_BUCKET, key, entry); err != nil {
        fmt.Printf("⚠️ S3 put (discovered domain) failed: %v\n", err)
    } else {
        fmt.Printf("✅ uploaded s3://%s/%s\n", DOMAIN_BUCKET, key)
    }
    // Emit to Kinesis if a stream is configured
    if cfg.ReferenceStream != "" {
        if err := SendDomainToStream(cfg.ReferenceStream, entry); err != nil {
            fmt.Printf("⚠️ Kinesis push failed: %v\n", err)
        }
    }
    // Write to DDB cache if available
    if store, err := NewDomainDDBStore(ctx); err == nil {
        if err := store.PutDomain(ctx, c.Ticker, c.Name, host); err != nil {
            fmt.Printf("⚠️ DDB put domain %s: %v\n", c.Ticker, err)
        }
    }
    return entry, false, nil
}

// SendDomainToStream marshals a DomainEntry and publishes it on the
// given Kinesis stream. PartitionKey is the ticker. See kinesis.go
// for SendToStreamRaw. Errors from PutRecord are returned to the
// caller.
func SendDomainToStream(stream string, entry *DomainEntry) error {
    payload, _ := json.Marshal(entry)
    return SendToStreamRaw(stream, entry.Ticker, payload)
}