package main

// domain.go contains functionality for discovering a company's official
// website using Google Custom Search and persisting the result to S3
// and Kinesis. It mirrors the original utils/domain.go implementation
// but lives in the top‑level package so that all code compiles
// together. See FindDomain for the primary entry point.

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

// FindDomain performs a Google Custom Search to find the most likely
// official domain for a company. If a file already exists for the
// current day in the DOMAIN_BUCKET the function returns early with
// skipped=true. Otherwise the first valid domain from the search
// results is stored in S3 and emitted to Kinesis. If no domain can
// be determined or an error occurs the error is returned and skipped
// remains false.
func FindDomain(c Company, cfg *Config) (*DomainEntry, bool, error) {
    now := time.Now().UTC()
    key := domainS3Key(c.Ticker, now)
    // Check if today's domain file already exists in S3.  If so we
    // honour the existing entry and skip any further work.
    exists, err := CheckIfExists(DOMAIN_BUCKET, key)
    if err != nil {
        return nil, false, fmt.Errorf("S3 head: %w", err)
    }
    if exists {
        fmt.Printf("📁 %s exists — skip\n", key)
        return nil, true, nil
    }

    // Attempt to fetch a cached domain from DynamoDB before issuing a
    // new Google search.  If the domain exists in the cache we still
    // write out an S3 record for today's date but avoid the external
    // API call.  Any errors while contacting DynamoDB are silently
    // ignored and fall through to the search path.
    var ddbStore *DomainDDBStore
    ctx := context.Background()
    if s, derr := NewDomainDDBStore(ctx); derr == nil {
        ddbStore = s
        if cached, ferr := s.GetDomain(ctx, c.Ticker); ferr == nil && cached != "" {
            entry := &DomainEntry{
                Company:  c.Name,
                Ticker:   c.Ticker,
                Domain:   cached,
                Source:   "DOMAIN",
                IngestTS: now.Format(time.RFC3339),
            }
            // Persist the cached domain to S3 for today's key and emit to Kinesis
            if err := PutDomainToS3(DOMAIN_BUCKET, key, entry); err != nil {
                return nil, false, fmt.Errorf("S3 put: %w", err)
            }
            fmt.Printf("✅ uploaded s3://%s/%s\n", DOMAIN_BUCKET, key)
            if err := SendDomainToStream(cfg.ReferenceStream, entry); err != nil {
                fmt.Printf("⚠️ Kinesis push failed: %v\n", err)
            }
            return entry, false, nil
        }
    }

    // No cached domain or DDB unavailable: perform a Google Custom
    // Search to infer the domain.  We construct a query combining
    // the company name and "official site".  If the API call
    // fails or returns no valid domain the error is returned.
    query := fmt.Sprintf("%s official site", c.Name)
    results, err := GoogleSearch(query, cfg)
    if err != nil {
        return nil, false, fmt.Errorf("google: %w", err)
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
    // Save the discovered domain to S3 and emit to Kinesis
    if err := PutDomainToS3(DOMAIN_BUCKET, key, entry); err != nil {
        return nil, false, fmt.Errorf("S3 put: %w", err)
    }
    fmt.Printf("✅ uploaded s3://%s/%s\n", DOMAIN_BUCKET, key)
    if err := SendDomainToStream(cfg.ReferenceStream, entry); err != nil {
        fmt.Printf("⚠️ Kinesis push failed: %v\n", err)
    }
    // Persist the domain to DynamoDB for future lookups if the
    // store is available.  Errors are logged but not fatal.
    if ddbStore != nil {
        if err := ddbStore.PutDomain(ctx, c.Ticker, c.Name, host); err != nil {
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