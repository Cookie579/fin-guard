package main

import (
    "context"
    "errors"
    "fmt"
    "log"
    "net/url"
    "os"
    "strings"
    "time"
)

// RunESGJob executes an ESG ingest job for a single ticker. It performs
// domain discovery and/or ESG link discovery based on the provided mode.
// The jobID and ingestDate are emitted with log lines for observability.
// Errors are returned to the caller for handling; recoverable failures
// (such as individual record failures) are logged but do not abort the
// entire job. The args struct holds any user‑supplied overrides such
// as S3 bucket, Kinesis stream or blacklist path.
func RunESGJob(ctx context.Context, jobID, ingestDate, ticker string, args *esgArgs) error {
    // Load configuration from config.json.  The Config struct includes
    // Google API keys (unused for domain discovery), a Serper API key
    // and Kinesis stream settings.
    cfg, err := LoadConfig()
    if err != nil {
        return fmt.Errorf("load config: %w", err)
    }
    // Determine region override if provided. s3.go and kinesis.go set
    // default regions internally so region is mainly relevant for
    // DynamoDB; we honour AWS_REGION if args.Region is non‑empty.
    if args.Region != "" {
        os.Setenv("AWS_REGION", args.Region)
    }
    // Determine the company record without relying on a static companies.json.
    // Historically we loaded companies.json to map tickers to human‑friendly
    // company names. However this configuration file is no longer
    // consulted. Instead we synthesise the company using the ticker as
    // both the name and ticker. This ensures the ingest proceeds even
    // when no local metadata is available. Additional metadata (such as
    // the official domain) is looked up in DynamoDB or discovered on the
    // fly via domain search.
    company := &Company{Name: ticker, Ticker: ticker}
    log.Printf("ℹ️ using ticker %s as company name", ticker)

    // Before performing any ESG scraping we consult the domain cache to
    // determine whether a domain has previously been discovered for this
    // ticker. If no domain is cached we proactively run a domain
    // discovery. This check prevents unnecessary lookups against a
    // removed config.json and ensures that the domain data (when
    // available) resides in DynamoDB. The domain discovery is
    // performed only when the cache is empty. Any failure to access
    // DynamoDB is logged but does not abort the job.
    func() {
        store, err := NewDomainDDBStore(ctx)
        if err != nil {
            log.Printf("⚠️ domain DDB init: %v", err)
            return
        }
        cached, err := store.GetDomain(ctx, ticker)
        if err != nil {
            log.Printf("⚠️ domain DDB get %s: %v", ticker, err)
            return
        }
        if cached == "" {
            // No cached domain; run domain discovery. The FindDomain
            // function will persist the result to DynamoDB, S3 and
            // Kinesis. We ignore errors here because ESG scraping can
            // continue even without a discovered domain.
            entry, skipped, err := FindDomain(*company, cfg)
            if err != nil {
                log.Printf("domain lookup %s: %v", ticker, err)
            } else if !skipped {
                log.Printf("domain saved for %s: %s", ticker, entry.Domain)
            }
        }
    }()
    // Load the blacklist if specified. Absence of the file is not fatal.
    blacklist := map[string]struct{}{}
    if args.Blacklist != "" {
        blk, err := LoadBlacklist(args.Blacklist)
        if err != nil {
            log.Printf("⚠️ no blacklist loaded (%v); proceeding without it", err)
        } else {
            blacklist = blk
            log.Printf("🛡️ loaded %d blacklist domains", len(blacklist))
        }
    }
    // Domain discovery. If mode includes "domain" we run FindDomain to
    // discover the official website of the company. FindDomain writes
    // its result to S3 and publishes a record to the reference stream.
    if args.Mode == "domain" || args.Mode == "esg+domain" {
        entry, skipped, err := FindDomain(*company, cfg)
        if err != nil {
            log.Printf("domain lookup %s: %v", ticker, err)
        } else if !skipped {
            log.Printf("domain saved for %s: %s", ticker, entry.Domain)
        }
    }
    // ESG link scraping. If mode includes "esg" we query Serper via
    // the broker, deduplicate results and emit records to S3 and Kinesis.
    if args.Mode == "esg" || args.Mode == "esg+domain" {
        log.Printf("🔍 Serper ESG search for %s …", company.Name)
        results, err := SearchESGLinks(company.Name, cfg)
        if err != nil {
            log.Printf("search %s: %v", ticker, err)
            return err
        }
        // Initialise DynamoDB seen store
        store, err := NewESGDDBStore(ctx)
        if err != nil {
            log.Printf("dynamo init: %v", err)
            return err
        }
        // Optionally load seen URLs from S3 as a fallback. This is
        // controlled by ESG_USE_S3_SEEN_FALLBACK. If true and the call
        // fails the error is ignored and only DDB deduplication will be
        // used.
        // Do not rely on environment variables for fallback deduplication. Always
        // perform deduplication via DynamoDB and avoid optional S3 seen list
        // controlled by ESG_USE_S3_SEEN_FALLBACK. If needed, you can set
        // useS3Fallback to true directly in code, but we default to false to
        // simplify configuration.
        useS3Fallback := false
        var seenS3 map[string]bool
        newCount := 0
        for _, item := range results {
            select {
            case <-ctx.Done():
                return errors.New("terminated")
            default:
            }
            // Deduplicate via DynamoDB
            already, err := store.IsSeenURL(ctx, ticker, item.Link)
            if err != nil {
                log.Printf("ddb check %s: %v", item.Link, err)
                // If DDB check fails we skip dedup and proceed to emit
            }
            if already {
                continue
            }
            // Fallback deduplication via S3 listing
            if useS3Fallback && seenS3 != nil && seenS3[item.Link] {
                continue
            }
            // Blacklist filtering
            if u, err := url.Parse(item.Link); err == nil {
                h := u.Hostname()
                blocked := false
                for d := range blacklist {
                    if hostMatchesDomain(h, d) {
                        blocked = true
                        break
                    }
                }
                if blocked {
                    continue
                }
            }
            // Build record and save
            rec := ESGLinkRecord{
                Ticker:   ticker,
                Title:    item.Title,
                URL:      item.Link,
                IngestTS: time.Now().UTC().Format(time.RFC3339),
                Source:   "ESG",
                Stage:    "raw",
            }
            if d := inferDateFromURL(item.Link); d != "" {
                rec.FilingDate = d
            } else {
                rec.FilingDate = time.Now().Format("2006-01-02")
            }
            key := linkS3Key(company.Name, ticker, item.Title, item.Link, time.Now().UTC())
            // Write to S3
            if err := PutLinkRecordToS3(args.Bucket, key, rec); err != nil {
                log.Printf("S3 put %s: %v", key, err)
                // continue on S3 errors
            }
            // Determine stream name: use flag override if provided, else from config
            stream := args.Stream
            if stream == "" {
                stream = cfg.KinesisStream
            }
            if stream != "" {
                if err := SendLinkRecord(stream, rec); err != nil {
                    log.Printf("Kinesis push %s: %v", ticker, err)
                }
            }
            // Mark the URL as seen in DDB
            if err := store.MarkSeenURL(ctx, ticker, item.Link, item.Title, rec.Source); err != nil {
                log.Printf("ddb put %s: %v", item.Link, err)
            }
            newCount++
        }
        log.Printf("✅ %s (new=%d / serp=%d)", ticker, newCount, len(results))
    }
    return nil
}