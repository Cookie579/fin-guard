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
    // Load companies.json and find the requested ticker. We assume
    // companies.json exists alongside the binary. If multiple entries
    // exist for the same ticker (unlikely) the first is used.
    // Load companies.json to map tickers to company names. If the file
    // cannot be loaded or the ticker is not found we fall back to
    // using the ticker itself as the company name. This preserves
    // backwards compatibility but allows clients to omit companies.json
    // when launching jobs via the orchestrator.
    var company *Company
    if comps, err := LoadCompanies("companies.json"); err == nil {
        for i := range comps {
            if strings.EqualFold(comps[i].Ticker, ticker) {
                company = &comps[i]
                break
            }
        }
    }
    // Fallback: synthesise a company using the ticker as its name. This
    // allows the ingest to proceed even when companies.json is absent or
    // the ticker is unknown. The Ticker is always upper‑cased.
    if company == nil {
        company = &Company{Name: ticker, Ticker: ticker}
        log.Printf("ℹ️ using ticker %s as company name", ticker)
    }
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
        useS3Fallback := strings.EqualFold(os.Getenv("ESG_USE_S3_SEEN_FALLBACK"), "true")
        var seenS3 map[string]bool
        if useS3Fallback {
            seenS3, _ = LoadSeenURLs(args.Bucket, ticker)
        }
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