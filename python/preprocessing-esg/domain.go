package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
)

// ─────────────────────────── constants ───────────────────────────
const (
	DOMAIN_BUCKET = "fin-processed-esg-reports" // hard-coded target
)

// ───────────────────────── helper key builder ────────────────────
func domainS3Key(ticker string) string {
	return fmt.Sprintf("company_domains/%s.json", strings.ToUpper(ticker))
}

// ────────────────────────── core function ────────────────────────
// FindDomain()  -->  looks up or creates  <TICKER>.json
func FindDomain(c Company, cfg *Config) (*DomainEntry, bool, error) {
	key := domainS3Key(c.Ticker)

	// 1️⃣  skip if already on S3
	exists, err := CheckIfExists(DOMAIN_BUCKET, key)
	if err != nil {
		return nil, false, fmt.Errorf("S3 head: %w", err)
	}
	if exists {
		fmt.Printf("📁 %s exists — skip\n", key)
		return nil, true, nil
	}

	// 2️⃣  Google search
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
		Name:   c.Name,
		Ticker: c.Ticker,
		Domain: host,
	}

	// 3️⃣  upload JSON  (reuse existing PutDomainToS3 helper)
	if err := PutDomainToS3(DOMAIN_BUCKET, key, entry); err != nil {
		return nil, false, fmt.Errorf("S3 put: %w", err)
	}
	fmt.Printf("✅ uploaded s3://%s/%s\n", DOMAIN_BUCKET, key)

	// 4️⃣  push a tiny event to Kinesis  (new helper below)
	if err := SendDomainToStream(cfg.KinesisStream, entry); err != nil {
		fmt.Printf("⚠️ Kinesis push failed: %v\n", err)
	}

	return entry, false, nil
}

// ────────────────────── Kinesis publisher ────────────────────────
// Uses the same boto-style SendToStream pattern but for DomainEntry.
func SendDomainToStream(stream string, entry *DomainEntry) error {
	payload, _ := json.Marshal(entry)
	return SendToStreamRaw(stream, entry.Ticker, payload)
}
