package main

import (
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"
)

// ─────────────────────────── constants ───────────────────────────
const DOMAIN_BUCKET = "fin-processed-esg-reports"

// ───────────────────────── helper key builder ────────────────────
// e.g. company_domains/Apple-Domain-2025-08-06.json
func domainS3Key(company string, t time.Time) string {
	safeName := strings.ReplaceAll(company, " ", "_") // keep key URL-safe
	return fmt.Sprintf(
		"company_domains/%s-domain-%04d-%02d-%02d.json",
		safeName,
		t.Year(),
		int(t.Month()),
		t.Day(),
	)
}

// ────────────────────────── core function ────────────────────────
func FindDomain(c Company, cfg *Config) (*DomainEntry, bool, error) {
	// 1️⃣  Build the key up front (no host needed)
	key := domainS3Key(c.Name, time.Now().UTC())

	// 2️⃣  Skip if today’s file already exists
	exists, err := CheckIfExists(DOMAIN_BUCKET, key)
	if err != nil {
		return nil, false, fmt.Errorf("S3 head: %w", err)
	}
	if exists {
		fmt.Printf("📁 %s exists — skip\n", key)
		return nil, true, nil
	}

	// 3️⃣  Google search for the official domain
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

	// 4️⃣  Assemble JSON payload
	entry := &DomainEntry{
		Name:   c.Name,
		Ticker: c.Ticker,
		Domain: host,
	}

	// 5️⃣  Upload to S3 using the new key
	if err := PutDomainToS3(DOMAIN_BUCKET, key, entry); err != nil {
		return nil, false, fmt.Errorf("S3 put: %w", err)
	}
	fmt.Printf("✅ uploaded s3://%s/%s\n", DOMAIN_BUCKET, key)

	// 6️⃣  Publish a tiny event to Kinesis
	if err := SendDomainToStream(cfg.ReferenceStream, entry); err != nil {
		fmt.Printf("⚠️ Kinesis push failed: %v\n", err)
	}

	return entry, false, nil
}

// ────────────────────── Kinesis publisher ────────────────────────
func SendDomainToStream(stream string, entry *DomainEntry) error {
	payload, _ := json.Marshal(entry)
	return SendToStreamRaw(stream, entry.Ticker, payload)
}
