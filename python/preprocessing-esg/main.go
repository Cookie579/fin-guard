// main.go — ESG-preprocessor entrypoint
// ------------------------------------
// Usage examples
//
//	go run . -mode=domain        # discover domains only
//	go run . -mode=esg           # search ESG links only
//	go run .                     # default = esg+domain
//
// Mode values:  esg | domain | esg+domain
package main

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"
)

// The single-link payload we’ll store / stream
type ESGLinkRecord struct {
	Ticker       string `json:"ticker"`
	Title        string `json:"title"`
	URL          string `json:"url"`
	RetrievedUTC string `json:"retrieved_utc"`
}

// Clean the title so it’s key-safe and reasonably short
var badChars = regexp.MustCompile(`[^A-Za-z0-9_-]+`)

func slugify(s string) string {
	s = strings.TrimSpace(s)
	s = badChars.ReplaceAllString(s, "_")
	return strings.Trim(s, "_")
}

// Deterministic S3 key, e.g.  scraping-jsons/AAPL/Environmental_Progress_Report-f0c2d1e2-2025-08-06.json
func linkS3Key(company, ticker, title, url string, t time.Time) string {
	hash := sha1.Sum([]byte(url))
	return fmt.Sprintf(
		"scraping-jsons/%s/%s-%s-%s-%04d-%02d-%02d.json",
		strings.ToUpper(ticker),
		slugify(company), //  ← NEW  company prefix
		slugify(title),
		hex.EncodeToString(hash[:])[:8],
		t.Year(), int(t.Month()), t.Day(),
	)
}

// ── helper to load companies.json ────────────────────────────────
func LoadCompanies(path string) ([]Company, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var wrap struct {
		Companies []Company `json:"companies"`
	}
	if err := json.NewDecoder(f).Decode(&wrap); err != nil {
		return nil, err
	}
	return wrap.Companies, nil
}

// ─────────────────────────── main ───────────────────────────────
func main() {
	mode := flag.String("mode", "esg+domain", "domain | esg | esg+domain")
	flag.Parse()

	log.Printf("🌱 Starting ESG pre-processor  (mode=%s)…", *mode)

	// ---- config + company list ----------------------------------
	companies, err := LoadCompanies("companies.json")
	if err != nil {
		log.Fatalf("❌ load companies.json: %v", err)
	}
	cfg, err := LoadConfig() // or LoadConfig("config.json") locally
	if err != nil {
		log.Fatalf("❌ load config: %v", err)
	}

	// ---- 1️⃣ per-ticker domain discovery ------------------------
	if *mode == "domain" || *mode == "esg+domain" {
		for _, c := range companies {
			if _, skipped, err := FindDomain(c, cfg); err != nil {
				log.Printf("❌ domain lookup %s: %v", c.Ticker, err)
			} else if !skipped {
				log.Printf("🌐 domain saved for %s", c.Ticker)
			}
		}
	}

	// ---- 2️⃣ ESG link search & save -----------------------------
	if *mode == "esg" || *mode == "esg+domain" {
		for _, c := range companies {

			log.Printf("🔍 Google ESG search for %s …", c.Name)
			results, err := SearchESGLinks(c.Name, cfg)
			if err != nil {
				log.Printf("❌ search %s: %v", c.Ticker, err)
				continue
			}

			for _, item := range results {
				rec := ESGLinkRecord{
					Ticker:       c.Ticker,
					Title:        item.Title,
					URL:          item.Link,
					RetrievedUTC: time.Now().UTC().Format(time.RFC3339),
				}

				key := linkS3Key(c.Name, c.Ticker, item.Title, item.Link, time.Now().UTC())

				// skip if already saved (idempotent runs)
				if ok, _ := CheckIfExists("fin-processed-esg-reports", key); ok {
					continue
				}

				if err := PutLinkRecordToS3("fin-processed-esg-reports", key, rec); err != nil {
					log.Printf("❌ S3 put %s: %v", key, err)
					continue
				}

				if err := SendLinkRecord(cfg.KinesisStream, rec); err != nil {
					log.Printf("❌ Kinesis push %s: %v", c.Ticker, err)
				}
			}

			log.Printf("✅ ESG links stored for %s (%d items)", c.Ticker, len(results))
		}
	}

	log.Println("🎉 Pre-processing complete.")
}
