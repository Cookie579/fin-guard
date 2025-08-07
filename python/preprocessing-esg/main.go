// main.go — ESG-preprocessor entry-point
package main

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ─────────────────────────── record ────────────────────────────
type ESGLinkRecord struct {
	Ticker       string `json:"ticker"`
	Title        string `json:"title"`
	URL          string `json:"url"`
	RetrievedUTC string `json:"retrieved_utc"`

	Source     string `json:"source"`     // "ESG"
	State      string `json:"state"`      // "raw"
	FilingDate string `json:"filingDate"` // YYYY-MM-DD
}

// ───────────────────────── helpers ─────────────────────────────
var (
	badChars    = regexp.MustCompile(`[^A-Za-z0-9_-]+`)
	datePattern = regexp.MustCompile(`(\d{4})([/-](\d{2}))?([/-](\d{2}))?`)
)

func slugify(s string) string {
	return strings.Trim(badChars.ReplaceAllString(strings.TrimSpace(s), "_"), "_")
}

// scraping-jsons/<TICKER>/<Company_Title>-<hash>-YYYY-MM-DD.json
func linkS3Key(company, ticker, title, url string, t time.Time) string {
	h := sha1.Sum([]byte(url))
	return fmt.Sprintf(
		"scraping-jsons/%s/%s_%s-%s-%04d-%02d-%02d.json",
		strings.ToUpper(ticker),
		slugify(company),
		slugify(title),
		hex.EncodeToString(h[:])[:8],
		t.Year(), int(t.Month()), t.Day())
}

// quick URL / filename date sniff
func inferDateFromURL(u string) string {
	if m := datePattern.FindStringSubmatch(u); m != nil {
		yy, mm, dd := m[1], m[3], m[5]
		switch {
		case mm == "":
			return yy + "-01-01"
		case dd == "":
			return fmt.Sprintf("%s-%s-01", yy, mm)
		default:
			return fmt.Sprintf("%s-%s-%s", yy, mm, dd)
		}
	}
	return ""
}

// ───── pull set of URLs we already have for this ticker ────────
func LoadSeenURLs(bucket, ticker string) (map[string]bool, error) {
	seen := make(map[string]bool)

	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
	svc := s3.New(sess)

	prefix := fmt.Sprintf("scraping-jsons/%s/", strings.ToUpper(ticker))
	err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, obj := range page.Contents {
			// read just the first 2 KB of each JSON object
			out, err := svc.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
				Range:  aws.String("bytes=0-2047"),
			})
			if err != nil {
				continue // skip on error
			}
			url := extractURL(out.Body)
			out.Body.Close()
			if url != "" {
				seen[url] = true
			}
		}
		return true
	})
	return seen, err
}

// pull `"url"` field from small JSON snippet
func extractURL(r io.Reader) string {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanBytes)
	var buf bytes.Buffer
	for scanner.Scan() {
		buf.Write(scanner.Bytes())
		if bytes.Contains(buf.Bytes(), []byte(`"url"`)) {
			break
		}
	}
	var partial map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &partial); err == nil {
		if u, ok := partial["url"].(string); ok {
			return u
		}
	}
	return ""
}

// ── helper to load companies.json ───────────────────────────────
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

	log.Printf("🌱 Starting ESG pre-processor (mode=%s)…", *mode)

	companies, err := LoadCompanies("companies.json")
	if err != nil {
		log.Fatalf("❌ load companies.json: %v", err)
	}
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("❌ load config: %v", err)
	}

	// ---- 1️⃣ domain discovery -----------------------------------
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

			seen, _ := LoadSeenURLs("fin-processed-esg-reports", c.Ticker)

			log.Printf("🔍 Google ESG search for %s …", c.Name)
			results, err := SearchESGLinks(c.Name, cfg)
			if err != nil {
				log.Printf("❌ search %s: %v", c.Ticker, err)
				continue
			}

			newCount := 0
			for _, item := range results {
				if seen[item.Link] {
					continue // duplicate => ignore completely
				}

				rec := ESGLinkRecord{
					Ticker:       c.Ticker,
					Title:        item.Title,
					URL:          item.Link,
					RetrievedUTC: time.Now().UTC().Format(time.RFC3339),
					Source:       "ESG",
					State:        "raw",
				}
				if d := inferDateFromURL(item.Link); d != "" {
					rec.FilingDate = d
				} else {
					rec.FilingDate = time.Now().Format("2006-01-02")
				}

				key := linkS3Key(c.Name, c.Ticker, item.Title, item.Link, time.Now().UTC())
				if err := PutLinkRecordToS3("fin-processed-esg-reports", key, rec); err != nil {
					log.Printf("❌ S3 put %s: %v", key, err)
					continue
				}
				if err := SendLinkRecord(cfg.KinesisStream, rec); err != nil {
					log.Printf("❌ Kinesis push %s: %v", c.Ticker, err)
				}
				newCount++
			}
			log.Printf("✅ ESG links stored for %s (new=%d totalGoogle=%d)", c.Ticker, newCount, len(results))
		}
	}

	log.Println("🎉 Pre-processing complete.")
}
