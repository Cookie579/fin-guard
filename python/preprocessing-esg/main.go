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
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

/* ─────────────────────── helpers ──────────────────────────── */

var (
	badChars    = regexp.MustCompile(`[^A-Za-z0-9_-]+`)
	datePattern = regexp.MustCompile(`\b(19[0-9]{2}|20[0-9]{2}|2100)\b`)
)

func slugify(s string) string {
	return strings.Trim(badChars.ReplaceAllString(strings.TrimSpace(s), "_"), "_")
}

// scraping-jsons/<TICKER>/<Company_Title>-<hash>-YYYY-MM-DD.json
func linkS3Key(company, ticker, title, u string, t time.Time) string {
	h := sha1.Sum([]byte(u))
	return fmt.Sprintf("scraping-jsons/%s/%s_%s-%s-%04d-%02d-%02d.json",
		strings.ToUpper(ticker),
		slugify(company),
		slugify(title),
		hex.EncodeToString(h[:])[:8],
		t.Year(), int(t.Month()), t.Day())
}

func inferDateFromURL(u string) string {
	// Look for year first
	if m := datePattern.FindString(u); m != "" {
		return fmt.Sprintf("%s-01-01", m) // default to Jan 1 if no month/day
	}

	// Then try full YYYY-MM-DD or YYYY/MM/DD
	reFull := regexp.MustCompile(`\b(19[0-9]{2}|20[0-9]{2}|2100)[/-](0[1-9]|1[0-2])[/-](0[1-9]|[12][0-9]|3[01])\b`)
	if m := reFull.FindStringSubmatch(u); m != nil {
		return fmt.Sprintf("%s-%s-%s", m[1], m[2], m[3])
	}

	return ""
}

/* ───── pull URLs we already have for a ticker ─────────────── */

func LoadSeenURLs(bucket, ticker string) (map[string]bool, error) {
	seen := make(map[string]bool)
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
	svc := s3.New(sess)

	prefix := fmt.Sprintf("scraping-jsons/%s/", strings.ToUpper(ticker))
	err := svc.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	}, func(p *s3.ListObjectsV2Output, _ bool) bool {
		for _, obj := range p.Contents {
			out, err := svc.GetObject(&s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    obj.Key,
				Range:  aws.String("bytes=0-2047"),
			})
			if err != nil {
				continue
			}
			if u := extractURL(out.Body); u != "" {
				seen[u] = true
			}
			out.Body.Close()
		}
		return true
	})
	return seen, err
}

func extractURL(r io.Reader) string {
	sc := bufio.NewScanner(r)
	sc.Split(bufio.ScanBytes)
	var buf bytes.Buffer
	for sc.Scan() {
		buf.Write(sc.Bytes())
		if bytes.Contains(buf.Bytes(), []byte(`"url"`)) {
			break
		}
	}
	var tmp map[string]any
	if json.Unmarshal(buf.Bytes(), &tmp) == nil {
		if u, ok := tmp["url"].(string); ok {
			return u
		}
	}
	return ""
}

/* ─── helper to load companies.json ────────────────────────── */

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

/* ─────────────────────────── main ────────────────────────── */

func main() {
	mode := flag.String("mode", "esg+domain", "domain | esg | esg+domain")
	flag.Parse()

	log.Printf("🌱 ESG pre-processor starting (mode=%s)…", *mode)

	companies, err := LoadCompanies("companies.json")
	if err != nil {
		log.Fatalf("load companies.json: %v", err)
	}
	cfg, err := LoadConfig()
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	/* 0️⃣  blacklist (optional) */
	blacklist, err := LoadBlacklist("blacklist.txt")
	if err != nil {
		log.Printf("⚠️  no blacklist loaded (%v); proceeding without it", err)
		blacklist = map[string]struct{}{}
	} else {
		log.Printf("🛡️  loaded %d blacklist domains", len(blacklist))
	}

	/* 1️⃣  domain discovery (unchanged) */
	if *mode == "domain" || *mode == "esg+domain" {
		for _, c := range companies {
			if _, skipped, err := FindDomain(c, cfg); err != nil {
				log.Printf("domain lookup %s: %v", c.Ticker, err)
			} else if !skipped {
				log.Printf("domain saved for %s", c.Ticker)
			}
		}
	}

	/* 2️⃣  ESG scraping ---------------------------------------- */
	if *mode == "esg" || *mode == "esg+domain" {
		for _, c := range companies {

			// URLs we already saved for this ticker → skip re-uploads
			seen, _ := LoadSeenURLs("fin-processed-esg-reports", c.Ticker)

			log.Printf("🔍 Google ESG search for %s …", c.Name)
			results, err := SearchESGLinks(c.Name, cfg)
			if err != nil {
				log.Printf("search %s: %v", c.Ticker, err)
				continue
			}

			newCount := 0
			for _, item := range results {
				// 2a) skip if we've already uploaded this exact URL
				if seen[item.Link] {
					continue
				}

				// 2b) skip if host is blacklisted
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

				// 2c) build record and save
				rec := ESGLinkRecord{
					Ticker:   c.Ticker,
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

				key := linkS3Key(c.Name, c.Ticker, item.Title, item.Link, time.Now().UTC())
				if err := PutLinkRecordToS3("fin-processed-esg-reports", key, rec); err != nil {
					log.Printf("S3 put %s: %v", key, err)
					continue
				}
				if err := SendLinkRecord(cfg.KinesisStream, rec); err != nil {
					log.Printf("Kinesis push %s: %v", c.Ticker, err)
				}
				newCount++
			}
			log.Printf("✅ %s (new=%d / serp=%d)", c.Ticker, newCount, len(results))
		}
	}

	log.Println("🎉 Pre-processing complete.")
}
