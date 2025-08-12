package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Cookie579/fin-guard/ingest-esg-go/utils"

	// AWS SDK v1
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ----------------------------- Models -----------------------------

// Domain file example:
// {"company":"Apple","ticker":"AAPL","domain":"www.apple.com","source":"DOMAIN","ingest_ts":"2025-08-09T03:38:03Z"}
type DomainFile struct {
	Company  string `json:"company"`
	Ticker   string `json:"ticker"`
	Domain   string `json:"domain"`
	Source   string `json:"source"`
	IngestTS string `json:"ingest_ts"`
}

// For quick stage filtering when present
type esgStageProbe struct {
	Stage string `json:"stage"`
}

// ------------------------- Main Pipeline --------------------------

func main() {
	log.Println("🌱 Starting ESG ingest (download-only; no extractor)…")

	const (
		region        = "us-east-1"
		bucket        = "fin-processed-esg-reports"
		domainPrefix  = "company_domains/" // per-ticker *-domain-*.json
		esgBasePrefix = "scraping-jsons/"  // ESG JSONs under scraping-jsons/<TICKER>/
		maxWorkers    = 16
		processNonRaw = false // set true to also process non-"raw" stages
	)

	// AWS clients (v1)
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatalf("❌ AWS session error: %v", err)
	}
	s3cli := s3.New(sess)

	// 1) List domain files under company_domains/
	allDomainKeys, err := listAllKeysV1(s3cli, bucket, domainPrefix)
	if err != nil {
		log.Fatalf("❌ Failed listing domain files: %v", err)
	}

	// Keep only "*-domain-*.json"
	var domainKeys []string
	for _, k := range allDomainKeys {
		base := path.Base(k)
		if strings.Contains(base, "-domain-") && strings.HasSuffix(strings.ToLower(base), ".json") {
			domainKeys = append(domainKeys, k)
		}
	}
	if len(domainKeys) == 0 {
		log.Fatalf("❌ No domain files found under %q.", domainPrefix)
	}
	log.Printf("📂 Using domain prefix: %s (found %d domain files)", domainPrefix, len(domainKeys))

	// 2) Load & dedupe to latest per ticker by ingest_ts
	tickerLatest := make(map[string]DomainFile)
	for _, key := range domainKeys {
		jb, err := utils.LoadJSONFromS3(bucket, key)
		if err != nil {
			log.Printf("⚠️  Skipping domain file %s: load error: %v", key, err)
			continue
		}
		var df DomainFile
		if err := json.Unmarshal(jb, &df); err != nil {
			log.Printf("⚠️  Skipping domain file %s: unmarshal error: %v", key, err)
			continue
		}
		tk := strings.ToUpper(strings.TrimSpace(df.Ticker))
		if tk == "" {
			tk = strings.ToUpper(strings.Split(path.Base(key), "-")[0]) // AAPL-domain-...
		}
		if tk == "" {
			log.Printf("⚠️  Skipping domain file %s: couldn't determine ticker", key)
			continue
		}
		ex, ok := tickerLatest[tk]
		if !ok || parseRFC3339(df.IngestTS).After(parseRFC3339(ex.IngestTS)) {
			tickerLatest[tk] = df
		}
	}

	if len(tickerLatest) == 0 {
		log.Fatalf("❌ No usable ticker domain files after parsing.")
	}
	log.Printf("🔎 Found %d tickers.", len(tickerLatest))

	// 3) Worker pool to process all ESG JSONs strictly under scraping-jsons/<TICKER>/
	type task struct{ Ticker, Key string }
	tasksCh := make(chan task, 4096)
	var wg sync.WaitGroup

	// HTTP clients: default (HTTP/2 on) + fallback (HTTP/1.1 only)
	httpClient := newHTTPClient(false)
	httpClientH1 := newHTTPClient(true)

	for i := 0; i < maxWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for t := range tasksCh {
				jb, err := utils.LoadJSONFromS3(bucket, t.Key)
				if err != nil {
					log.Printf("❌ [%d] %s: load error for %s: %v", workerID, t.Ticker, t.Key, err)
					continue
				}

				// Optional stage filter (if present and not "raw", skip)
				if !processNonRaw {
					var probe esgStageProbe
					_ = json.Unmarshal(jb, &probe)
					stage := strings.ToLower(strings.TrimSpace(probe.Stage))
					if stage != "" && stage != "raw" {
						continue
					}
				}

				// Normalize to array of objects
				norm := normalizeToJSONArray(jb)
				if norm == nil {
					log.Printf("⚠️  [%d] %s: skipping %s (unrecognized JSON root)", workerID, t.Ticker, t.Key)
					continue
				}

				var arr []map[string]interface{}
				if err := json.Unmarshal(norm, &arr); err != nil {
					log.Printf("❌ [%d] %s: unmarshal array failed for %s: %v", workerID, t.Ticker, t.Key, err)
					continue
				}

				// Prepare download dir
				saveDir := filepath.Join("esg-content", t.Ticker)
				_ = os.MkdirAll(saveDir, 0o755)

				// For each item, extract URL & title and download
				for _, it := range arr {
					u := getURLFromItem(it)
					if u == "" {
						continue
					}
					title := getTitleFromItem(it)
					if title == "" {
						title = deriveNameFromURL(u)
					}
					fileBase := slugify(title)
					if fileBase == "" {
						fileBase = "esg_01"
					}

					if err := downloadWithRetries(httpClient, httpClientH1, u, saveDir, fileBase); err != nil {
						log.Printf("❌ Failed to download %s: %v", u, err)
						continue
					}
				}

				log.Printf("✅ [%d] %s: processed %s", workerID, t.Ticker, t.Key)
			}
		}(i + 1)
	}

	// 4) Enqueue all JSONs under scraping-jsons/<TICKER>/
	totalQueued := 0
	for tk := range tickerLatest {
		pfx := esgBasePrefix + tk + "/"
		keys, err := listAllKeysV1(s3cli, bucket, pfx)
		if err != nil {
			log.Printf("⚠️  %s: list error under %q: %v", tk, pfx, err)
			continue
		}
		queued := 0
		for _, k := range keys {
			if strings.HasSuffix(strings.ToLower(k), ".json") {
				tasksCh <- task{Ticker: tk, Key: k}
				queued++
			}
		}
		totalQueued += queued
		if queued == 0 {
			log.Printf("ℹ️  %s: no ESG JSON objects found under %q", tk, pfx)
		} else {
			log.Printf("📦 %s: queued %d JSON objects from %q", tk, queued, pfx)
		}
	}

	close(tasksCh)
	wg.Wait()
	log.Printf("🎉 Done. Total ESG JSON objects processed (attempted): %d", totalQueued)
}

// ------------------------- S3 Utilities --------------------------

func listAllKeysV1(s3cli *s3.S3, bucket, prefix string) ([]string, error) {
	var keys []string
	var token *string
	for {
		out, err := s3cli.ListObjectsV2(&s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range out.Contents {
			if obj.Key != nil {
				keys = append(keys, *obj.Key)
			}
		}
		if out.IsTruncated != nil && *out.IsTruncated && out.NextContinuationToken != nil {
			token = out.NextContinuationToken
		} else {
			break
		}
	}
	return keys, nil
}

// --------------------------- Helpers -----------------------------

func parseRFC3339(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, strings.TrimSpace(s))
	return t
}

func normalizeToJSONArray(b []byte) []byte {
	// trim leading spaces
	i := 0
	for i < len(b) && (b[i] == ' ' || b[i] == '\n' || b[i] == '\t' || b[i] == '\r') {
		i++
	}
	if i >= len(b) {
		return nil
	}
	switch b[i] {
	case '[':
		return b
	case '{':
		out := make([]byte, 0, len(b)+2)
		out = append(out, '[')
		out = append(out, b...)
		out = append(out, ']')
		return out
	default:
		return nil
	}
}

func firstNonEmptyString(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return s
			}
		}
	}
	return ""
}

func getURLFromItem(it map[string]interface{}) string {
	u := firstNonEmptyString(it, "url", "link", "linkUrl", "link_to_filing_details", "linkToFilingDetails")
	u = strings.TrimSpace(u)
	if u == "" {
		return ""
	}
	if strings.HasPrefix(u, "www.") {
		u = "https://" + u
	}
	if !(strings.HasPrefix(u, "http://") || strings.HasPrefix(u, "https://")) {
		return ""
	}
	return u
}

func getTitleFromItem(it map[string]interface{}) string {
	return firstNonEmptyString(it, "title", "headline", "name", "fileName")
}

func deriveNameFromURL(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || parsed == nil {
		return "esg_01"
	}
	last := path.Base(parsed.Path)
	last = strings.TrimSuffix(last, path.Ext(last))
	if last == "" || last == "/" || last == "." || last == ".." {
		last = parsed.Host
	}
	return last
}

func slugify(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	// Replace separators with dash
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")
	// Keep only [a-z0-9-]
	out := make([]rune, 0, len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z':
			out = append(out, r)
		case r >= '0' && r <= '9':
			out = append(out, r)
		case r == '-':
			out = append(out, r)
		default:
			// drop
		}
	}
	res := string(out)
	res = strings.Trim(res, "-")
	if res == "" {
		return "esg_01"
	}
	if len(res) > 120 {
		res = res[:120]
	}
	return res
}

// ----------------------- HTTP Downloading ------------------------

func newHTTPClient(disableHTTP2 bool) *http.Client {
	tr := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		// reasonable timeouts
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,
		IdleConnTimeout:       90 * time.Second,
		MaxIdleConns:          100,
		MaxConnsPerHost:       0,
		DisableCompression:    false,
		TLSClientConfig:       &tls.Config{MinVersion: tls.VersionTLS12},
	}
	// Force HTTP/1.1 if requested (workaround for h2 INTERNAL_ERROR)
	if disableHTTP2 {
		tr.ForceAttemptHTTP2 = false
		// disable http2 entirely
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{}
	}

	return &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second, // whole request timeout
	}
}

func downloadWithRetries(client, clientH1 *http.Client, rawURL, dir, fileBase string) error {
	const maxAttempts = 4
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		cl := client
		// Attempts 1-2: normal; attempts 3-4: HTTP/1.1 only
		if attempt >= 3 {
			cl = clientH1
		}
		err := downloadOnce(cl, rawURL, dir, fileBase)
		if err == nil {
			return nil
		}
		lastErr = err

		// brief backoff
		time.Sleep(time.Duration(150*attempt) * time.Millisecond)
	}
	return fmt.Errorf("%w (after %d attempts)", lastErr, maxAttempts)
}

func downloadOnce(client *http.Client, rawURL, dir, fileBase string) error {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return err
	}

	// Friendly headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	// Consider 2xx only success
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		// Some sites block with 403/429; let retries handle
		return fmt.Errorf("http %d", resp.StatusCode)
	}

	// Decide extension from content-type or URL
	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	urlPath := req.URL.Path
	ext := pickExt(ct, urlPath)
	filename := ensureUnique(filepath.Join(dir, fileBase+ext))

	// Stream to file with size limit (100 MB)
	const maxBytes = int64(100 * 1024 * 1024)
	out, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, io.LimitReader(resp.Body, maxBytes))
	if err != nil {
		return err
	}

	log.Printf("💾 Saved file locally: %s", filepath.ToSlash(filename))
	return nil
}

func pickExt(contentType, urlPath string) string {
	// Prefer contentType mapping
	switch {
	case strings.Contains(contentType, "pdf"):
		return ".pdf"
	case strings.Contains(contentType, "html"):
		return ".html"
	case strings.Contains(contentType, "xml"):
		return ".xml"
	case strings.Contains(contentType, "json"):
		return ".json"
	case strings.Contains(contentType, "csv"):
		return ".csv"
	case strings.Contains(contentType, "plain"):
		return ".txt"
	case strings.Contains(contentType, "msword"):
		return ".doc"
	case strings.Contains(contentType, "officedocument.wordprocessingml"):
		return ".docx"
	case strings.Contains(contentType, "officedocument.spreadsheetml"):
		return ".xlsx"
	case strings.Contains(contentType, "officedocument.presentationml"):
		return ".pptx"
	}

	// Fallback to URL path extension if present
	ext := strings.ToLower(path.Ext(urlPath))
	switch ext {
	case ".pdf", ".html", ".htm", ".xml", ".json", ".csv", ".txt", ".doc", ".docx", ".xlsx", ".pptx":
		return ext
	default:
		return ".html" // default best-effort
	}
}

func ensureUnique(fullpath string) string {
	if _, err := os.Stat(fullpath); os.IsNotExist(err) {
		return fullpath
	}
	dir := filepath.Dir(fullpath)
	base := strings.TrimSuffix(filepath.Base(fullpath), filepath.Ext(fullpath))
	ext := filepath.Ext(fullpath)
	for i := 1; ; i++ {
		candidate := filepath.Join(dir, fmt.Sprintf("%s_%d%s", base, i, ext))
		if _, err := os.Stat(candidate); os.IsNotExist(err) {
			return candidate
		}
	}
}
