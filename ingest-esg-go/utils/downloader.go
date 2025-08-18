package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"

	// AWS SDK v1
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type DomainFile struct {
	Company  string `json:"company"`
	Ticker   string `json:"ticker"`
	Domain   string `json:"domain"`
	Source   string `json:"source"`
	IngestTS string `json:"ingest_ts"`
}

type esgStageProbe struct {
	Stage string `json:"stage"`
}

func download() {
	log.Println("🌱 Starting ESG ingest (download-only; no extractor)…")

	const (
		region        = "us-east-1"
		bucket        = "fin-processed-esg-reports"
		domainPrefix  = "company_domains/" // per-ticker *-domain-*.json
		esgBasePrefix = "scraping-jsons/"  // ESG JSONs under scraping-jsons/<TICKER>/
		maxWorkers    = 16
		processNonRaw = false // set true to also process non-"raw" stages
		streamName    = "fin-guard-stream"
	)

	// AWS clients
	sess, err := session.NewSession(&aws.Config{Region: aws.String(region)})
	if err != nil {
		log.Fatalf("❌ AWS session error: %v", err)
	}
	s3cli := s3.New(sess)
	uploader := NewS3Uploader(region)

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
		jb, err := LoadJSONFromS3(bucket, key)
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
			// fallback: AAPL-domain-...  -> "AAPL"
			parts := strings.SplitN(path.Base(key), "-", 2)
			if len(parts) > 0 {
				tk = strings.ToUpper(strings.TrimSpace(parts[0]))
			}
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
	type task struct {
		Ticker  string
		Company string
		Key     string
	}
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
				jb, err := LoadJSONFromS3(bucket, t.Key)
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

				for _, it := range arr {
					u := getURLFromItem(it)
					if u == "" {
						continue
					}
					title := getTitleFromItem(it)
					if title == "" {
						title = deriveNameFromURL(u)
					}
					fileBase := slugify_download(title)
					if fileBase == "" {
						fileBase = "esg_01"
					}

					// filing date only (no year in payload)
					fd := firstNonEmptyString(it, "filingDate", "filingdate")
					if strings.TrimSpace(fd) == "" {
						fd = inferDateFromURL(u)
					}
					if strings.TrimSpace(fd) == "" {
						fd = time.Now().Format("2006-01-02")
					}

					// build S3 key prefix and upload
					// S3 key: esg-reports/<TICKER>/<fileBase>.<ext>
					prefix := fmt.Sprintf("esg-reports/%s/", strings.ToUpper(t.Ticker))

					key, nbytes, ctype, ext, err := downloadUploadWithRetries(
						httpClient, httpClientH1, uploader, s3cli, bucket, prefix, fileBase, u,
					)
					if err != nil {
						log.Printf("❌ [%d] %s: failed upload for %s: %v", workerID, t.Ticker, u, err)
						continue
					}

					// Kinesis metadata payload (NO 'year' field)
					now := time.Now().UTC().Format(time.RFC3339)
					fileType := strings.TrimPrefix(ext, ".")
					meta := map[string]interface{}{
						"company":      t.Company,
						"form":         "",
						"filingDate":   fd, // <-- only this; no 'year'
						"accession":    "",
						"title":        title,
						"url":          u,
						"txt_s3_key":   key,
						"txt_bytes":    nbytes,
						"ingest_ts":    now,
						"snippet":      "",
						"ticker":       t.Ticker,
						"source":       "ESG",
						"stage":        "download",
						"file_type":    fileType,
						"content_type": ctype,
						"s3_uri":       fmt.Sprintf("s3://%s/%s", bucket, key),
					}
					data, _ := json.Marshal(meta)
					if err := SendToStreamRaw(streamName, t.Ticker, data); err != nil {
						log.Printf("⚠️  [%d] %s: Kinesis send failed: %v", workerID, t.Ticker, err)
					} else {
						log.Printf("📤 [%d] %s: sent download meta to Kinesis", workerID, t.Ticker)
					}
				}

				log.Printf("✅ [%d] %s: processed %s", workerID, t.Ticker, t.Key)
			}
		}(i + 1)
	}

	// 4) Enqueue all JSONs under scraping-jsons/<TICKER>/
	totalQueued := 0
	for tk, df := range tickerLatest {
		pfx := esgBasePrefix + tk + "/"
		keys, err := listAllKeysV1(s3cli, bucket, pfx)
		if err != nil {
			log.Printf("⚠️  %s: list error under %q: %v", tk, pfx, err)
			continue
		}
		queued := 0
		for _, k := range keys {
			if strings.HasSuffix(strings.ToLower(k), ".json") {
				tasksCh <- task{Ticker: tk, Company: df.Company, Key: k}
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

// ------------------------- S3 + HTTP helpers ---------------------

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
	if disableHTTP2 {
		tr.ForceAttemptHTTP2 = false
		tr.TLSNextProto = map[string]func(string, *tls.Conn) http.RoundTripper{} // disable h2
	}
	return &http.Client{
		Transport: tr,
		Timeout:   60 * time.Second,
	}
}

// download + upload (with retries). returns final S3 key, bytes, content-type, ext
func downloadUploadWithRetries(
	client, clientH1 *http.Client,
	uploader *s3manager.Uploader,
	s3cli *s3.S3,
	bucket, prefix, fileBase, rawURL string,
) (string, int64, string, string, error) {

	const maxAttempts = 4
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		cl := client
		if attempt >= 3 {
			cl = clientH1
		}
		key, n, ctype, ext, err := downloadOnceToS3(cl, uploader, s3cli, bucket, prefix, fileBase, rawURL)
		if err == nil {
			return key, n, ctype, ext, nil
		}
		lastErr = err
		time.Sleep(time.Duration(150*attempt) * time.Millisecond)
	}
	return "", 0, "", "", fmt.Errorf("%w (after %d attempts)", lastErr, maxAttempts)
}

// counts bytes written via io.TeeReader
type countWriter struct{ n int64 }

func (w *countWriter) Write(p []byte) (int, error) {
	w.n += int64(len(p))
	return len(p), nil
}

func downloadOnceToS3(
	client *http.Client,
	uploader *s3manager.Uploader,
	s3cli *s3.S3,
	bucket, prefix, fileBase, rawURL string,
) (string, int64, string, string, error) {

	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return "", 0, "", "", err
	}
	// Friendly headers
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "*/*")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := client.Do(req)
	if err != nil {
		return "", 0, "", "", err
	}
	defer resp.Body.Close()

	// 2xx only
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", 0, "", "", fmt.Errorf("http %d", resp.StatusCode)
	}

	// Hard-cap protection: if server tells us it's too large, abort before upload.
	const maxBytes = int64(100 * 1024 * 1024) // 100 MB
	if resp.ContentLength > 0 && resp.ContentLength > maxBytes {
		// optionally drain a bit to allow connection reuse, but not required
		return "", 0, "", "", fmt.Errorf("content-length %d exceeds cap %d", resp.ContentLength, maxBytes)
	}

	ct := strings.ToLower(resp.Header.Get("Content-Type"))
	urlPath := req.URL.Path
	ext := pickExt(ct, urlPath)
	fileName := fileBase + ext
	key := prefix + fileName

	// ensure S3 key uniqueness (_1, _2, …) if collision
	key, err = ensureS3UniqueKey(s3cli, bucket, key)
	if err != nil {
		return "", 0, "", "", err
	}

	// Limit + count while uploading (guards unknown-length streams)
	lr := &io.LimitedReader{R: resp.Body, N: maxBytes}
	cw := &countWriter{}
	tr := io.TeeReader(lr, cw)

	ctx := context.Background()
	if err := UploadToS3(ctx, uploader, bucket, key, tr, ct); err != nil {
		return "", 0, "", "", err
	}

	// If LimitedReader hit the cap exactly and server didn't give a length, warn.
	// (We already aborted above when Content-Length was known and > cap.)
	if lr.N == 0 && (resp.ContentLength < 0 || resp.ContentLength > maxBytes) {
		log.Printf("⚠️  uploaded exactly %d bytes for %s; source size unknown or larger — potential truncation", maxBytes, rawURL)
	}

	return key, cw.n, ct, ext, nil
}

func ensureS3UniqueKey(s3cli *s3.S3, bucket, key string) (string, error) {
	// HEAD once; if exists, add suffixes _1, _2, …
	tryKey := key
	for i := 1; i <= 1000; i++ {
		_, err := s3cli.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(tryKey),
		})
		if err != nil {
			// assume NotFound or other error -> use this key
			return tryKey, nil
		}
		// exists; add suffix
		dot := strings.LastIndex(tryKey, ".")
		if dot <= 0 {
			tryKey = fmt.Sprintf("%s_%d", key, i)
		} else {
			tryKey = fmt.Sprintf("%s_%d%s", tryKey[:dot], i, tryKey[dot:])
		}
	}
	return "", fmt.Errorf("could not ensure unique key for %s after many attempts", key)
}

// --------------------------- JSON helpers ------------------------

func parseRFC3339(s string) time.Time {
	t, _ := time.Parse(time.RFC3339, strings.TrimSpace(s))
	return t
}

func normalizeToJSONArray(b []byte) []byte {
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

func slugify_download(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	s = strings.ReplaceAll(s, " ", "-")
	s = strings.ReplaceAll(s, "_", "-")
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
		}
	}
	res := strings.Trim(string(out), "-")
	if res == "" {
		return "esg_01"
	}
	if len(res) > 120 {
		res = res[:120]
	}
	return res
}

func pickExt(contentType, urlPath string) string {
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
	ext := strings.ToLower(path.Ext(urlPath))
	switch ext {
	case ".pdf", ".html", ".htm", ".xml", ".json", ".csv", ".txt", ".doc", ".docx", ".xlsx", ".pptx":
		return ext
	default:
		return ".html"
	}
}
