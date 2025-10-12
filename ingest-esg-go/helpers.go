package main

// helpers.go consolidates a handful of free‑standing functions that were
// previously tucked away in utils/main.go. These helpers support
// slugification of names, construction of S3 keys, inference of
// filing dates from URLs and fetching already seen URLs from S3. They
// are grouped here to make them accessible to both runner_esg.go and
// other top‑level files without introducing a new package.

import (
    "bufio"
    "bytes"
    "crypto/sha1"
    "encoding/hex"
    "encoding/json"
    "fmt"
    "io"
    "regexp"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/s3"
)

var (
    badChars    = regexp.MustCompile(`[^A-Za-z0-9_-]+`)
    datePattern = regexp.MustCompile(`\b(19[0-9]{2}|20[0-9]{2}|2100)\b`)
)

// slugify converts an arbitrary string into a safe filename component by
// replacing runs of non alphanumeric characters with underscores and
// trimming any leading or trailing underscores. It mirrors the
// behaviour of the original utils/main.go implementation.
func slugify(s string) string {
    return strings.Trim(badChars.ReplaceAllString(strings.TrimSpace(s), "_"), "_")
}

// linkS3Key builds the S3 key for a scraped ESG link. The layout is
// scraping-jsons/<TICKER>/<Company_Title>-<hash>-YYYY-MM-DD.json where
// the hash is a truncated SHA‑1 of the URL. The company and title are
// slugified to remove unsafe characters. The caller must provide the
// ingest timestamp so that multiple uploads on the same day share the
// same date suffix.
func linkS3Key(company, ticker, title, u string, t time.Time) string {
    h := sha1.Sum([]byte(u))
    return fmt.Sprintf("scraping-jsons/%s/%s_%s-%s-%04d-%02d-%02d.json",
        strings.ToUpper(ticker),
        slugify(company),
        slugify(title),
        hex.EncodeToString(h[:])[:8],
        t.Year(), int(t.Month()), t.Day(),
    )
}

// inferDateFromURL attempts to extract a year or full date from a URL.
// If a year is found it returns YYYY-01-01 as a fallback. If a
// YYYY-MM-DD or YYYY/MM/DD pattern is found it returns that exact
// date. Otherwise an empty string is returned. The patterns are
// conservative to avoid false positives.
func inferDateFromURL(u string) string {
    if m := datePattern.FindString(u); m != "" {
        return fmt.Sprintf("%s-01-01", m)
    }
    reFull := regexp.MustCompile(`\b(19[0-9]{2}|20[0-9]{2}|2100)[/-](0[1-9]|1[0-2])[/-](0[1-9]|[12][0-9]|3[01])\b`)
    if m := reFull.FindStringSubmatch(u); m != nil {
        return fmt.Sprintf("%s-%s-%s", m[1], m[2], m[3])
    }
    return ""
}

// LoadSeenURLs lists previously saved ESG link JSON files for a given
// ticker in the provided S3 bucket and returns a map of URL → true.
// Only the first 2KB of each file is read to locate the URL field.
// Errors during listing or reading objects are returned. The caller
// decides whether to ignore or abort on such errors. Region defaults
// to us-east-1 for backwards compatibility; AWS_REGION can override it
// globally via the SDK.
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

// extractURL scans a small prefix of a JSON file and extracts the
// "url" field. It assumes the URL appears early in the file. This
// helper mirrors the original implementation from utils/main.go.
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