package utils

import (
	"context"
	"encoding/json"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

type SearchItem struct {
	Title       string `json:"title"`
	Link        string `json:"link"`
	DisplayLink string `json:"displayLink"`
}

func DownloadFromJSON(ticker string, data []byte, uploader *s3manager.Uploader, bucket string) error {
	var items []SearchItem
	if err := json.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	usedNames := make(map[string]int)

	for idx, item := range items {
		resp, err := http.Get(item.Link)
		if err != nil {
			fmt.Printf("❌ Failed to download %s: %v\n", item.Link, err)
			continue
		}
		defer resp.Body.Close()

		parsedURL, _ := url.Parse(item.Link)
		filename := path.Base(parsedURL.Path)
		filename = strings.Split(filename, "?")[0]

		if filename == "" || !strings.Contains(filename, ".") {
			ext := ".html"
			if strings.Contains(strings.ToLower(item.Link), ".pdf") {
				ext = ".pdf"
			}
			filename = fmt.Sprintf("esg_%02d%s", idx+1, ext)
		}

		cleanName := filename
		if count, exists := usedNames[filename]; exists {
			count++
			ext := filepath.Ext(filename)
			base := strings.TrimSuffix(filename, ext)
			cleanName = fmt.Sprintf("%s_%d%s", base, count, ext)
			usedNames[filename] = count
		} else {
			usedNames[filename] = 0
		}

		key := BuildS3Key(ticker, cleanName)

		contentType := resp.Header.Get("Content-Type")
		if contentType == "" {
			contentType = mime.TypeByExtension(filepath.Ext(cleanName))
		}

		err = UploadToS3(context.Background(), uploader, bucket, key, resp.Body, contentType)
		if err != nil {
			fmt.Printf("❌ Failed to upload to S3: %v\n", err)
			continue
		}

		fmt.Printf("✅ Uploaded to s3://%s/%s\n", bucket, key)
	}

	return nil
}
