package utils

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
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

		// Download the file
		resp, err := http.Get(item.Link)
		if err != nil {
			fmt.Printf("❌ Failed to download %s: %v\n", item.Link, err)
			continue
		}
		defer resp.Body.Close()

		// Save file locally
		localDir := filepath.Join("ingest-esg-go", "esg-content", ticker)
		if err := os.MkdirAll(localDir, os.ModePerm); err != nil {
			fmt.Printf("❌ Failed to create local dir: %v\n", err)
			continue
		}

		localPath := filepath.Join(localDir, cleanName)
		out, err := os.Create(localPath)
		if err != nil {
			fmt.Printf("❌ Failed to create local file: %v\n", err)
			continue
		}
		_, err = io.Copy(out, resp.Body)
		out.Close()
		if err != nil {
			fmt.Printf("❌ Failed to write local file: %v\n", err)
			continue
		}
		fmt.Printf("💾 Saved file locally: %s\n", localPath)

		// Extract text directly to memory (stdout)
		cmd := exec.Command(
			"C:\\Users\\Agasthya\\Desktop\\esg-finguard\\fin-guard\\venv\\Scripts\\python.exe",
			"ingest-esg-go\\utils\\extractor.py",
			localPath,
		)
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Printf("❌ Text extraction failed for %s:\n%s\n", cleanName, string(output))
			continue
		}

		// Upload extracted text to S3
		textS3Key := fmt.Sprintf("texts/%s/%s.txt", ticker, strings.TrimSuffix(cleanName, filepath.Ext(cleanName)))
		_, err = uploader.Upload(&s3manager.UploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(textS3Key),
			Body:   bytes.NewReader(output),
		})
		if err != nil {
			fmt.Printf("❌ Failed to upload extracted text to S3: %v\n", err)
		} else {
			fmt.Printf("📤 Uploaded extracted text to s3://%s/%s\n", bucket, textS3Key)
		}
	}

	return nil
}
