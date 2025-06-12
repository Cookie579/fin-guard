package utils

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/Cookie579/fin-guard/ingest-esg-go/discovery"
)

// DownloadAllESGContent loads from existing JSON and downloads files
func DownloadAllESGContent(ticker string) error {
	// Step 1: Check if results JSON exists
	resultPath := filepath.Join("esg-results", fmt.Sprintf("%s_esg.json", ticker))
	file, err := os.Open(resultPath)
	if err != nil {
		fmt.Printf("⚠️  No saved ESG results for %s — skipping download.\n", ticker)
		return nil
	}
	defer file.Close()

	var items []discovery.GoogleSearchItem
	if err := json.NewDecoder(file).Decode(&items); err != nil {
		fmt.Printf("❌ Failed to parse results for %s: %v\n", ticker, err)
		return err
	}

	// Step 2: Prepare download folder
	outputDir := filepath.Join("esg-content", ticker)
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
	}

	usedNames := make(map[string]int)

	// Step 3: Download each link
	for idx, item := range items {
		resp, err := http.Get(item.Link)
		if err != nil {
			fmt.Printf("❌ Failed to download %s: %v\n", item.Link, err)
			continue
		}
		defer resp.Body.Close()

		// Try to extract filename from URL
		parsedURL, err := url.Parse(item.Link)
		filename := ""
		if err == nil {
			filename = path.Base(parsedURL.Path)
			filename = strings.Split(filename, "?")[0] // remove query params
		}

		// Fallback filename
		if filename == "" || !strings.Contains(filename, ".") {
			ext := ".html"
			if strings.Contains(strings.ToLower(item.Link), ".pdf") {
				ext = ".pdf"
			}
			filename = fmt.Sprintf("esg_%02d%s", idx+1, ext)
		}

		// Handle duplicates
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

		filePath := filepath.Join(outputDir, cleanName)
		out, err := os.Create(filePath)
		if err != nil {
			fmt.Printf("❌ Failed to create file: %v\n", err)
			continue
		}
		defer out.Close()

		_, err = io.Copy(out, resp.Body)
		if err != nil {
			fmt.Printf("❌ Failed to write content: %v\n", err)
			continue
		}

		fmt.Printf("✅ Downloaded: %s\n", filePath)
	}

	return nil
}
