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
)

type SearchItem struct {
	Title       string `json:"title"`
	Link        string `json:"link"`
	DisplayLink string `json:"displayLink"`
}

func DownloadFromJSON(ticker string, data []byte) error {
	var items []SearchItem
	if err := json.Unmarshal(data, &items); err != nil {
		return fmt.Errorf("failed to parse JSON: %w", err)
	}

	outputDir := filepath.Join("esg-content", ticker)
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
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
