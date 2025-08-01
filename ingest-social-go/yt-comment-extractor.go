//go:build ytextractor
// +build ytextractor

// yt-comment-extractor.go
//
//   go run -tags ytextractor yt-comment-extractor.go
//
// Groups comments from each video-dump (*V.json) by their own published date.
// Writes grouped output to:
//   data/raw/youtube/<ticker>/comments/<year>/YouTube_<TICKER>_<YYYY>_<MM>_<DD>_<N>.json
// Maintains an index (data/youtube_comment_index.json) so each video is only indexed once.

package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const indexFile = "data/youtube_comment_index.json"

/*─────────────────────────────
  Index helpers
─────────────────────────────*/

func loadIndex() map[string]struct{} {
	m := map[string]struct{}{}
	if b, err := os.ReadFile(indexFile); err == nil {
		_ = json.Unmarshal(b, &m)
	}
	return m
}

func saveIndex(m map[string]struct{}) {
	_ = os.MkdirAll(filepath.Dir(indexFile), 0o755)
	b, _ := json.MarshalIndent(m, "", "  ")
	_ = os.WriteFile(indexFile+".tmp", b, 0o644)
	_ = os.Rename(indexFile+".tmp", indexFile)
}

/*─────────────────────────────
  File-naming helpers
─────────────────────────────*/

func chooseCommentFile(dir, ticker, y, m, d string) string {
	prefix := fmt.Sprintf("YouTube_%s_%s_%s_%s_", ticker, y, m, d)
	files, _ := os.ReadDir(dir)
	n := 0
	for _, f := range files {
		if strings.HasPrefix(f.Name(), prefix) {
			n++
		}
	}
	return prefix + strconv.Itoa(n+1) + ".json"
}

/*─────────────────────────────
  Per-ticker processing
─────────────────────────────*/

func processTicker(ticker string, idx map[string]struct{}) error {
	root := filepath.Join("data", "raw", "youtube", ticker, "videos")
	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(d.Name(), "V.json") {
			return nil // skip dirs & metadata
		}
		if _, done := idx[path]; done {
			return nil // already indexed
		}

		f, _ := os.Open(path)
		sc := bufio.NewScanner(f)

		buckets := map[[3]string][]map[string]any{} // [y,m,d] -> []comments

		for sc.Scan() {
			var row map[string]any
			if json.Unmarshal(sc.Bytes(), &row) != nil {
				continue
			}

			metaName := filepath.Base(strings.TrimSuffix(path, ".json") + ".metadata.json")
			row["video_metadata"] = metaName

			pstr, _ := row["published"].(string)
			pt, err := time.Parse(time.RFC3339, pstr)
			if err != nil {
				continue
			}
			Y, M, D := pt.UTC().Format("2006"), pt.Format("01"), pt.Format("02")
			key := [3]string{Y, M, D}
			buckets[key] = append(buckets[key], row)
		}
		f.Close()

		// write grouped output
		for k, list := range buckets {
			dir := filepath.Join("data", "raw", "youtube", ticker, "comments", k[0])
			_ = os.MkdirAll(dir, 0o755)
			outPath := filepath.Join(dir, chooseCommentFile(dir, ticker, k[0], k[1], k[2]))
			out, _ := os.Create(outPath)
			w := bufio.NewWriter(out)
			for _, row := range list {
				_ = json.NewEncoder(w).Encode(row)
			}
			w.Flush()
			out.Close()
			fmt.Printf("📝 %s → %s (%d comments)\n",
				filepath.Base(path), filepath.Base(outPath), len(list))
		}

		// mark as processed & persist index
		idx[path] = struct{}{}
		saveIndex(idx)
		return nil
	})
}

/*─────────────────────────────
  main
─────────────────────────────*/

func main() {
	root := filepath.Join("data", "raw", "youtube")
	entries, err := os.ReadDir(root)
	if err != nil {
		fmt.Println("failed to read YouTube data folder:", err)
		return
	}

	index := loadIndex()

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		ticker := entry.Name()
		if err := processTicker(ticker, index); err != nil {
			fmt.Println("ERR", ticker, err)
		}
	}
}
