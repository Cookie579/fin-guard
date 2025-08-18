//go:build ignore
// +build ignore

// combine_day.go
//
//   go run -tags combine combine_day.go -root data/raw -delete
//
// For every directory under -root it merges files named *_<N>.json (N≥1)
// that share the same prefix into a single  *_1.json file.  Shards are
// processed in lexical order, deduplicating by the “id” field in each row.

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

var (
	rootDir = flag.String("root", "data/raw", "root of raw-data tree")
	rmShard = flag.Bool("delete", false, "delete original shard files after merge")
)

// regexp to capture “prefix” and the numeric shard suffix
var shardRe = regexp.MustCompile(`^(.*)_([0-9]+)\.json$`)

// ───────────────────────── helpers ─────────────────────────

// minimal struct to inspect the id quickly
type idHeader struct {
	ID string `json:"id"`
}

// safeOpen creates the parent dirs if needed (helps when we merge into a new dir)
func safeOpen(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	return os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
}

// mergeShards concatenates shardFiles → dest, skipping duplicate ids.
func mergeShards(dest string, shardFiles []string) error {
	out, err := safeOpen(dest)
	if err != nil {
		return err
	}
	defer out.Close()

	seen := make(map[string]struct{})

	for _, file := range shardFiles {
		f, err := os.Open(file)
		if err != nil {
			return fmt.Errorf("open %s: %w", file, err)
		}
		sc := bufio.NewScanner(f)

		for sc.Scan() {
			line := sc.Bytes()
			var h idHeader
			if json.Unmarshal(line, &h) != nil || h.ID == "" {
				continue // malformed row
			}
			if _, dup := seen[h.ID]; dup {
				continue
			}
			seen[h.ID] = struct{}{}
			_, _ = out.Write(append(line, '\n'))
		}
		f.Close()
	}
	return nil
}

// ───────────────────────── main ─────────────────────────

func main() {
	flag.Parse()

	// 1. discover shards → map[prefix][]files
	groups := map[string][]string{}

	err := filepath.WalkDir(*rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !strings.HasSuffix(path, ".json") {
			return err
		}
		name := filepath.Base(path)
		m := shardRe.FindStringSubmatch(name)
		if m == nil {
			return nil // not a shard
		}
		prefix := m[1] // everything before "_N.json"
		key := filepath.Join(filepath.Dir(path), prefix)
		groups[key] = append(groups[key], path)
		return nil
	})
	if err != nil {
		fmt.Println("❌ walk:", err)
		return
	}

	if len(groups) == 0 {
		fmt.Println("nothing to merge – all good!")
		return
	}

	// 2. merge every group
	for key, files := range groups {
		if len(files) == 1 {
			continue // already a single shard → nothing to do
		}
		sort.Strings(files) // deterministic order

		dest := filepath.Join(filepath.Dir(files[0]), filepath.Base(key)+"_1.json")
		if err := mergeShards(dest, files); err != nil {
			fmt.Println("❌", err)
			continue
		}
		fmt.Printf("✔  merged %2d shards → %s\n", len(files), dest)

		if *rmShard {
			for _, f := range files {
				if f == dest {
					continue // keep the combined file
				}
				_ = os.Remove(f)
			}
		}
	}
	fmt.Println("✅ combine complete")
}
