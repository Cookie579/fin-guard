// broker/config_util.go
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

type PreprocessConfig struct {
	GoogleAPIKey   string   `json:"google_api_key"`
	GoogleCX       string   `json:"google_cx"`
	KinesisStream  string   `json:"kinesis_stream"`
	AWSRegion      string   `json:"aws_region"`
	RapidAPIKeys   []string `json:"rapidapi_keys"`
	YouTubeAPIKeys []string `json:"youtube_api_keys"`
}

// loadConfigBytes reads config.json from well-known locations.
// We purposely avoid any env — paths are fixed.
func loadConfigBytes() ([]byte, error) {
	paths := []string{
		"/app/config.json", // ← K8s mount target (we’ll mount the Secret here)
		"./config.json",
		"../config.json",
	}
	for _, p := range paths {
		if b, err := os.ReadFile(p); err == nil {
			return b, nil
		}
	}
	return nil, fmt.Errorf("config.json not found in /app, ./, or ../")
}

// LoadPreprocessConfig loads and parses config.json into a struct.
func LoadPreprocessConfig() (PreprocessConfig, error) {
	b, err := loadConfigBytes()
	if err != nil {
		return PreprocessConfig{}, err
	}
	var cfg PreprocessConfig
	if err := json.Unmarshal(b, &cfg); err != nil {
		return PreprocessConfig{}, fmt.Errorf("parse config.json: %w", err)
	}
	return cfg, nil
}

// LoadRapidAPIKeys returns rapidapi_keys (deduped, trimmed) from config.json.
// No env lookups involved.
func LoadRapidAPIKeys() ([]string, error) {
	cfg, err := LoadPreprocessConfig()
	if err != nil {
		return nil, err
	}
	if len(cfg.RapidAPIKeys) == 0 {
		return nil, errors.New("config.json: rapidapi_keys is empty or missing")
	}
	return dedupeTrim(cfg.RapidAPIKeys), nil
}

func dedupeTrim(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
