package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
)

// DefaultRapidAPIHost is the fallback host for RapidAPI StockTwits if none is provided
// in the configuration. The fin-guard/config.json file may optionally set
// rapidapi_host; otherwise this constant is used.
const DefaultRapidAPIHost = "stocktwits.p.rapidapi.com"

// FlatConfig matches the expected shape of fin-guard/config.json. Only API
// keys are extracted; all infrastructure names remain hard-coded in
// constants.go. Additional fields are ignored.
type FlatConfig struct {
	GoogleAPIKey    string   `json:"google_api_key"`
	GoogleAPIKeys   []string `json:"google_api_keys"`
	SerperAPIKey    string   `json:"serper_api_key"`
	GoogleCX        string   `json:"google_cx"`
	TwitterBearer   string   `json:"twitter_bearer"`
	KinesisStream   string   `json:"kinesis_stream"`
	ReferenceStream string   `json:"reference_stream"`
	AWSRegion       string   `json:"aws_region"`
	Stocktwitkeys   []string `json:"stocktwit_keys"`
	Youtubekeys     []string `json:"youtube_keys"`
	RapidAPIHost    string   `json:"rapidapi_host"`
}

// readFirstConfig attempts to read config.json from several possible
// locations. It first checks the /app mount used in Kubernetes, then
// local development directories. The first file found is returned.
func readFirstConfig() ([]byte, error) {
	for _, p := range []string{"/app/config.json", "./config.json", "../config.json"} {
		if b, err := os.ReadFile(p); err == nil {
			return b, nil
		}
	}
	return nil, errors.New("config.json not found in /app, ./, or ../")
}

// loadFlatConfig unmarshals the fin-guard/config.json file into a FlatConfig.
// Only the keys defined in FlatConfig are used.
func loadFlatConfig() (*FlatConfig, error) {
	b, err := readFirstConfig()
	if err != nil {
		return nil, err
	}
	var c FlatConfig
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, fmt.Errorf("parse config.json: %w", err)
	}
	return &c, nil
}

// getYouTubeAPIKey selects a YouTube API key from the configuration. It
// returns config.google_api_key if set, otherwise the first entry in
// config.google_api_keys. An error is returned if neither is provided.
func getYouTubeAPIKey() (string, error) {
	cfg, err := loadFlatConfig()
	if err != nil {
		return "", err
	}
	if len(cfg.Youtubekeys) > 0 && cfg.Youtubekeys[0] != "" {
		return cfg.Youtubekeys[0], nil
	}
	return "", errors.New("no YouTube key found in config.json (youtube_keys)")
}
