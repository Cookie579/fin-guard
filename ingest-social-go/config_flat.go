package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"
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
	stocktwitkeys   []string `json:"stocktwits_keys"`
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

// getRapidAPIHostAndKey returns a RapidAPI host and one API key from the
// configuration. The host is config.rapidapi_host if present, else
// DefaultRapidAPIHost. The key is selected from config.stocktwits_keys using
// a simple round-robin based on the current Unix timestamp to spread
// requests across keys. An error is returned if no keys are found.
func getRapidAPIHostAndKey() (host, key string, err error) {
	cfg, err := loadFlatConfig()
	if err != nil {
		return "", "", err
	}
	host = cfg.RapidAPIHost
	if host == "" {
		host = DefaultRapidAPIHost
	}
	if len(cfg.stocktwitkeys) == 0 {
		return "", "", errors.New("no RapidAPI keys found in config.json (stocktwits_keys)")
	}
	idx := int(time.Now().Unix()) % len(cfg.stocktwitkeys)
	key = cfg.stocktwitkeys[idx]
	return host, key, nil
}

// getYouTubeAPIKey selects a YouTube API key from the configuration. It
// returns config.google_api_key if set, otherwise the first entry in
// config.google_api_keys. An error is returned if neither is provided.
func getYouTubeAPIKey() (string, error) {
	cfg, err := loadFlatConfig()
	if err != nil {
		return "", err
	}
	if cfg.GoogleAPIKey != "" {
		return cfg.GoogleAPIKey, nil
	}
	if len(cfg.GoogleAPIKeys) > 0 && cfg.GoogleAPIKeys[0] != "" {
		return cfg.GoogleAPIKeys[0], nil
	}
	return "", errors.New("no YouTube key found in config.json (google_api_key/google_api_keys)")
}
