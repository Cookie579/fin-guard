package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ----- JSON schema we expect in /app/config.json -----

// StocktwitsLimits holds rate limiting settings for the Stocktwits API.
// When a field is zero or empty in the config file, a reasonable default
// will be supplied by helper functions below.  See GetStocktwitsRate for
// defaults.
type StocktwitsLimits struct {
	Host        string  `json:"host"`         // e.g. "stocktwits.p.rapidapi.com"
	RatePerSec  float64 `json:"rate_per_sec"` // e.g. 0.2777778
	Burst       int     `json:"burst"`        // e.g. 10
	MaxInflight int     `json:"max_inflight"` // e.g. 1
}

// YouTubeLimits holds rate limiting settings for the YouTube RapidAPI proxy.
// When fields are zero or empty in the config file, helper functions will
// supply sane defaults.  See GetYouTubeRate for defaults.
type YouTubeLimits struct {
	Host        string  `json:"host"`
	RatePerSec  float64 `json:"rate_per_sec"`
	Burst       int     `json:"burst"`
	MaxInflight int     `json:"max_inflight"`
}

// BrokerConfig groups Redis connection information and per-API rate limit
// settings.  A new Youtube section has been added to allow configuring
// the RapidAPI YouTube proxy.  If the Youtube section is omitted or
// contains zero values, helper functions will fall back to defaults and
// a fallback key pool.
type BrokerConfig struct {
	RedisAddr  string           `json:"redis_addr"` // e.g. "redis:6379"
	Stocktwits StocktwitsLimits `json:"stocktwits"`
	Youtube    YouTubeLimits    `json:"youtube"`
}

type Config struct {
	GoogleAPIKey   string       `json:"google_api_key"`
	GoogleCX       string       `json:"google_cx"`
	KinesisStream  string       `json:"kinesis_stream"`
	AWSRegion      string       `json:"aws_region"`
	Stocktwitkeys  []string     `json:"stocktwit_keys"`
	YouTubeAPIKeys []string     `json:"youtube_keys"`
	Broker         BrokerConfig `json:"broker"`
}

// ----- File loading (no envs) -----

func loadConfigBytes() ([]byte, error) {
	paths := []string{
		"/app/config.json", // mounted by Deployment
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

func LoadConfig() (Config, error) {
	b, err := loadConfigBytes()
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config.json: %w", err)
	}
	return cfg, nil
}

// ----- Helpers (with sane defaults so old config.json still works) -----

func Loadstocktwitkeys() ([]string, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	if len(cfg.Stocktwitkeys) == 0 {
		return nil, errors.New("config.json: stocktwit_keys is empty or missing")
	}
	return dedupeTrim(cfg.Stocktwitkeys), nil
}

func GetRedisAddr(cfg Config) string {
	if a := strings.TrimSpace(cfg.Broker.RedisAddr); a != "" {
		return a
	}
	// default if not provided
	return "redis:6379"
}

func GetStocktwitsHost(cfg Config) string {
	if h := strings.TrimSpace(cfg.Broker.Stocktwits.Host); h != "" {
		return h
	}
	return "stocktwits.p.rapidapi.com"
}

func GetStocktwitsRate(cfg Config) (rate float64, burst, maxInflight int) {
	rate = cfg.Broker.Stocktwits.RatePerSec
	if rate <= 0 {
		rate = 0.2777778 // 1000/hour
	}
	burst = cfg.Broker.Stocktwits.Burst
	if burst <= 0 {
		burst = 10
	}
	maxInflight = cfg.Broker.Stocktwits.MaxInflight
	if maxInflight <= 0 {
		maxInflight = 1
	}
	return
}

// GetYouTubeHost returns the RapidAPI host for the YouTube API from the
// configuration.  If the host is missing or empty, a reasonable default
// (youtube-v311.p.rapidapi.com) is returned.  This helper allows
// older config files to omit the youtube.host field entirely.
func GetYouTubeHost(cfg Config) string {
	if h := strings.TrimSpace(cfg.Broker.Youtube.Host); h != "" {
		return h
	}
	// Default host for the RapidAPI YouTube proxy
	return "youtube-v311.p.rapidapi.com"
}

// GetYouTubeRate returns the rate limit settings for the YouTube API.  If
// any of the values are zero or negative in the loaded configuration,
// sane defaults are provided: ratePerSec = 0.1388888, burst = 10,
// maxInflight = 1.  These defaults correspond roughly to a 500 requests
// per day quota on RapidAPI.
func GetYouTubeRate(cfg Config) (rate float64, burst, maxInflight int) {
	rate = cfg.Broker.Youtube.RatePerSec
	if rate <= 0 {
		// Default to ~500 calls/day (0.1388888 rps)
		rate = 0.1388888
	}
	burst = cfg.Broker.Youtube.Burst
	if burst <= 0 {
		burst = 10
	}
	maxInflight = cfg.Broker.Youtube.MaxInflight
	if maxInflight <= 0 {
		maxInflight = 1
	}
	return
}

// getYouTubeRate is a lowercase alias for GetYouTubeRate.  It exists so
// that code can call either function without regard to capitalization.
func getYouTubeRate(cfg Config) (float64, int, int) { return GetYouTubeRate(cfg) }

// getyoutubehost is an additional alias that matches a completely
// lowercase invocation.  Some code may call getyoutubehost(cfg) with
// all lowercase letters; this function forwards to GetYouTubeHost.
func getyoutubehost(cfg Config) string { return GetYouTubeHost(cfg) }

// getyoutuberate is an additional alias that matches a completely
// lowercase invocation.  It forwards to GetYouTubeRate.
func getyoutuberate(cfg Config) (float64, int, int) { return GetYouTubeRate(cfg) }

// Loadyoutubekeys loads the RapidAPI keys for the YouTube API from
// config.json.  It trims whitespace, deduplicates entries, and returns
// an error only if config loading fails.  If youtube_keys is absent or
// empty, the returned slice will be empty; callers are expected to
// implement their own fallback (e.g. falling back to stocktwit_keys) if
// appropriate.
func Loadyoutubekeys() ([]string, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	// The YouTube keys live under cfg.YouTubeAPIKeys; older configs may
	// omit this field entirely.  We do not treat an empty list as an
	// error here to allow fallbacks in the caller.
	if len(cfg.YouTubeAPIKeys) == 0 {
		return []string{}, nil
	}
	return dedupeTrim(cfg.YouTubeAPIKeys), nil
}

// loadyoutubekeys is a lowercase alias for Loadyoutubekeys.  It exists
// purely for compatibility with code that might call the helper with a
// lowercase initial letter.
func loadyoutubekeys() ([]string, error) { return Loadyoutubekeys() }

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
