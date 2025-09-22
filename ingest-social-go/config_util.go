package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
)

// ----- JSON schema we expect in /app/config.json -----

type StocktwitsLimits struct {
	Host        string  `json:"host"`         // e.g. "stocktwits.p.rapidapi.com"
	RatePerSec  float64 `json:"rate_per_sec"` // e.g. 0.2777778
	Burst       int     `json:"burst"`        // e.g. 10
	MaxInflight int     `json:"max_inflight"` // e.g. 1
}

type BrokerConfig struct {
	RedisAddr  string           `json:"redis_addr"` // e.g. "redis:6379"
	Stocktwits StocktwitsLimits `json:"stocktwits"`
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

func LoadYouTubeAPIKeys() ([]string, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return nil, err
	}
	if len(cfg.YouTubeAPIKeys) == 0 {
		return nil, errors.New("config.json: youtube_keys is empty or missing")
	}
	return dedupeTrim(cfg.YouTubeAPIKeys), nil
}
