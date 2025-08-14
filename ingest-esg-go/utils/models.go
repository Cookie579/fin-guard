package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Company struct {
	Name   string `json:"name"`
	Ticker string `json:"ticker"`
	CIK    string `json:"cik"`
}

type DomainEntry struct {
	Company  string `json:"company"`
	Ticker   string `json:"ticker"`
	Domain   string `json:"domain"`
	Source   string `json:"source"`    // always "domain"
	IngestTS string `json:"ingest_ts"` // RFC3339 UTC
}

type ESGLinkRecord struct {
	Ticker   string `json:"ticker"`
	Title    string `json:"title"`
	URL      string `json:"url"`
	IngestTS string `json:"ingest_ts"`

	Source     string `json:"source"`     // "ESG"
	Stage      string `json:"stage"`      // "raw"
	FilingDate string `json:"filingDate"` // YYYY-MM-DD
}

type Config struct {
	GoogleAPIKey    string `json:"google_api_key"`
	GoogleSearchCX  string `json:"google_cx"`
	KinesisStream   string `json:"kinesis_stream"`
	AWSRegion       string `json:"aws_region"`
	SerperAPIKey    string `json:"serper_api_key"`
	ReferenceStream string `json:"reference_stream"`
}

func LoadConfig() (*Config, error) {
	paths := []string{
		"/app/config.json", // for K8s
		"config.json",      // for local
		"../config.json",   // fallback for dev
	}

	var file *os.File
	var err error

	for _, path := range paths {
		file, err = os.Open(path)
		if err == nil {
			fmt.Printf("✅ Loaded config from %s\n", path)
			break
		}
	}
	if file == nil {
		return nil, fmt.Errorf("❌ Could not find config.json in known locations")
	}
	defer file.Close()

	var cfg Config
	if err := json.NewDecoder(file).Decode(&cfg); err != nil {
		return nil, fmt.Errorf("❌ Failed to parse config.json: %w", err)
	}

	return &cfg, nil
}
