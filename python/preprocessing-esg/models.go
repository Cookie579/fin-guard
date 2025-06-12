package main

import (
	"encoding/json"
	"os"
)

type Company struct {
	Name   string `json:"name"`
	Ticker string `json:"ticker"`
	CIK    string `json:"cik"`
}

type Config struct {
	GoogleAPIKey   string `json:"google_api_key"`
	GoogleSearchCX string `json:"google_search_cx"`
	KinesisStream  string
	AWSRegion      string
}

func LoadConfig(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var cfg Config
	err = json.NewDecoder(f).Decode(&cfg)
	return &cfg, err
}
