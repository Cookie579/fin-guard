package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"log"
)

type Company struct {
	Name   string `json:"name"`
	Ticker string `json:"ticker"`
	CIK    string `json:"cik"`
}

type Config struct {
	Companies []Company `json:"companies"`
}

// LoadCompanies loads companies.json safely from expected path or ENV override.
func LoadCompanies(filename string) ([]Company, error) {
	// Optional override via environment variable
	if envPath := os.Getenv("COMPANIES_FILE"); envPath != "" {
		filename = envPath
	}

	// Try opening relative path first
	file, err := os.Open(filename)
	if err != nil {
		// If relative path fails, try resolving based on this file's location
		_, here, _, _ := runtime.Caller(0)
		fallback := filepath.Join(filepath.Dir(here), filename)
		file, err = os.Open(fallback)
		if err != nil {
			log.Printf("❌ Failed to open companies.json at '%s' or '%s': %v", filename, fallback, err)
			return nil, err
		}
	}

	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		log.Printf("❌ Error decoding companies.json: %v", err)
		return nil, err
	}

	return config.Companies, nil
}
