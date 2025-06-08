package main

import (
	"encoding/json"
	"os"
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

// LoadCompanies reads companies from companies.json file.
func LoadCompanies(filename string) ([]Company, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error opening companies file: %v", err)
		return nil, err
	}
	defer file.Close()

	var config Config
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&config); err != nil {
		log.Printf("Error decoding companies file: %v", err)
		return nil, err
	}

	return config.Companies, nil
}
