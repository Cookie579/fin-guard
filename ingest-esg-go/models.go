package main

// This file defines the core data models and configuration loader used
// throughout the ESG ingest. It consolidates definitions that were
// previously hidden under the utils/ directory so that a single Go
// package can be built without pulling in multiple packages. By
// flattening these types into the top‑level package we avoid
// duplicate definitions and make the code easier to test and compile.

import (
    "encoding/json"
    "fmt"
    "os"
)

// Company represents a single company entry in the companies.json
// configuration file. Each company has a human friendly name, the
// stock ticker and the SEC CIK identifier. The Name field is used
// when constructing search queries for ESG links.
type Company struct {
    Name   string `json:"name"`
    Ticker string `json:"ticker"`
    CIK    string `json:"cik"`
}

// DomainEntry models the payload stored when discovering a company's
// official domain. It is saved to S3 and emitted on the reference
// Kinesis stream. The Source field is always "DOMAIN" and the
// timestamp is recorded in UTC using RFC3339 format.
type DomainEntry struct {
    Company  string `json:"company"`
    Ticker   string `json:"ticker"`
    Domain   string `json:"domain"`
    Source   string `json:"source"`    // always "DOMAIN"
    IngestTS string `json:"ingest_ts"` // RFC3339 UTC
}

// ESGLinkRecord holds one scraped ESG link record. These records are
// saved individually to S3 and optionally emitted to a Kinesis stream.
// See runner_esg.go for how these fields are populated. The
// FilingDate may be inferred from the URL or default to the current
// date.
type ESGLinkRecord struct {
    Ticker     string `json:"ticker"`
    Title      string `json:"title"`
    URL        string `json:"url"`
    IngestTS   string `json:"ingest_ts"`
    Source     string `json:"source"`     // "ESG"
    Stage      string `json:"stage"`      // "raw"
    FilingDate string `json:"filingDate"` // YYYY-MM-DD
}

// Config captures secrets and settings loaded from config.json. The
// same file is used by the domain discovery and ESG scraping code. If
// additional fields are required they should be added here and the
// loader adjusted accordingly.
type Config struct {
    GoogleAPIKey    string `json:"google_api_key"`
    GoogleSearchCX  string `json:"google_cx"`
    KinesisStream   string `json:"kinesis_stream"`
    AWSRegion       string `json:"aws_region"`
    // SerperAPIKey holds a single API key for Serper searches. It is kept for
    // backward compatibility when only one key is provided. When multiple
    // keys are supplied the SerperKeys slice will be populated instead.
    SerperAPIKey    string   `json:"serper_api_key"`
    // SerperKeys allows specifying multiple Serper API keys. When present the
    // first non-empty key will be used for searches if SerperAPIKey is empty.
    SerperKeys      []string `json:"serper_keys"`
    ReferenceStream string   `json:"reference_stream"`
}

// LoadConfig attempts to read configuration from a small number of
// standard paths. It will search for config.json in /app (for
// containers), the working directory and the parent directory. The
// first file that can be opened and parsed is returned. If no file
// exists or the contents cannot be decoded an error is returned. The
// caller should treat failure to load config as fatal.
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

// LoadCompanies loads a JSON file containing an array of companies
// inside an object keyed by "companies". This function is preserved
// from the original utils/main.go to maintain compatibility with
// existing workflows. It is used by runner_esg.go to map tickers to
// company names. If the file cannot be read or parsed an error is
// returned. The caller must decide whether to abort on missing
// companies or handle the absence gracefully.
func LoadCompanies(path string) ([]Company, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()
    var wrap struct {
        Companies []Company `json:"companies"`
    }
    if err := json.NewDecoder(f).Decode(&wrap); err != nil {
        return nil, err
    }
    return wrap.Companies, nil
}