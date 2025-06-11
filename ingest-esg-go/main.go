package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"
)

// Company structure matches your companies.json
type Company struct {
	Name   string `json:"Name"`
	Ticker string `json:"Ticker"`
	CIK    string `json:"CIK"`
	Domain string `json:"Domain"` // Add this to companies.json for ESG scrapers
}

// LoadCompanies reads the company metadata from JSON
func LoadCompanies(path string) ([]Company, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var companies []Company
	err = json.NewDecoder(file).Decode(&companies)
	return companies, err
}

func main() {
	log.Println("🌱 Starting ingest-esg-go...")

	// Load companies.json (hardcoded path or use os.Getenv if needed)
	companies, err := LoadCompanies("companies.json")
	if err != nil {
		log.Fatalf("❌ Failed to load companies: %v", err)
	}

	for _, company := range companies {
		log.Printf("🔍 Checking ESG reports for %s (%s)", company.Name, company.Domain)

		// Example: construct a guess at the ESG or sustainability page
		esgURLs := []string{
			fmt.Sprintf("https://%s/sustainability", company.Domain),
			fmt.Sprintf("https://%s/esg", company.Domain),
			fmt.Sprintf("https://%s/csr", company.Domain),
			fmt.Sprintf("https://%s/investor-relations", company.Domain),
		}

		for _, url := range esgURLs {
			if strings.Contains(url, company.Domain) {
				log.Printf("🔗 Trying URL: %s", url)
				// TODO: Add request and scraping logic here
			}
		}
	}

	log.Println("✅ ingest-esg-go completed (no downloads yet)")
}
