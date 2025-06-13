package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

// Loaders
func LoadCompanies(path string) ([]Company, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data struct {
		Companies []Company `json:"companies"`
	}
	err = json.NewDecoder(file).Decode(&data)
	return data.Companies, err
}

func main() {
	log.Println("🌱 Starting ESG scraper preprocessor...")

	companies, err := LoadCompanies("companies.json")
	if err != nil {
		log.Fatalf("❌ Failed to load companies.json: %v", err)
	}

	cfg, err := LoadConfig("../../config.json")
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	for _, company := range companies {
		key := fmt.Sprintf("scraping-jsons/%s_esg.json", company.Ticker)
		exists, err := CheckIfExists("fin-processed-esg-reports", key)
		if err != nil {
			log.Printf("❌ S3 check failed for %s: %v", company.Ticker, err)
			continue
		}
		if exists {
			log.Printf("📁 %s already exists in S3 — skipping.", key)
			continue
		}

		log.Printf("🔍 Searching ESG links for %s...", company.Name)
		results, err := SearchESGLinks(company.Name, cfg)
		if err != nil {
			log.Printf("❌ Google search failed for %s: %v", company.Ticker, err)
			continue
		}

		if err := SaveResults("fin-processed-esg-reports", key, results); err != nil {
			log.Printf("❌ Failed to save %s to S3: %v", key, err)
			continue
		}

		if err := SendToStream(cfg.KinesisStream, company.Ticker, results); err != nil {
			log.Printf("❌ Failed to send to Kinesis for %s: %v", company.Ticker, err)
			continue
		}

		log.Printf("✅ Done with %s", company.Ticker)
	}

	// === Domain Mapping Section ===
	const domainKey = "Company_Domain_Links/company_domains.json"
	domainExists, err := CheckIfExists("fin-processed-esg-reports", domainKey)
	if err != nil {
		log.Printf("❌ Could not check if domain map exists: %v", err)
	}

	if !domainExists {
		log.Println("🌐 Domain mapping file not found. Generating...")

		domainMap := make(map[string]string)
		for _, company := range companies {
			domain, err := FindDomain(company.Name, cfg)
			if err != nil {
				log.Printf("❌ Failed to find domain for %s: %v", company.Name, err)
				continue
			}
			domainMap[company.Name] = domain
		}

		if err := SaveDomainMap("fin-processed-esg-reports", domainKey, domainMap); err != nil {
			log.Printf("❌ Failed to save domain map to S3: %v", err)
		} else {
			log.Printf("✅ Domain map uploaded to s3://%s/%s", "fin-processed-esg-reports", domainKey)
		}
	} else {
		log.Println("📁 Domain mapping file already exists — skipping.")
	}

	log.Println("✅ Preprocessing ESG scraping completed.")
}
