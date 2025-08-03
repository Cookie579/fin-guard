package main

import (
	"log"
	"os"

	"github.com/Cookie579/fin-guard/utils"
)

func main() {
	log.Println("🚀 Starting ingest-sec-go...")

	// Use env var if provided, otherwise default to /root/companies.json (Docker path)
	companyPath := os.Getenv("COMPANIES_JSON_PATH")
	if companyPath == "" {
		companyPath = "companies.json" // relative path in Docker runtime
	}

	// Load companies
	companies, err := LoadCompanies(companyPath)
	if err != nil {
		log.Fatalf("❌ Failed to load companies from %s: %v", companyPath, err)
	}

	for _, company := range companies {
		log.Printf("🔍 Fetching filings for %s (%s)", company.Name, company.CIK)

		filings, err := FetchRecentFilings(company)
		if err != nil {
			log.Printf("❌ Error fetching filings for %s: %v", company.Name, err)
			continue
		}

		log.Printf("📦 %d filings found for %s", len(filings), company.Name)

		for _, filing := range filings {
			if err := utils.PublishToKinesis(filing); err != nil {
				log.Printf("❌ Kinesis error: %v", err)
			} else {
				log.Printf("✅ Published to Kinesis: %s (%s)", filing.Company, filing.Form)
			}

			if err := utils.SaveToS3(filing); err != nil {
				log.Printf("❌ S3 error: %v", err)
			} else {
				log.Printf("✅ Saved to S3: %s (%s)", filing.Company, filing.Form)
			}
		}
	}
}
