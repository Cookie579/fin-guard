package main

import (
	"log"
)

func main() {
	log.Println("🚀 Starting ingest-sec-go...")

	// Load companies from JSON config
	companies, err := LoadCompanies("companies.json")
	if err != nil {
		log.Fatalf("❌ Failed to load companies: %v", err)
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
			err := PublishToKinesis(filing)
			if err != nil {
				log.Printf("❌ Kinesis error: %v", err)
			} else {
				log.Printf("✅ Published to Kinesis: %s (%s)", filing.CompanyName, filing.FormType)
			}

			err = SaveToS3(filing)
			if err != nil {
				log.Printf("❌ S3 error: %v", err)
			} else {
				log.Printf("✅ Saved to S3: %s (%s)", filing.CompanyName, filing.FormType)
			}
		}
	}
}
