package main

import (
	"log"
)

func main() {
	log.Println("🚀 Starting ingest-sec-go...")

	filings, err := FetchRecentFilings()
	if err != nil {
		log.Fatalf("❌ Error fetching filings: %v", err)
	}

	log.Printf("📦 Fetched %d filings", len(filings))

	for _, filing := range filings {
		if err := PublishToKinesis(filing); err != nil {
			log.Printf("❌ Error publishing to Kinesis: %v", err)
		} else {
			log.Printf("✅ Published filing to Kinesis: %s (%s)", filing.CompanyName, filing.FormType)
		}

		if err := SaveToS3(filing); err != nil {
			log.Printf("❌ Error saving to S3: %v", err)
		}
	}
}
