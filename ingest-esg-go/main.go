package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/Cookie579/fin-guard/ingest-esg-go/utils"
)

type DomainEntry struct {
	Name   string `json:"name"`
	Ticker string `json:"ticker"`
	Domain string `json:"domain"`
}

func main() {
	log.Println("🌱 Starting ESG ingest from company_domains.json...")

	bucket := "fin-processed-esg-reports"
	prefix := "Company_Domain_Links/company_domains.json"
	outBucket := "fin-processed-esg-reports"

	jsonBytes, err := utils.LoadJSONFromS3(bucket, prefix)
	if err != nil {
		log.Fatalf("❌ Failed to load company_domains.json: %v", err)
	}

	var domains []DomainEntry
	if err := json.Unmarshal(jsonBytes, &domains); err != nil {
		log.Fatalf("❌ JSON unmarshal error: %v", err)
	}

	uploader := utils.NewS3Uploader("us-east-1")

	for _, entry := range domains {
		key := fmt.Sprintf("scraping-jsons/%s_esg.json", entry.Ticker)
		log.Printf("🔍 Fetching scraped ESG results for: %s", entry.Ticker)

		jsonData, err := utils.LoadJSONFromS3(bucket, key)
		if err != nil {
			log.Printf("❌ Could not load ESG JSON for %s: %v", entry.Ticker, err)
			continue
		}

		log.Printf("📥 Found ESG JSON for %s — processing...", entry.Ticker)
		if err := utils.DownloadFromJSON(entry.Ticker, jsonData, uploader, outBucket); err != nil {
			log.Printf("❌ Download failed for %s: %v", entry.Ticker, err)
		}
	}

	log.Println("✅ ESG ingest complete.")
}
