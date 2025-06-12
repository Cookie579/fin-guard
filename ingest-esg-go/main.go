package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/Cookie579/fin-guard/ingest-esg-go/utils"
)

func LoadCompanies(path string) ([]utils.Company, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data struct {
		Companies []utils.Company `json:"companies"`
	}
	err = json.NewDecoder(file).Decode(&data)
	return data.Companies, err
}

func main() {
	log.Println("🌱 Starting ingest-esg-go...")

	companies, err := LoadCompanies("../companies.json")
	if err != nil {
		log.Fatalf("❌ Failed to load companies: %v", err)
	}

	bucket := "fin-processed-esg-reports"

	for _, company := range companies {
		key := fmt.Sprintf("scraping-jsons/%s_esg.json", company.Ticker)
		log.Printf("🔍 Checking for: %s in S3", key)

		jsonBytes, err := utils.LoadJSONFromS3(bucket, key)
		if err != nil {
			log.Printf("❌ Could not load ESG JSON for %s: %v", company.Ticker, err)
			continue
		}

		log.Printf("📥 Found ESG JSON for %s — starting download", company.Ticker)

		if err := utils.DownloadFromJSON(company.Ticker, jsonBytes); err != nil {
			log.Printf("❌ Download failed for %s: %v", company.Ticker, err)
		}
	}

	log.Println("✅ ingest-esg-go completed.")
}
