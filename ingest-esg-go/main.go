package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/Cookie579/fin-guard/config"
	"github.com/Cookie579/fin-guard/ingest-esg-go/discovery"
	"github.com/Cookie579/fin-guard/ingest-esg-go/utils"
)

type Company struct {
	Name   string `json:"name"`
	Ticker string `json:"ticker"`
	CIK    string `json:"cik"`
	Domain string // filled during runtime
}

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

func LoadResultsIfExist(ticker string) ([]discovery.GoogleSearchItem, bool) {
	path := filepath.Join("esg-results", fmt.Sprintf("%s_esg.json", ticker))
	file, err := os.Open(path)
	if err != nil {
		return nil, false
	}
	defer file.Close()

	var items []discovery.GoogleSearchItem
	if err := json.NewDecoder(file).Decode(&items); err != nil {
		return nil, false
	}
	return items, true
}

func main() {
	log.Println("🌱 Starting ingest-esg-go...")

	companies, err := LoadCompanies("../companies.json")
	if err != nil {
		log.Fatalf("❌ Failed to load companies: %v", err)
	}

	cfg, err := config.Load("../config.json")
	if err != nil {
		log.Fatalf("❌ Failed to load config: %v", err)
	}

	log.Printf("🔐 Google API Key: %s", cfg.GoogleAPIKey[:8]+"...")

	for _, company := range companies {
		// ✅ Check for existing results
		if results, found := LoadResultsIfExist(company.Ticker); found {
			log.Printf("📁 %d ESG results found for %s — skipping search.", len(results), company.Name)
			if err := utils.DownloadAllESGContent(company.Ticker); err != nil {
				log.Printf("❌ Download failed for %s: %v", company.Name, err)
			}
			continue
		}

		// 🔍 Else: Fetch domain and perform search
		domain, err := discovery.FindDomain(company.Name, cfg)
		if err != nil {
			log.Printf("❌ Could not find domain for %s: %v", company.Name, err)
			continue
		}
		company.Domain = domain
		log.Printf("🌐 Domain for %s: %s", company.Name, domain)

		pillars := []string{"ESG", "environment", "social", "governance", "CSR", "sustainability", "DEI"}
		var allResults []discovery.GoogleSearchItem
		seen := make(map[string]bool)

		for _, pillar := range pillars {
			q1 := fmt.Sprintf("%s %s site:%s", company.Name, pillar, company.Domain)
			q2 := fmt.Sprintf("%s %s report filetype:pdf", company.Name, pillar)
			queries := []string{q1, q2}

			for _, query := range queries {
				results, err := discovery.GoogleSearch(query, cfg)
				if err != nil {
					log.Printf("⚠️  Search error for %s [%s]: %v", company.Name, pillar, err)
					continue
				}
				for _, item := range results.Items {
					if !seen[item.Link] {
						allResults = append(allResults, item)
						seen[item.Link] = true
					}
				}
			}
		}

		if len(allResults) == 0 {
			log.Printf("⚠️  No ESG results found for %s", company.Name)
			continue
		}

		for _, item := range allResults {
			log.Printf("🔗 Found: %s (%s)", item.Title, item.Link)
		}

		if err := utils.SaveResults(company.Ticker, allResults); err != nil {
			log.Printf("❌ Failed to save results for %s: %v", company.Name, err)
			continue
		}

		if err := utils.DownloadAllESGContent(company.Ticker); err != nil {
			log.Printf("❌ Download failed for %s: %v", company.Name, err)
		}
	}

	log.Println("✅ ingest-esg-go completed")
}
