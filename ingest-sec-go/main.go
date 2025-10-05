package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"

	"github.com/Cookie579/fin-guard/utils"
)

// main serves as the entrypoint for the ingest‑sec‑go binary. It now
// supports both bulk operation via a companies.json file (legacy mode)
// and single‑ticker operation via the --ticker flag. When a ticker is
// provided the program will resolve the CIK from DynamoDB or the SEC
// company tickers list, fetch recent filings for that company and
// publish the results to Kinesis and S3. In legacy mode the program
// behaves as before, processing all companies defined in companies.json.
func main() {
	log.Println("🚀 Starting ingest-sec-go...")

	// Define a flag for single‑ticker operation. If provided, the
	// companies.json file is ignored and only the given ticker is
	// processed. The ticker value is case‑insensitive and will be
	// upper‑cased internally.
	tickerFlag := flag.String("ticker", "", "Stock ticker symbol to process (optional)")
	flag.Parse()

	ctx := context.Background()
	if *tickerFlag != "" {
		// Single ticker mode: resolve CIK, fetch filings and emit them.
		ticker := strings.ToUpper(*tickerFlag)
		cikStore, err := NewCompanyCikStore(ctx)
		if err != nil {
			log.Fatalf("❌ Failed to initialise DynamoDB client: %v", err)
		}
		// Attempt to load from DynamoDB first to avoid repeated remote fetches.
		cik, name, err := cikStore.GetCIK(ctx, ticker)
		if err != nil {
			log.Fatalf("❌ DynamoDB error looking up CIK for %s: %v", ticker, err)
		}
		if cik == "" {
			// Not found in table, fetch from SEC list.
			cik, name, err = FetchCIKFromRemote(ctx, ticker)
			if err != nil {
				log.Fatalf("❌ Failed to resolve CIK for %s: %v", ticker, err)
			}
			// Persist the mapping so future runs do not need to fetch again.
			if err := cikStore.PutCIK(ctx, ticker, cik, name); err != nil {
				log.Printf("⚠️ Failed to save CIK mapping for %s: %v", ticker, err)
			}
		}
		company := Company{Name: name, Ticker: ticker, CIK: cik}
		log.Printf("🔍 Fetching filings for %s (%s)", company.Name, company.CIK)
		filings, err := FetchRecentFilings(company)
		if err != nil {
			log.Fatalf("❌ Error fetching filings for %s: %v", company.Name, err)
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
		return
	}

	// Legacy mode: load companies from companies.json or the path
	// specified by the COMPANIES_JSON_PATH environment variable. This
	// preserves existing behaviour for bulk processing at startup.
	companyPath := os.Getenv("COMPANIES_JSON_PATH")
	if companyPath == "" {
		companyPath = "companies.json"
	}
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
