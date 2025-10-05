package main

import (
	"context"
	"flag"
	"log"
	"strings"

	"github.com/Cookie579/fin-guard/utils"
)

// main serves as the entrypoint for the ingest-sec-go binary.
// It now REQUIRES a single-ticker via the --ticker flag.
// Bulk operation (companies.json) is disabled.
func main() {
	log.Println("🚀 Starting ingest-sec-go...")

	// Require a ticker
	tickerFlag := flag.String("ticker", "", "Stock ticker symbol to process (REQUIRED, e.g. AAPL)")
	flag.Parse()

	ticker := strings.ToUpper(strings.TrimSpace(*tickerFlag))
	if ticker == "" {
		log.Fatalf("❌ Missing required flag: --ticker TICKER (e.g. --ticker AAPL)")
	}

	ctx := context.Background()

	// Resolve CIK using DynamoDB first, then SEC company list if needed.
	cikStore, err := NewCompanyCikStore(ctx)
	if err != nil {
		log.Fatalf("❌ Failed to initialise DynamoDB client: %v", err)
	}
	cik, name, err := cikStore.GetCIK(ctx, ticker)
	if err != nil {
		log.Fatalf("❌ DynamoDB error looking up CIK for %s: %v", ticker, err)
	}
	if cik == "" {
		cik, name, err = FetchCIKFromRemote(ctx, ticker)
		if err != nil {
			log.Fatalf("❌ Failed to resolve CIK for %s: %v", ticker, err)
		}
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
}
