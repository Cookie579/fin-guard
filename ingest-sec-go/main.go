package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"github.com/Cookie579/fin-guard/utils"
)

// main serves as the entrypoint for the ingest-sec-go binary. It supports
// both single-ticker operation via --ticker and legacy orchestrator mode.
// Single-ticker flow:
//  1. Try to read CIK from DynamoDB.
//  2. If missing, fetch SEC company_tickers.json at RUNTIME (with SEC-compliant
//     User-Agent supplied via SEC_HTTP_UA env) and resolve the CIK.
//     On success, cache it back into DynamoDB.
//  3. If that JSON fetch fails, fall back to the older remote lookup.
//     On success, cache it back into DynamoDB.
//  4. If still no CIK, log a warning and exit gracefully (code 0).
//  5. With a CIK, fetch recent filings and publish to Kinesis/S3.
//
// No local/prebundled JSON is used anywhere in this flow.
func main() {
	log.Println("🚀 Starting ingest-sec-go...")

	// --ticker enables single-ticker mode
	tickerFlag := flag.String("ticker", "", "Stock ticker symbol to process (optional)")
	flag.Parse()

	ctx := context.Background()

	if *tickerFlag != "" {
		// Single ticker mode
		ticker := strings.ToUpper(*tickerFlag)

		// DynamoDB CIK store (best-effort)
		var cikStore *CompanyCikStore
		if store, err := NewCompanyCikStore(ctx); err != nil {
			log.Printf("⚠️ DynamoDB client initialisation failed: %v; falling back to SEC runtime lookup", err)
		} else {
			cikStore = store
		}

		// 1) Try DynamoDB first to avoid external calls
		var cik, name string
		if cikStore != nil {
			if c, n, err := cikStore.GetCIK(ctx, ticker); err != nil {
				log.Printf("⚠️ DynamoDB error looking up CIK for %s: %v; ignoring", ticker, err)
			} else {
				cik, name = c, n
			}
		}

		// 2) If still missing, fetch SEC company_tickers.json AT RUNTIME
		// (requires SEC_HTTP_UA env to be set in the pod; see k8s launcher)
		if cik == "" {
			if c, n, err := FetchCIKFromTickerJSON(ctx, ticker); err == nil {
				cik, name = c, n
				if cikStore != nil {
					if err := cikStore.PutCIK(ctx, ticker, cik, name); err != nil {
						log.Printf("⚠️ Failed to save CIK mapping for %s: %v", ticker, err)
					}
				}
			} else {
				log.Printf("⚠️ Could not fetch CIK from ticker list for %s: %v", ticker, err)

				// 3) Fallback: older remote lookup
				if c2, n2, err2 := FetchCIKFromRemote(ctx, ticker); err2 == nil {
					cik, name = c2, n2
					if cikStore != nil {
						if err := cikStore.PutCIK(ctx, ticker, cik, name); err != nil {
							log.Printf("⚠️ Failed to save CIK mapping for %s: %v", ticker, err)
						}
					}
				} else {
					// 4) No CIK → exit gracefully (code 0). Job won't be marked failed.
					log.Printf("⚠️ Failed to resolve CIK for %s via remote lookup: %v; skipping filings", ticker, err2)
					return
				}
			}
		}

		// 5) With CIK resolved, proceed to filings
		company := Company{Name: name, Ticker: ticker, CIK: cik}
		log.Printf("🔍 Fetching filings for %s (%s)", company.Name, company.CIK)

		filings, err := FetchRecentFilings(company)
		if err != nil {
			// Do not hard-fail the Job; log and exit cleanly
			log.Printf("⚠️ Error fetching filings for %s: %v; no filings will be emitted", company.Name, err)
			return
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

	// Legacy/orchestrator mode: start FastAPI so the pod stays alive
	exePath, err := os.Executable()
	if err != nil {
		log.Fatalf("❌ Unable to determine executable path: %v", err)
	}
	binDir := filepath.Dir(exePath)
	appDir := filepath.Join(binDir, "..", "ingest-sec-go")
	if _, err := os.Stat(appDir); err != nil {
		appDir = "./ingest-sec-go"
	}

	log.Println("🧭 No ticker provided; starting orchestrator on :8081 ...")
	cmd := exec.Command(
		"python3", "-m", "uvicorn", "orchestrator:app",
		"--host", "0.0.0.0", "--port", "8081",
		"--app-dir", appDir,
	)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		log.Fatalf("❌ Failed to start orchestrator: %v", err)
	}
}
