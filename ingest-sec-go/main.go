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
        // Initialize the DynamoDB CIK store. If this fails (e.g. missing
        // credentials or table), we log a warning and fall back to the
        // remote SEC CIK list. The program no longer exits on a missing
        // CompanyCikTable so that one-off runs can still succeed.
        var cikStore *CompanyCikStore
        if store, err := NewCompanyCikStore(ctx); err != nil {
            log.Printf("⚠️ DynamoDB client initialisation failed: %v; falling back to remote CIK lookup", err)
        } else {
            cikStore = store
        }
        // Attempt to load from DynamoDB first to avoid repeated remote fetches.
        var cik, name string
        if cikStore != nil {
            var err error
            cik, name, err = cikStore.GetCIK(ctx, ticker)
            if err != nil {
                log.Printf("⚠️ DynamoDB error looking up CIK for %s: %v; ignoring", ticker, err)
                cik, name = "", ""
            }
        }
        if cik == "" {
            // Not found in table or DynamoDB unavailable, fetch from SEC list.
            var err error
            cik, name, err = FetchCIKFromRemote(ctx, ticker)
            if err != nil {
                // If the remote CIK lookup fails (e.g. due to network issues or
                // the ticker not being present in the SEC data set) we no
                // longer abort the program. Instead we log a warning and
                // terminate early without emitting any filings. This ensures
                // the one‑off Job completes successfully even when external
                // services are unreachable. Exit code 0 signals success to
                // Kubernetes so the CronJob controller does not mark the
                // execution as failed.
                log.Printf("⚠️ Failed to resolve CIK for %s: %v; skipping filings", ticker, err)
                return
            }
            // Persist the mapping so future runs do not need to fetch again.
            if cikStore != nil {
                if err := cikStore.PutCIK(ctx, ticker, cik, name); err != nil {
                    log.Printf("⚠️ Failed to save CIK mapping for %s: %v", ticker, err)
                }
            }
        }
		company := Company{Name: name, Ticker: ticker, CIK: cik}
		log.Printf("🔍 Fetching filings for %s (%s)", company.Name, company.CIK)
        filings, err := FetchRecentFilings(company)
        if err != nil {
            // Do not abort on failures fetching the filings. Many network or
            // API errors are transient or outside our control, and a hard
            // failure would cause the Kubernetes Job to be marked as failed.
            // Log the error and exit gracefully without emitting any filings.
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

	// No ticker specified: start the FastAPI orchestrator so the pod stays alive.
	// Determine the path to the orchestrator sources relative to the binary.
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
	cmd := exec.Command("python3", "-m", "uvicorn", "orchestrator:app", "--host", "0.0.0.0", "--port", "8081", "--app-dir", appDir)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	if err := cmd.Run(); err != nil {
		log.Fatalf("❌ Failed to start orchestrator: %v", err)
	}
}
