package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/Cookie579/fin-guard/utils"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/google/uuid"
)

// main serves as the entrypoint for the ingest-sec-go binary. It supports
// both single-ticker operation via --ticker and legacy orchestrator mode.
// Single-ticker flow:
//  1. Try to read CIK from DynamoDB.
//  2. If missing, fetch SEC company_tickers.json at RUNTIME (with SEC-compliant
//     User-Agent supplied via SEC_HTTP_UA env) and resolve the CIK.//     On success, cache it back into DynamoDB.
//  3. If that JSON fetch fails, fall back to the older remote lookup.
//     On success, cache it back into DynamoDB.
//  4. If still no CIK, log a warning and exit gracefully (code 0).
//  5. With a CIK, fetch recent filings and publish to Kinesis/S3.
//
// No local/prebundled JSON is used anywhere in this flow.
func main() {
	log.Println("🚀 Starting ingest-sec-go...")

	// Generate a unique job ID for this ingest run. The job ID is used
	// when emitting control‑plane metadata events to Kinesis. We use a
	// UUID to avoid collisions between multiple concurrent jobs.
	jobID := uuid.New().String()

	// --ticker enables single-ticker mode
	tickerFlag := flag.String("ticker", "", "Stock ticker symbol to process (optional)")
	flag.Parse()

	ctx := context.Background()

	if *tickerFlag != "" {
		// Single ticker mode
		ticker := strings.ToUpper(*tickerFlag)

		// Emit a job_started metadata event before any processing. This
		// allows downstream systems to detect when a SEC ingest run has
		// commenced. If the emit fails it is logged but does not abort
		// the job.
		emitMeta("job_started", ticker, jobID, nil)

		// DynamoDB CIK store (best-effort)
		var cikStore *CompanyCikStore
		if store, err := NewCompanyCikStore(ctx); err != nil {
			log.Printf("⚠️ DynamoDB client initialisation failed: %v; falling back to SEC runtime lookup", err)
		} else {
			cikStore = store
		}

		// DynamoDB SEC seen store (best-effort). This table tracks
		// previously processed filings per ticker so that we do not
		// re‑emit identical filings on subsequent runs. If the store
		// cannot be initialised, ingestion will continue but duplicates
		// may be processed.
		var seenStore *SecSeenStore
		if s, err := NewSecSeenStore(ctx); err != nil {
			log.Printf("⚠️ DynamoDB client initialisation for SEC seen table failed: %v; continuing without deduplication", err)
		} else {
			seenStore = s
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
					// Emit job_failed event before exiting
					emitMeta("job_failed", ticker, jobID, map[string]any{"error": fmt.Sprintf("no CIK for %s", ticker)})
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
			// Emit job_failed event before exiting
			emitMeta("job_failed", ticker, jobID, map[string]any{"error": fmt.Sprintf("fetch filings: %v", err)})
			return
		}

		log.Printf("📦 %d filings found for %s", len(filings), company.Name)
		for _, filing := range filings {
			// Skip filings that have already been processed. Use the
			// accession number as the sort key within the SEC seen
			// table. If the check fails, log and continue processing.
			if seenStore != nil {
				if ok, err := seenStore.HasSeen(ctx, filing.Ticker, filing.Accession); err != nil {
					log.Printf("⚠️ DynamoDB error checking seen for %s: %v; processing anyway", filing.Accession, err)
				} else if ok {
					log.Printf("🔁 Skipping already processed filing %s for %s", filing.Accession, filing.Ticker)
					continue
				}
			}
			// Publish to Kinesis and S3
			if err := utils.PublishToKinesis(filing); err != nil {
				log.Printf("❌ Kinesis error: %v", err)
			} else {
				log.Printf("✅ Published to Kinesis: %s (%s)", filing.Company, filing.Form)
			}
			if err := utils.SaveToS3(filing); err != nil {
				log.Printf("❌ S3 error: %v", err)
			} else {
				log.Printf("✅ Saved to S3: %s (%s)", filing.Company, filing.Form)
				// Mark the filing as seen only after successful S3 write. Even
				// if the Kinesis write failed, marking seen prevents
				// repeated attempts on subsequent runs.
				if seenStore != nil {
					if err := seenStore.MarkSeen(ctx, filing.Ticker, filing.Accession); err != nil {
						log.Printf("⚠️ Failed to mark SEC filing %s as seen: %v", filing.Accession, err)
					}
				}
			}
		}
		// Emit job_completed metadata event once all filings have been processed
		emitMeta("job_completed", ticker, jobID, map[string]any{"records": len(filings)})
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

// emitMeta sends a control‑plane metadata event to Kinesis. These events
// signal job lifecycle stages (job_started, job_completed, job_failed)
// and carry optional extra fields. Events are written to the
// fin-guard-stream under a METADATA stage prefix so that the data lake
// can partition them separately from raw filings. Any failure to
// publish the event is logged but does not interrupt the caller.
func emitMeta(eventType, ticker, jobID string, extra map[string]any) {
	rec := map[string]any{
		"stage":  "METADATA",
		"type":   eventType,
		"source": "SEC",
		"ticker": strings.ToUpper(ticker),
		"job_id": jobID,
		"ts":     time.Now().UTC().Format(time.RFC3339),
	}
	if extra != nil {
		for k, v := range extra {
			rec[k] = v
		}
	}
	data, err := json.Marshal(rec)
	if err != nil {
		log.Printf("WARN: failed to marshal metadata event %s: %v", eventType, err)
		return
	}
	// Determine the AWS region and stream name. We read the region from
	// the AwsRegion constant and use fin-guard-stream as the stream. If
	// you need to customize these values, update the constants or add
	// environment overrides here.
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(AwsRegion),
	}))
	svc := kinesis.New(sess)
	_, err = svc.PutRecord(&kinesis.PutRecordInput{
		StreamName:   aws.String("fin-guard-stream"),
		PartitionKey: aws.String(strings.ToUpper(ticker)),
		Data:         data,
	})
	if err != nil {
		log.Printf("⚠️ Failed to emit metadata event %s: %v", eventType, err)
	}
}
