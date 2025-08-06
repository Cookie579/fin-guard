// main.go — ESG-preprocessor entrypoint
// ------------------------------------
// Usage examples
//
//	go run . -mode=domain        # discover domains only
//	go run . -mode=esg           # search ESG links only
//	go run .                     # default = esg+domain
//
// Mode values:  esg | domain | esg+domain
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
)

// ── helper to load companies.json ────────────────────────────────
func LoadCompanies(path string) ([]Company, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var wrap struct {
		Companies []Company `json:"companies"`
	}
	if err := json.NewDecoder(f).Decode(&wrap); err != nil {
		return nil, err
	}
	return wrap.Companies, nil
}

// ─────────────────────────── main ───────────────────────────────
func main() {
	mode := flag.String("mode", "esg+domain", "domain | esg | esg+domain")
	flag.Parse()

	log.Printf("🌱 Starting ESG pre-processor  (mode=%s)…", *mode)

	// ---- config + company list ----------------------------------
	companies, err := LoadCompanies("companies.json")
	if err != nil {
		log.Fatalf("❌ load companies.json: %v", err)
	}
	cfg, err := LoadConfig() // or LoadConfig("config.json") locally
	if err != nil {
		log.Fatalf("❌ load config: %v", err)
	}

	// ---- 1️⃣ per-ticker domain discovery ------------------------
	if *mode == "domain" || *mode == "esg+domain" {
		for _, c := range companies {
			if _, skipped, err := FindDomain(c, cfg); err != nil {
				log.Printf("❌ domain lookup %s: %v", c.Ticker, err)
			} else if !skipped {
				log.Printf("🌐 domain saved for %s", c.Ticker)
			}
		}
	}

	// ---- 2️⃣ ESG link search & save -----------------------------
	if *mode == "esg" || *mode == "esg+domain" {
		for _, c := range companies {
			key := fmt.Sprintf("scraping-jsons/%s_esg.json", c.Ticker)

			if ok, _ := CheckIfExists("fin-processed-esg-reports", key); ok {
				log.Printf("📁 %s already on S3 — skip", key)
				continue
			}

			log.Printf("🔍 Google ESG search for %s …", c.Name)
			results, err := SearchESGLinks(c.Name, cfg) // could later add site: filter
			if err != nil {
				log.Printf("❌ search %s: %v", c.Ticker, err)
				continue
			}

			if err := SaveResults("fin-processed-esg-reports", key, results); err != nil {
				log.Printf("❌ S3 put %s: %v", key, err)
				continue
			}
			if err := SendToStream(cfg.KinesisStream, c.Ticker, results); err != nil {
				log.Printf("❌ Kinesis push %s: %v", c.Ticker, err)
			}

			log.Printf("✅ ESG links stored for %s", c.Ticker)
		}
	}

	log.Println("🎉 Pre-processing complete.")
}
