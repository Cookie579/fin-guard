//go:build ignore
// +build ignore

package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/Cookie579/fin-guard/config"
)

func main() {
	// ─── Load config (must include RapidAPIKey) ───
	cfg, err := config.Load("../config.json")
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	// ─── Choose tickers from env or fallback ───
	tickers := strings.Split(os.Getenv("TICKER_LIST"), ",")
	if len(tickers) == 1 && tickers[0] == "" {
		tickers = []string{"AAPL"}
	}

	// ─── Start collector with config ───
	if err := StartStockTwitsCollector(context.Background(), tickers, cfg); err != nil {
		log.Fatalf("collector error: %v", err)
	}
}
