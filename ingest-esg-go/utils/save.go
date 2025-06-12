package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/Cookie579/fin-guard/ingest-esg-go/discovery"
)

// SaveResults saves search results to a local file named <TICKER>_esg.json
func SaveResults(ticker string, results []discovery.GoogleSearchItem) error {
	outputDir := "esg-results"
	if err := os.MkdirAll(outputDir, os.ModePerm); err != nil {
		return err
	}

	filePath := filepath.Join(outputDir, fmt.Sprintf("%s_esg.json", ticker))
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	return json.NewEncoder(file).Encode(results)
}
