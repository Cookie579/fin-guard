package utils

type Filing struct {
	Company    string `json:"company"`    // “Apple Inc.”
	Ticker     string `json:"ticker"`     // “AAPL”
	Source     string `json:"source"`     // “SEC”
	Form       string `json:"form"`       // “10-K”
	FilingDate string `json:"filingDate"` // “2024-11-01”
	Accession  string `json:"accession"`  // “0000320193-24-000123”
	URL        string `json:"url"`        // filing detail page
	Stage      string `json:"stage"`      // e.g. "raw", "processed"
}
