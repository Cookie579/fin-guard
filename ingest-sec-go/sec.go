package main

// import (
// 	"encoding/json"
// 	"io/ioutil"
// 	"net/http"
// )

type Filing struct {
	CompanyName string `json:"companyName"`
	FormType    string `json:"formType"`
	FilingDate  string `json:"filingDate"`
	Link        string `json:"linkToFilingDetails"`
}

// func FetchRecentFilings() ([]Filing, error) {
// 	url := "https://data.sec.gov/submissions/CIK0000320193.json" // Example: Apple

// 	req, _ := http.NewRequest("GET", url, nil)
// 	req.Header.Set("User-Agent", "esg.finguard@gmail.com") // Required by SEC

// 	client := &http.Client{}
// 	resp, err := client.Do(req)
// 	if err != nil {
// 		return nil, err
// 	}
// 	defer resp.Body.Close()

// 	body, _ := ioutil.ReadAll(resp.Body)

// 	var result map[string]interface{}
// 	json.Unmarshal(body, &result)

// 	// parse into []Filing — you’ll customize based on SEC JSON
// 	// For now, return dummy
// 	return []Filing{
// 		{CompanyName: "Apple", FormType: "10-K", FilingDate: "2024-12-31", Link: "https://sec.gov/…"},
// 	}, nil
// }

func FetchRecentFilings() ([]Filing, error) {
	return []Filing{
		{
			CompanyName: "🔥 IngestSecGo",
			FormType:    "TEST-INGEST",
			FilingDate:  "2025-06-07",
			Link:        "https://example.com/unique-test",
		},
	}, nil
}

