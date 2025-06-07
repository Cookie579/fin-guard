package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

type Filing struct {
	CompanyName string `json:"companyName"`
	FormType    string `json:"formType"`
	FilingDate  string `json:"filingDate"`
	Link        string `json:"linkToFilingDetails"`
}

type SECSubmission struct {
	CIK        string `json:"cik"`
	Company    string `json:"name"`
	Filings    struct {
		Recent struct {
			Form      []string `json:"form"`
			FilingDate []string `json:"filingDate"`
			Accession []string `json:"accessionNumber"`
			PrimaryDoc []string `json:"primaryDocument"`
		} `json:"recent"`
	} `json:"filings"`
}

func FetchRecentFilings() ([]Filing, error) {
	cik := "0000320193" // Apple Inc.
	url := fmt.Sprintf("https://data.sec.gov/submissions/CIK%s.json", cik)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// SEC requires a valid User-Agent
	req.Header.Set("User-Agent", "YourName Contact@YourDomain.com")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, _ := ioutil.ReadAll(resp.Body)

	var submission SECSubmission
	if err := json.Unmarshal(body, &submission); err != nil {
		return nil, err
	}

	var filings []Filing
	for i := range submission.Filings.Recent.Form {
		if submission.Filings.Recent.Form[i] == "10-K" {
			link := fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s",
				cik[3:],                                 // remove leading zeroes for link
				submission.Filings.Recent.Accession[i],
				submission.Filings.Recent.PrimaryDoc[i],
			)
			filings = append(filings, Filing{
				CompanyName: submission.Company,
				FormType:    submission.Filings.Recent.Form[i],
				FilingDate:  submission.Filings.Recent.FilingDate[i],
				Link:        link,
			})
		}
	}

	return filings, nil
}
