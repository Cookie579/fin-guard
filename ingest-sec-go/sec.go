package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
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
	cikStripped := cik[3:] // remove leading zeroes
	url := fmt.Sprintf("https://data.sec.gov/submissions/CIK%s.json", cik)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Fin-guard fin-guard@gmail.com") 

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var submission SECSubmission
	if err := json.Unmarshal(body, &submission); err != nil {
		return nil, err
	}

	var filings []Filing
	for i := range submission.Filings.Recent.Form {
		if submission.Filings.Recent.Form[i] == "10-K" {
			accession := submission.Filings.Recent.Accession[i]
			accessionNoDashes := removeDashes(accession)

			indexURL := fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s-index.htm",
				cikStripped, accessionNoDashes, accession)

			filings = append(filings, Filing{
				CompanyName: submission.Company,
				FormType:    submission.Filings.Recent.Form[i],
				FilingDate:  submission.Filings.Recent.FilingDate[i],
				Link:        indexURL,
			})
		}
	}

	return filings, nil
}

func removeDashes(s string) string {
	var out string
	for _, ch := range s {
		if ch != '-' {
			out += string(ch)
		}
	}
	return out
}