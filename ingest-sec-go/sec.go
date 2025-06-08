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
	CIK     string `json:"cik"`
	Company string `json:"name"`
	Filings struct {
		Recent struct {
			Form       []string `json:"form"`
			FilingDate []string `json:"filingDate"`
			Accession  []string `json:"accessionNumber"`
			PrimaryDoc []string `json:"primaryDocument"`
		} `json:"recent"`
	} `json:"filings"`
}

var allowedForms = map[string]bool{
	"10-K": true,
	"10-Q": true,
	"8-K": true,
	"DEF 14A": true,
	"20-F": true,
	"40-F": true,
	"SD": true,
	"S-1": true,
	"F-1": true,
}

// Accepts a Company struct, returns all filings that match allowed forms
func FetchRecentFilings(company Company) ([]Filing, error) {
	url := fmt.Sprintf("https://data.sec.gov/submissions/CIK%s.json", company.CIK)

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
	cikStripped := stripLeadingZeros(company.CIK)

	for i := range submission.Filings.Recent.Form {
		formType := submission.Filings.Recent.Form[i]
		if allowedForms[formType] {
			accession := submission.Filings.Recent.Accession[i]
			accessionNoDashes := removeDashes(accession)

			indexURL := fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%s/%s/%s-index.htm",
				cikStripped, accessionNoDashes, accession)

			filings = append(filings, Filing{
				CompanyName: company.Name,
				FormType:    formType,
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

func stripLeadingZeros(cik string) string {
	for i, ch := range cik {
		if ch != '0' {
			return cik[i:]
		}
	}
	return cik
}
