package main

type Filing struct {
	CompanyName string `json:"companyName"`
	FormType    string `json:"formType"`
	FilingDate  string `json:"filingDate"`
	Link        string `json:"linkToFilingDetails"`
}

func FetchRecentFilings() ([]Filing, error) {
	return []Filing{
		{
			CompanyName: "Apple Inc.",
			FormType:    "10-K",
			FilingDate:  "2023-10-27",
			Link:        "https://www.sec.gov/Archives/edgar/data/320193/000032019323000106/0000320193-23-000106-index.htm",
		},
	}, nil
}
