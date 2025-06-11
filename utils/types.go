package utils

type Filing struct {
	CompanyName string `json:"companyName"`
	FormType    string `json:"formType"`
	FilingDate  string `json:"filingDate"`
	Link        string `json:"linkToFilingDetails"`
}
