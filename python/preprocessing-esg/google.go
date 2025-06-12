package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
)

type SearchItem struct {
	Title string `json:"title"`
	Link  string `json:"link"`
}

type SearchResults struct {
	Items []SearchItem `json:"items"`
}

func SearchESGLinks(company string, cfg *Config) ([]SearchItem, error) {
	query := url.QueryEscape(fmt.Sprintf("%s ESG report site:.com", company))
	apiURL := fmt.Sprintf("https://www.googleapis.com/customsearch/v1?q=%s&cx=%s&key=%s", query, cfg.GoogleSearchCX, cfg.GoogleAPIKey)

	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var parsed SearchResults
	err = json.NewDecoder(resp.Body).Decode(&parsed)
	return parsed.Items, err
}
