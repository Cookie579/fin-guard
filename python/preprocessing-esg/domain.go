package main

import (
	"fmt"
	"net/url"
)

// FindDomain attempts to find the main website domain using Google Search
func FindDomain(companyName string, cfg *Config) (string, error) {
	query := fmt.Sprintf("%s official site", companyName)
	results, err := GoogleSearch(query, cfg)
	if err != nil {
		return "", err
	}

	for _, item := range results.Items {
		link := item.Link
		u, err := url.Parse(link)
		if err == nil && u.Host != "" {
			return u.Host, nil
		}
	}

	return "", fmt.Errorf("no valid domain found for %s", companyName)
}
