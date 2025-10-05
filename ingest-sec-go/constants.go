package main

// constants.go holds infrastructure constants for the ingest-sec-go collector.
// These values define the AWS region used for DynamoDB operations and the
// DynamoDB table that stores ticker → CIK mappings. Adjust these values
// as needed for your deployment. Unlike the companies.json file used
// previously, CIK lookups are persisted in DynamoDB so that repeated
// lookups do not require downloading the SEC company tickers list.

const (
	// AwsRegion is the AWS region used for all DynamoDB API calls. If you
	// deploy your infrastructure to another region, update this value.
	AwsRegion = "us-east-1"

	// CompanyCikTable is the name of the DynamoDB table used to store
	// mappings from stock tickers (partition key) to CIK strings and
	// company names. The table must be created ahead of time and should
	// have a partition key of "ticker" (String). See the provided
	// PowerShell commands in the README or user instructions for how to
	// create this table.
	CompanyCikTable = "company_cik_map"
)
