package main

// company_cik_map.go defines a simple helper for looking up a
// company’s human‑readable name (and optional CIK) from a DynamoDB
// table keyed by ticker. When performing ESG ingestion we want to
// construct search queries using the full company name rather than
// just the ticker symbol. This store provides a thin wrapper over
// DynamoDB’s GetItem API and mirrors the patterns used in other
// DDB helpers in this repository.

import (
    "context"
    "strings"

    // AWS SDK v2 packages. See documentation for migrating from v1 to v2.
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CompanyCIKMapStore wraps a DynamoDB client and a table name. The
// table is expected to have a primary key named "ticker" (string)
// and attributes "name" and "cik" (both strings). The ticker
// attribute is normalised to upper case before being used as the key.
// The table name is fixed to "company_cik_map" rather than being
// configurable via environment variables to simplify deployment.
type CompanyCIKMapStore struct {
    client *dynamodb.Client
    table  string
}

// NewCompanyCIKMapStore constructs a new store using the default AWS
// credential chain. It initialises a DynamoDB client from the
// environment and sets the table name to "company_cik_map". If
// configuration or client creation fails an error is returned and
// callers should fall back to using the ticker as the company name.
func NewCompanyCIKMapStore(ctx context.Context) (*CompanyCIKMapStore, error) {
    table := "company_cik_map"
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        return nil, err
    }
    svc := dynamodb.NewFromConfig(cfg)
    return &CompanyCIKMapStore{client: svc, table: table}, nil
}

// GetCompanyName looks up the provided ticker in the company_cik_map
// table. It returns the company name and CIK when present. If no
// record exists or the name attribute is empty the returned strings
// will be empty. Errors from DynamoDB are propagated to the caller.
// The ticker is upper‑cased and trimmed prior to lookup.
func (s *CompanyCIKMapStore) GetCompanyName(ctx context.Context, ticker string) (string, string, error) {
    tk := strings.ToUpper(strings.TrimSpace(ticker))
    key := map[string]dynamodbtypes.AttributeValue{
        "ticker": &dynamodbtypes.AttributeValueMemberS{Value: tk},
    }
    out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
        TableName: aws.String(s.table),
        Key:       key,
        // Only fetch the name and cik attributes; other attributes are ignored.
        ProjectionExpression: aws.String("name, cik"),
    })
    if err != nil {
        return "", "", err
    }
    if len(out.Item) == 0 {
        return "", "", nil
    }
    var name string
    var cik string
    if v, ok := out.Item["name"]; ok {
        if sv, ok2 := v.(*dynamodbtypes.AttributeValueMemberS); ok2 {
            name = sv.Value
        }
    }
    if v, ok := out.Item["cik"]; ok {
        if sv, ok2 := v.(*dynamodbtypes.AttributeValueMemberS); ok2 {
            cik = sv.Value
        }
    }
    return name, cik, nil
}