package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// CompanyCikStore wraps a DynamoDB client and provides helper methods
// for retrieving and storing ticker → CIK mappings. The table name and
// AWS region are defined in constants.go. The partition key is the
// upper‑cased ticker symbol, stored under the attribute name "ticker".
// Additional attributes include "cik" and "name" for the numeric CIK
// string and the company title respectively.
type CompanyCikStore struct {
	client *dynamodb.Client
}

// NewCompanyCikStore instantiates a new DynamoDB client bound to the
// configured AWS region. The default credential chain is used for
// authentication. If configuration loading fails, the returned error
// wraps the underlying cause.
func NewCompanyCikStore(ctx context.Context) (*CompanyCikStore, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(AwsRegion))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	cli := dynamodb.NewFromConfig(cfg)
	return &CompanyCikStore{client: cli}, nil
}

// GetCIK looks up the given ticker in the CompanyCikTable and
// returns the associated CIK and company name if present. If the
// ticker is not found the returned cik and name will be empty strings.
// No error is returned for a missing record. Any DynamoDB error will
// be returned to the caller.
func (s *CompanyCikStore) GetCIK(ctx context.Context, ticker string) (cik string, name string, err error) {
	pk := strings.ToUpper(ticker)
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(CompanyCikTable),
		Key: map[string]types.AttributeValue{
			"ticker": &types.AttributeValueMemberS{Value: pk},
		},
	})
	if err != nil {
		return "", "", err
	}
	if out.Item == nil || len(out.Item) == 0 {
		return "", "", nil
	}
	if v, ok := out.Item["cik"].(*types.AttributeValueMemberS); ok {
		cik = v.Value
	}
	if v, ok := out.Item["name"].(*types.AttributeValueMemberS); ok {
		name = v.Value
	}
	return cik, name, nil
}

// PutCIK upserts a ticker → CIK mapping into the CompanyCikTable. The
// ticker is upper‑cased to provide a consistent partition key. No
// conditional logic is applied; existing mappings will be overwritten.
// Only DynamoDB errors are returned to the caller.
func (s *CompanyCikStore) PutCIK(ctx context.Context, ticker, cik, name string) error {
	pk := strings.ToUpper(ticker)
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(CompanyCikTable),
		Item: map[string]types.AttributeValue{
			"ticker": &types.AttributeValueMemberS{Value: pk},
			"cik":    &types.AttributeValueMemberS{Value: cik},
			"name":   &types.AttributeValueMemberS{Value: name},
		},
	})
	return err
}
