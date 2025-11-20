package main

import (
    "context"
    "fmt"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// SecSeenStore wraps a DynamoDB client and provides helper methods for
// tracking which SEC filings have already been processed. Each entry
// inserted into the table uses a partition key (pk) prefixed with
// "SEC#" followed by the upper‑cased ticker. The sort key stores
// the filing accession number. An additional timestamp attribute is
// recorded to capture when the entry was inserted. Callers should
// instantiate this store once and reuse it across multiple HasSeen
// and MarkSeen calls.
type SecSeenStore struct {
    client *dynamodb.Client
}

// NewSecSeenStore initialises a DynamoDB client for the configured
// region and returns a SecSeenStore. It relies on the default AWS
// credential chain for authentication. On error, a non‑nil error is
// returned and the store should not be used.
func NewSecSeenStore(ctx context.Context) (*SecSeenStore, error) {
    cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(AwsRegion))
    if err != nil {
        return nil, fmt.Errorf("load AWS config: %w", err)
    }
    cli := dynamodb.NewFromConfig(cfg)
    return &SecSeenStore{client: cli}, nil
}

// HasSeen returns true if a record for the given ticker and accession
// already exists in the SecSeenTable. It performs a GetItem using the
// composite key (pk, accession). If the item is not found, it returns
// false with a nil error. Any DynamoDB error is returned to the caller.
func (s *SecSeenStore) HasSeen(ctx context.Context, ticker, accession string) (bool, error) {
    pk := fmt.Sprintf("SEC#%s", strings.ToUpper(ticker))
    out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
        TableName: aws.String(SecSeenTable),
        Key: map[string]types.AttributeValue{
            "pk":        &types.AttributeValueMemberS{Value: pk},
            "accession": &types.AttributeValueMemberS{Value: accession},
        },
    })
    if err != nil {
        return false, err
    }
    if out.Item == nil || len(out.Item) == 0 {
        return false, nil
    }
    return true, nil
}

// MarkSeen inserts or updates a record into the SecSeenTable for the
// given ticker and accession. It records the current UTC timestamp
// under the attribute "ts". No conditional checks are performed; an
// existing record will be overwritten. Any DynamoDB error is returned
// to the caller.
func (s *SecSeenStore) MarkSeen(ctx context.Context, ticker, accession string) error {
    pk := fmt.Sprintf("SEC#%s", strings.ToUpper(ticker))
    _, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
        TableName: aws.String(SecSeenTable),
        Item: map[string]types.AttributeValue{
            "pk":        &types.AttributeValueMemberS{Value: pk},
            "accession": &types.AttributeValueMemberS{Value: accession},
            "ts":        &types.AttributeValueMemberS{Value: time.Now().UTC().Format(time.RFC3339)},
        },
    })
    return err
}
