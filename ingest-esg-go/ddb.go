// ddb.go
//
// DynamoDB helper functions for ESG ingest. This file defines a simple
// client that stores and checks whether URLs have already been seen for a
// given ticker. Each item uses a composite key with a partition key of
// ``esg#<TICKER>`` and a sort key derived from a SHA‑1 hash of the URL.
// Additional attributes store the URL itself, optional title/source
// metadata and timestamps. The table name is read from the environment via
// ESG_DDB_TABLE. If unset the default name ``fin-guard-esg-seen`` is used.

package main

import (
    "context"
    "crypto/sha1"
    "encoding/hex"
    "fmt"
    "strings"
    "time"

    // AWS SDK v2 packages. The v1 SDK is deprecated as of July 31, 2025 so we
    // migrate to v2. LoadDefaultConfig constructs an AWS configuration using
    // the default credential chain and environment variables. The DynamoDB
    // client is created from this config and attributevalue provides the
    // MarshalMap helper to convert our structs to a map of AttributeValues【686512825217556†L286-L303】.
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ESGDDBStore wraps an AWS DynamoDB client and the target table. It
// provides helper methods for deduplicating URLs on a per‑ticker basis.
type ESGDDBStore struct {
    client *dynamodb.Client
    table  string
}

// NewESGDDBStore constructs a new ESGDDBStore using the default AWS
// credential chain. The table name is derived from the ESG_DDB_TABLE
// environment variable or falls back to “fin-guard-esg-seen“ if unset.
func NewESGDDBStore(ctx context.Context) (*ESGDDBStore, error) {
    // Determine the table name from environment or default. Do not rely on
    // environment variables for the table name; use a sensible default
    // directly. Environment variable ESG_DDB_TABLE is ignored to simplify
    // configuration.
    table := "fin-guard-esg-seen"
    // Load AWS configuration using the SDK v2. This picks up region and
    // credentials from the environment or shared config files. See the AWS
    // documentation for details.
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        return nil, err
    }
    // Create a DynamoDB client from the loaded config.
    svc := dynamodb.NewFromConfig(cfg)
    return &ESGDDBStore{client: svc, table: table}, nil
}

// hashURL computes the SHA‑1 hash of the provided URL and returns the
// hexadecimal string representation. Truncation is not applied because
// DynamoDB sort keys can accommodate the full 40‑character hex digest.
func hashURL(u string) string {
	h := sha1.Sum([]byte(u))
	return hex.EncodeToString(h[:])
}

// IsSeenURL checks whether the given URL has already been stored for the
// specified ticker. If an error occurs while contacting DynamoDB it is
// returned and the caller should decide how to proceed. A false return
// value means the URL is not yet stored.
func (s *ESGDDBStore) IsSeenURL(ctx context.Context, ticker, url string) (bool, error) {
    pk := fmt.Sprintf("esg#%s", strings.ToUpper(strings.TrimSpace(ticker)))
    sk := hashURL(url)
    // Build the key using the v2 types. Each key attribute is represented
    // using a specific AttributeValue type. Here we use AttributeValueMemberS
    // for string values【686512825217556†L286-L303】.
    key := map[string]dynamodbtypes.AttributeValue{
        "pk": &dynamodbtypes.AttributeValueMemberS{Value: pk},
        "sk": &dynamodbtypes.AttributeValueMemberS{Value: sk},
    }
    out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
        TableName: aws.String(s.table),
        Key:       key,
        ProjectionExpression: aws.String("pk"),
    })
    if err != nil {
        return false, err
    }
    return len(out.Item) > 0, nil
}

// markPayload represents the attributes persisted to DynamoDB when a URL is
// marked as seen. DynamoDB only stores non‑empty fields so omitting Title
// or Source is fine. Timestamps are stored as RFC3339 strings.
type markPayload struct {
	PK        string `dynamodbav:"pk"`
	SK        string `dynamodbav:"sk"`
	URL       string `dynamodbav:"url"`
	Title     string `dynamodbav:"title,omitempty"`
	Source    string `dynamodbav:"source,omitempty"`
	FirstSeen string `dynamodbav:"first_seen"`
	LastSeen  string `dynamodbav:"last_seen"`
}

// MarkSeenURL stores a record for the provided URL and ticker. If a row
// already exists it will be overwritten and the LastSeen timestamp will be
// updated. On success nil is returned. Use of this method should always
// follow a call to IsSeenURL to prevent unnecessary writes.
func (s *ESGDDBStore) MarkSeenURL(ctx context.Context, ticker, url, title, source string) error {
    pk := fmt.Sprintf("esg#%s", strings.ToUpper(strings.TrimSpace(ticker)))
    sk := hashURL(url)
    now := time.Now().UTC().Format(time.RFC3339)
    item := markPayload{
        PK:        pk,
        SK:        sk,
        URL:       url,
        Title:     title,
        Source:    source,
        FirstSeen: now,
        LastSeen:  now,
    }
    // Marshal the payload into a map of DynamoDB AttributeValues using the
    // v2 attributevalue package【686512825217556†L286-L303】.
    av, err := attributevalue.MarshalMap(item)
    if err != nil {
        return err
    }
    _, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
        TableName: aws.String(s.table),
        Item:      av,
    })
    return err
}
