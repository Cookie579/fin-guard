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
    "os"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// ESGDDBStore wraps an AWS DynamoDB client and the target table. It
// provides helper methods for deduplicating URLs on a per‑ticker basis.
type ESGDDBStore struct {
    client *dynamodb.DynamoDB
    table  string
}

// NewESGDDBStore constructs a new ESGDDBStore using the default AWS
// credential chain. The table name is derived from the ESG_DDB_TABLE
// environment variable or falls back to ``fin-guard-esg-seen`` if unset.
func NewESGDDBStore(ctx context.Context) (*ESGDDBStore, error) {
    // Determine the table name from environment or default.
    table := os.Getenv("ESG_DDB_TABLE")
    if table == "" {
        table = "fin-guard-esg-seen"
    }
    // Use the default region for the AWS SDK. If AWS_REGION is set the
    // session will pick it up automatically. S3/Kinesis elsewhere in this
    // repository explicitly set us‑east‑1 but leaving it unset here allows
    // override via env.
    sess, err := session.NewSession()
    if err != nil {
        return nil, err
    }
    svc := dynamodb.New(sess)
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
    out, err := s.client.GetItemWithContext(ctx, &dynamodb.GetItemInput{
        TableName: aws.String(s.table),
        Key: map[string]*dynamodb.AttributeValue{
            "pk": {S: aws.String(pk)},
            "sk": {S: aws.String(sk)},
        },
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
    av, err := dynamodbattribute.MarshalMap(item)
    if err != nil {
        return err
    }
    _, err = s.client.PutItemWithContext(ctx, &dynamodb.PutItemInput{
        TableName: aws.String(s.table),
        Item:      av,
    })
    return err
}