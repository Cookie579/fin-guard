package main

// blacklist_ddb.go provides a helper for loading a set of blocked
// domains from a DynamoDB table. Each item in the table should have a
// primary key named "domain" (string). The environment variable
// ESG_BLACKLIST_TABLE controls the table name; if unset the default
// "fin-guard-esg-blacklist" is used. When the table does not exist or
// cannot be read the caller should fall back to a local file or an
// empty set.

import (
    "context"
    "strings"
    "os"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
)

// LoadBlacklistFromDDB scans the blacklist table and returns a set of
// lower‑cased domains. It performs a full table scan using the AWS
// SDK. Callers should be aware that a Scan can be expensive on large
// tables; however the blacklist is expected to be small. Errors from
// DynamoDB are returned to the caller.
func LoadBlacklistFromDDB() (map[string]struct{}, error) {
    table := os.Getenv("ESG_BLACKLIST_TABLE")
    if table == "" {
        table = "fin-guard-esg-blacklist"
    }
    sess, err := session.NewSession()
    if err != nil {
        return nil, err
    }
    svc := dynamodb.New(sess)
    out := make(map[string]struct{})
    input := &dynamodb.ScanInput{
        TableName: aws.String(table),
        ProjectionExpression: aws.String("domain"),
    }
    // Use ScanPages to iterate through all pages of the scan result
    err = svc.ScanPagesWithContext(context.Background(), input, func(page *dynamodb.ScanOutput, last bool) bool {
        for _, item := range page.Items {
            if v, ok := item["domain"]; ok && v.S != nil {
                d := strings.ToLower(strings.TrimSpace(*v.S))
                if d != "" {
                    out[d] = struct{}{}
                }
            }
        }
        return true
    })
    if err != nil {
        return nil, err
    }
    return out, nil
}