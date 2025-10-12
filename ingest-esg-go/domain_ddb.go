package main

// domain_ddb.go provides a simple DynamoDB-backed cache for company
// domains.  When discovering a company's official website we first
// consult this cache to avoid hitting the external Google Custom Search
// API on every invocation.  When a new domain is found it is written
// back to the cache for future runs.  The table name is controlled via
// the environment variable ESG_DOMAIN_DDB_TABLE; if unset a default
// "fin-guard-esg-domain" is used.

import (
    "context"
    "os"
    "strings"
    "time"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/dynamodb"
    "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// DomainRecord represents a row in the domain cache table.  Ticker is
// the partition key.  Company and Domain are included to retain
// human‑readable context, while IngestTS records when the entry was
// created.
type DomainRecord struct {
    Ticker   string `dynamodbav:"ticker"`
    Company  string `dynamodbav:"company"`
    Domain   string `dynamodbav:"domain"`
    IngestTS string `dynamodbav:"ingest_ts"`
}

// DomainDDBStore wraps a DynamoDB client and a table name.  It
// provides helpers to retrieve and persist domain information.
type DomainDDBStore struct {
    client *dynamodb.DynamoDB
    table  string
}

// NewDomainDDBStore constructs a new store using the default AWS
// credential chain.  It reads the target table name from
// ESG_DOMAIN_DDB_TABLE or falls back to a sensible default when unset.
func NewDomainDDBStore(ctx context.Context) (*DomainDDBStore, error) {
    table := os.Getenv("ESG_DOMAIN_DDB_TABLE")
    if table == "" {
        table = "fin-guard-esg-domain"
    }
    sess, err := session.NewSession()
    if err != nil {
        return nil, err
    }
    svc := dynamodb.New(sess)
    return &DomainDDBStore{client: svc, table: table}, nil
}

// GetDomain returns the cached domain for the given ticker.  If the
// record does not exist or the domain attribute is empty an empty
// string is returned.  Errors from DynamoDB are propagated to the
// caller.  The ticker is normalised to upper case.
func (s *DomainDDBStore) GetDomain(ctx context.Context, ticker string) (string, error) {
    tk := strings.ToUpper(strings.TrimSpace(ticker))
    out, err := s.client.GetItemWithContext(ctx, &dynamodb.GetItemInput{
        TableName: aws.String(s.table),
        Key: map[string]*dynamodb.AttributeValue{
            "ticker": {S: aws.String(tk)},
        },
        ProjectionExpression: aws.String("domain"),
    })
    if err != nil {
        return "", err
    }
    if len(out.Item) == 0 {
        return "", nil
    }
    if v, ok := out.Item["domain"]; ok && v.S != nil {
        return *v.S, nil
    }
    return "", nil
}

// PutDomain stores the provided domain for the given ticker.  The
// company name is stored verbatim to aid debugging and future
// migrations, while the ingest timestamp records when the entry was
// added.  Errors from DynamoDB are returned to the caller.
func (s *DomainDDBStore) PutDomain(ctx context.Context, ticker, company, domain string) error {
    item := DomainRecord{
        Ticker:   strings.ToUpper(strings.TrimSpace(ticker)),
        Company:  company,
        Domain:   domain,
        IngestTS: time.Now().UTC().Format(time.RFC3339),
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