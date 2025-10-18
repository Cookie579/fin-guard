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
    "strings"
    "time"

    // AWS SDK v2 packages. See documentation for migrating from v1 to v2.
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
    "github.com/aws/aws-sdk-go-v2/service/dynamodb"
    dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
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
    client *dynamodb.Client
    table  string
}

// NewDomainDDBStore constructs a new store using the default AWS
// credential chain.  It reads the target table name from
// ESG_DOMAIN_DDB_TABLE or falls back to a sensible default when unset.
func NewDomainDDBStore(ctx context.Context) (*DomainDDBStore, error) {
    // Do not rely on environment variables for the table name; use a
    // sensible default directly. Environment variable ESG_DOMAIN_DDB_TABLE
    // is ignored to simplify configuration.
    table := "fin-guard-esg-domain"
    cfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        return nil, err
    }
    svc := dynamodb.NewFromConfig(cfg)
    return &DomainDDBStore{client: svc, table: table}, nil
}

// GetDomain returns the cached domain for the given ticker.  If the
// record does not exist or the domain attribute is empty an empty
// string is returned.  Errors from DynamoDB are propagated to the
// caller.  The ticker is normalised to upper case.
func (s *DomainDDBStore) GetDomain(ctx context.Context, ticker string) (string, error) {
    tk := strings.ToUpper(strings.TrimSpace(ticker))
    key := map[string]dynamodbtypes.AttributeValue{
        "ticker": &dynamodbtypes.AttributeValueMemberS{Value: tk},
    }
    out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
        TableName: aws.String(s.table),
        Key:       key,
        ProjectionExpression: aws.String("domain"),
    })
    if err != nil {
        return "", err
    }
    if len(out.Item) == 0 {
        return "", nil
    }
    if v, ok := out.Item["domain"]; ok {
        if sv, ok := v.(*dynamodbtypes.AttributeValueMemberS); ok {
            return sv.Value, nil
        }
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
