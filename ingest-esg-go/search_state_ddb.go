package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dynamodbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// SearchStateStore tracks which pages have been searched for each query to enable pagination.
// Uses the same table as ESG URL dedup (fin-guard-esg-seen) but with different PK prefix.
// PK: "search_state#{TICKER}" (vs "esg#{TICKER}" for URLs)
// SK: "{query_type}#{query_text}" (e.g., "year#NVIDIA CORP ESG 2024")
// Attributes: last_page (number), updated_at (string)
type SearchStateStore struct {
	client *dynamodb.Client
	table  string
}

func NewSearchStateStore(ctx context.Context) (*SearchStateStore, error) {
	// Reuse the existing ESG seen table instead of creating a new one
	table := "fin-guard-esg-seen"
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	svc := dynamodb.NewFromConfig(cfg)
	return &SearchStateStore{client: svc, table: table}, nil
}

// GetLastPage returns the last page number searched for this query, or 0 if never searched.
func (s *SearchStateStore) GetLastPage(ctx context.Context, ticker, queryType, queryText string) (int, error) {
	pk := fmt.Sprintf("search_state#%s", strings.ToUpper(ticker))
	sk := fmt.Sprintf("%s#%s", queryType, queryText)

	key := map[string]dynamodbtypes.AttributeValue{
		"pk": &dynamodbtypes.AttributeValueMemberS{Value: pk},
		"sk": &dynamodbtypes.AttributeValueMemberS{Value: sk},
	}

	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:            aws.String(s.table),
		Key:                  key,
		ProjectionExpression: aws.String("last_page"),
	})
	if err != nil {
		return 0, err
	}
	if len(out.Item) == 0 {
		return 0, nil
	}

	if v, ok := out.Item["last_page"]; ok {
		if nv, ok2 := v.(*dynamodbtypes.AttributeValueMemberN); ok2 {
			var page int
			fmt.Sscanf(nv.Value, "%d", &page)
			return page, nil
		}
	}
	return 0, nil
}

// SetLastPage updates the last page searched for this query.
func (s *SearchStateStore) SetLastPage(ctx context.Context, ticker, queryType, queryText string, page int) error {
	pk := fmt.Sprintf("search_state#%s", strings.ToUpper(ticker))
	sk := fmt.Sprintf("%s#%s", queryType, queryText)

	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key: map[string]dynamodbtypes.AttributeValue{
			"pk": &dynamodbtypes.AttributeValueMemberS{Value: pk},
			"sk": &dynamodbtypes.AttributeValueMemberS{Value: sk},
		},
		UpdateExpression: aws.String("SET last_page = :page"),
		ExpressionAttributeValues: map[string]dynamodbtypes.AttributeValue{
			":page": &dynamodbtypes.AttributeValueMemberN{Value: fmt.Sprintf("%d", page)},
		},
	})
	return err
}
