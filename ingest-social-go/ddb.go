package main

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// DynamoStore wraps the AWS DynamoDB client and provides helper methods for
// locking, state management and YouTube video ID insertion. See the
// specification for the precise contract of each operation.
type DynamoStore struct {
	client *dynamodb.Client
}

// ErrLockNotAcquired is returned by AcquireLock when another worker
// currently holds the lock for the requested source/ticker pair. Callers
// should treat this as a non-fatal condition and exit with status code 2
// without retrying. The returned error may wrap this sentinel to provide
// additional context.
var ErrLockNotAcquired = errors.New("lock not acquired")

// NewDynamoStore instantiates a new DynamoStore bound to the configured
// AWS region. The underlying client uses the default credential chain.
func NewDynamoStore(ctx context.Context) (*DynamoStore, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(AwsRegion))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	cli := dynamodb.NewFromConfig(cfg)
	return &DynamoStore{client: cli}, nil
}

// AcquireLock attempts to insert a row into the locks table. It will
// succeed if the row does not already exist or if the existing lock has
// expired. On success the caller becomes the lock owner until it either
// releases the lock or the TTL expires. The ownerID should uniquely
// identify the current process/job.
func (s *DynamoStore) AcquireLock(ctx context.Context, source, ticker, ownerID, jobID string) error {
	pk := fmt.Sprintf("%s#%s", strings.ToLower(source), strings.ToUpper(ticker))
	now := time.Now().UTC().Unix()
	expires := now + LockTTLSeconds
	item := map[string]types.AttributeValue{
		"pk":         &types.AttributeValueMemberS{Value: pk},
		"owner_id":   &types.AttributeValueMemberS{Value: ownerID},
		"job_id":     &types.AttributeValueMemberS{Value: jobID},
		"expires_at": &types.AttributeValueMemberN{Value: strconv.FormatInt(expires, 10)},
	}
	// Condition: lock row does not exist or it has expired
	cond := "attribute_not_exists(pk) OR expires_at < :now"
	input := &dynamodb.PutItemInput{
		TableName:           aws.String(DdbLocksTable),
		Item:                item,
		ConditionExpression: aws.String(cond),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now": &types.AttributeValueMemberN{Value: strconv.FormatInt(now, 10)},
		},
	}
	_, err := s.client.PutItem(ctx, input)
	if err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			// Another worker holds the lock. Return the sentinel so callers
			// can exit gracefully. Wrap the sentinel to preserve context.
			return fmt.Errorf("lock already held for %s: %w", pk, ErrLockNotAcquired)
		}
		return err
	}
	return nil
}

// HeartbeatLock extends the lock TTL for the specified owner. If the
// condition fails the caller no longer owns the lock and should abort. The
// new expiration time is computed from the current time.
func (s *DynamoStore) HeartbeatLock(ctx context.Context, source, ticker, ownerID string) error {
	pk := fmt.Sprintf("%s#%s", strings.ToLower(source), strings.ToUpper(ticker))
	newExp := time.Now().UTC().Unix() + LockTTLSeconds
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(DdbLocksTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression:    aws.String("SET expires_at = :newExp"),
		ConditionExpression: aws.String("owner_id = :me"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newExp": &types.AttributeValueMemberN{Value: strconv.FormatInt(newExp, 10)},
			":me":     &types.AttributeValueMemberS{Value: ownerID},
		},
	}
	_, err := s.client.UpdateItem(ctx, input)
	if err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return fmt.Errorf("heartbeat failed, lock not owned: %w", err)
		}
		return err
	}
	return nil
}

// ReleaseLock deletes the lock row if the caller is still the owner. If the
// condition fails the lock is assumed to have been lost and nothing is
// deleted.
func (s *DynamoStore) ReleaseLock(ctx context.Context, source, ticker, ownerID string) error {
	pk := fmt.Sprintf("%s#%s", strings.ToLower(source), strings.ToUpper(ticker))
	input := &dynamodb.DeleteItemInput{
		TableName: aws.String(DdbLocksTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		ConditionExpression: aws.String("owner_id = :me"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":me": &types.AttributeValueMemberS{Value: ownerID},
		},
	}
	_, err := s.client.DeleteItem(ctx, input)
	if err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return fmt.Errorf("release failed, lock not owned: %w", err)
		}
		return err
	}
	return nil
}

// GetStocktwitsState retrieves the latest and oldest processed message IDs
// from the state table. If the row does not exist the returned values are
// zeros.
func (s *DynamoStore) GetStocktwitsState(ctx context.Context, ticker string) (latestID, oldestID int64, err error) {
	pk := fmt.Sprintf("stocktwits#%s", strings.ToUpper(ticker))
	input := &dynamodb.GetItemInput{
		TableName: aws.String(DdbStateTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
	}
	resp, err := s.client.GetItem(ctx, input)
	if err != nil {
		return 0, 0, err
	}
	if len(resp.Item) == 0 {
		return 0, 0, nil
	}
	if v, ok := resp.Item["latest_id"].(*types.AttributeValueMemberN); ok {
		if i, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			latestID = i
		}
	}
	if v, ok := resp.Item["oldest_id"].(*types.AttributeValueMemberN); ok {
		if i, err := strconv.ParseInt(v.Value, 10, 64); err == nil {
			oldestID = i
		}
	}
	return latestID, oldestID, nil
}

// UpdateStocktwitsState persists the provided latest and oldest IDs along
// with an updated_at timestamp. The timestamp is always the current time in
// RFC3339 format. This method will create the row if it does not exist.
// ddb.go
func (s *DynamoStore) UpdateStocktwitsState(ctx context.Context, ticker string, latestID, oldestID int64) error {
	pk := fmt.Sprintf("stocktwits#%s", strings.ToUpper(ticker))
	now := time.Now().UTC().Format(time.RFC3339)

	if oldestID > 0 {
		input := &dynamodb.UpdateItemInput{
			TableName:        aws.String(DdbStateTable),
			Key:              map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}},
			UpdateExpression: aws.String("SET latest_id=:latest, oldest_id=:oldest, updated_at=:now"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":latest": &types.AttributeValueMemberN{Value: strconv.FormatInt(latestID, 10)},
				":oldest": &types.AttributeValueMemberN{Value: strconv.FormatInt(oldestID, 10)},
				":now":    &types.AttributeValueMemberS{Value: now},
			},
		}
		_, err := s.client.UpdateItem(ctx, input)
		return err
	}

	// don't touch oldest_id if caller passed 0
	input := &dynamodb.UpdateItemInput{
		TableName:        aws.String(DdbStateTable),
		Key:              map[string]types.AttributeValue{"pk": &types.AttributeValueMemberS{Value: pk}},
		UpdateExpression: aws.String("SET latest_id=:latest, updated_at=:now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":latest": &types.AttributeValueMemberN{Value: strconv.FormatInt(latestID, 10)},
			":now":    &types.AttributeValueMemberS{Value: now},
		},
	}
	_, err := s.client.UpdateItem(ctx, input)
	return err
}

// GetYouTubeState retrieves the last_search_at timestamp (if any) for the
// specified ticker. If the row does not exist nil is returned for the
// timestamp.
func (s *DynamoStore) GetYouTubeState(ctx context.Context, ticker string) (*time.Time, error) {
	pk := fmt.Sprintf("youtube#%s", strings.ToUpper(ticker))
	input := &dynamodb.GetItemInput{
		TableName: aws.String(DdbStateTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
	}
	resp, err := s.client.GetItem(ctx, input)
	if err != nil {
		return nil, err
	}
	if len(resp.Item) == 0 {
		return nil, nil
	}
	if v, ok := resp.Item["last_search_at"].(*types.AttributeValueMemberS); ok {
		if ts, err := time.Parse(time.RFC3339, v.Value); err == nil {
			return &ts, nil
		}
	}
	return nil, nil
}

// UpdateYouTubeState sets last_search_at and updated_at to now for the
// specified ticker. This method will create the row if it does not exist.
func (s *DynamoStore) UpdateYouTubeState(ctx context.Context, ticker string) error {
	pk := fmt.Sprintf("youtube#%s", strings.ToUpper(ticker))
	now := time.Now().UTC().Format(time.RFC3339)
	input := &dynamodb.UpdateItemInput{
		TableName: aws.String(DdbStateTable),
		Key: map[string]types.AttributeValue{
			"pk": &types.AttributeValueMemberS{Value: pk},
		},
		UpdateExpression: aws.String("SET last_search_at = :now, updated_at = :now"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":now": &types.AttributeValueMemberS{Value: now},
		},
		ReturnValues: types.ReturnValueNone,
	}
	_, err := s.client.UpdateItem(ctx, input)
	return err
}

// InsertYouTubeVideoID attempts to insert a discovered video ID into the
// YouTube ID table. The per‑year partition key is derived from the ticker
// and the given year. The operation is idempotent – if the row already
// exists the method returns false with no error. Additional attributes
// include ticker, year, discovered_at, optional published_at and channel_id,
// and discovered_by_job_id. Times are formatted using RFC3339.
//
// Note: The argument order is (ticker, year, videoID, discoveredAt,
// publishedAt, channelID, discoveredByJobID). This allows callers to
// naturally specify the partitioning year immediately after the ticker.
func (s *DynamoStore) InsertYouTubeVideoID(ctx context.Context, ticker string, year int, videoID string, discoveredAt time.Time, publishedAt *time.Time, channelID string, discoveredByJobID string) (bool, error) {
	pky := fmt.Sprintf("%s#%d", strings.ToUpper(ticker), year)
	item := map[string]types.AttributeValue{
		"pky":                  &types.AttributeValueMemberS{Value: pky},
		"video_id":             &types.AttributeValueMemberS{Value: videoID},
		"ticker":               &types.AttributeValueMemberS{Value: strings.ToUpper(ticker)},
		"year":                 &types.AttributeValueMemberN{Value: strconv.Itoa(year)},
		"discovered_at":        &types.AttributeValueMemberS{Value: discoveredAt.UTC().Format(time.RFC3339)},
		"discovered_by_job_id": &types.AttributeValueMemberS{Value: discoveredByJobID},
	}
	if channelID != "" {
		item["channel_id"] = &types.AttributeValueMemberS{Value: channelID}
	}
	if publishedAt != nil {
		item["published_at"] = &types.AttributeValueMemberS{Value: publishedAt.UTC().Format(time.RFC3339)}
	}
	cond := "attribute_not_exists(video_id)"
	input := &dynamodb.PutItemInput{
		TableName:           aws.String(DdbYTIdsTable),
		Item:                item,
		ConditionExpression: aws.String(cond),
	}
	_, err := s.client.PutItem(ctx, input)
	if err != nil {
		var ccfe *types.ConditionalCheckFailedException
		if errors.As(err, &ccfe) {
			return false, nil // duplicate
		}
		return false, err
	}
	return true, nil
}
