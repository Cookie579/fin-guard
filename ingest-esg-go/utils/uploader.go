package utils

import (
	"context"
	"io"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func NewS3Uploader(region string) *s3manager.Uploader {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	return s3manager.NewUploader(sess)
}

func UploadToS3(ctx context.Context, uploader *s3manager.Uploader, bucket, key string, reader io.Reader, contentType string) error {
	_, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        reader,
		ContentType: aws.String(contentType),
	})
	return err
}

func BuildS3Key(ticker, filename string) string {
	return filepath.Join("reports", ticker, filename)
}
