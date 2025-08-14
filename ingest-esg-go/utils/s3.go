package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

func CheckIfExists(bucket, key string) (bool, error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), // ✅ add region here
	}))
	svc := s3.New(sess)

	_, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// PrefixExists checks if at least one object exists with the given prefix.
func PrefixExists(bucket, prefix string) (bool, error) {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
	svc := s3.New(sess)

	out, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int64(1),
	})
	if err != nil {
		return false, err
	}
	return *out.KeyCount > 0, nil
}

func SaveDomainMap(bucket, key string, data map[string]string) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	svc := s3.New(sess)

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(jsonData),
	})
	return err
}

func SaveDomainEntries(bucket, key string, entries map[string]DomainEntry) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	svc := s3.New(sess)

	jsonData, err := json.Marshal(entries)
	if err != nil {
		return err
	}

	_, err = svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(jsonData),
	})
	return err
}

// PutDomainToS3 saves a single DomainEntry as JSON
func PutDomainToS3(bucket, key string, entry *DomainEntry) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	svc := s3.New(sess)

	body, _ := json.Marshal(entry)
	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	return err
}

// PutLinkRecordToS3 saves one ESGLinkRecord as its own JSON file
func PutLinkRecordToS3(bucket, key string, rec ESGLinkRecord) error {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
	svc := s3.New(sess)

	body, _ := json.Marshal(rec)
	_, err := svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(body),
	})
	if err == nil {
		log.Printf("✅ Saved JSON to s3://%s/%s", bucket, key)
	}
	return err
}

func LoadJSONFromS3(bucket, key string) ([]byte, error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))

	svc := s3.New(sess)

	out, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer out.Body.Close()

	var buf bytes.Buffer
	_, err = io.Copy(&buf, out.Body)
	if err != nil {
		return nil, err
	}

	log.Printf("✅ Loaded %s from s3://%s", key, bucket)
	return buf.Bytes(), nil
}

func FileExistsInS3(bucket, key string) (bool, error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	svc := s3.New(sess)

	_, err := svc.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

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
	// OLD: return filepath.Join("reports", ticker, filename)
	return filepath.Join("esg-reports", ticker, filename)
}
