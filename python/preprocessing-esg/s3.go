package main

import (
	"bytes"
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
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
