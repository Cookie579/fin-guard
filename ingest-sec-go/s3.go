package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

var bucketName = "fin-processed-filings"

func SaveToS3(filing Filing) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))
	s3svc := s3.New(sess)

	data, err := json.Marshal(filing)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s/%s-%s.json", filing.CompanyName, filing.FormType, filing.FilingDate)

	_, err = s3svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return err
	}

	log.Printf("✅ Saved filing to S3: %s", key)
	return nil
}
