package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"

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

	// ✅ Use sanitized company name as folder
	safeCompany := sanitizePath(filing.CompanyName)
	key := fmt.Sprintf("%s/%s-%s.json", safeCompany, filing.FormType, filing.FilingDate)

	_, err = s3svc.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		return err
	}

	log.Printf("✅ Saved filing to S3: s3://%s/%s", bucketName, key)
	return nil
}

// sanitizePath makes a string safe for use as an S3 path segment
func sanitizePath(name string) string {
	// Replace spaces with underscores and remove problematic characters
	name = strings.ReplaceAll(name, " ", "_")
	var safe []rune
	for _, r := range name {
		if (r >= 'a' && r <= 'z') ||
			(r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') ||
			r == '_' || r == '-' {
			safe = append(safe, r)
		}
	}
	return string(safe)
}
