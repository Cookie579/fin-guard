package utils

import (
	"bytes"
	"io"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

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
