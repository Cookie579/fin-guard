package main

// s3.go provides helpers for reading and writing data to Amazon S3.
// These functions are extracted from utils/s3.go and placed in the
// top‑level package so that S3 operations can be used by domain and
// ESG scraping code without referencing a separate package.

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

// CheckIfExists returns true if an object exists at the given bucket and
// key. A NotFound error is not considered fatal. Any other error is
// returned to the caller. Region defaults to us-east-1 for
// backwards compatibility.
func CheckIfExists(bucket, key string) (bool, error) {
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
    svc := s3.New(sess)
    _, err := svc.HeadObject(&s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
    if err != nil {
        if aerr, ok := err.(awserr.Error); ok && aerr.Code() == "NotFound" {
            return false, nil
        }
        return false, err
    }
    return true, nil
}

// PrefixExists checks whether any object exists with the given prefix
// in the specified bucket. It returns true if at least one object is
// found. Errors from ListObjectsV2 are propagated.
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

// SaveDomainMap writes a map of company→domain to S3 as JSON. It
// overwrites any existing object at the given key.
func SaveDomainMap(bucket, key string, data map[string]string) error {
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
    svc := s3.New(sess)
    jsonData, err := json.Marshal(data)
    if err != nil {
        return err
    }
    _, err = svc.PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(jsonData)})
    return err
}

// SaveDomainEntries writes a map of ticker→DomainEntry to S3 as JSON.
func SaveDomainEntries(bucket, key string, entries map[string]DomainEntry) error {
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
    svc := s3.New(sess)
    jsonData, err := json.Marshal(entries)
    if err != nil {
        return err
    }
    _, err = svc.PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(jsonData)})
    return err
}

// PutDomainToS3 writes a single DomainEntry to S3 as JSON.
func PutDomainToS3(bucket, key string, entry *DomainEntry) error {
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
    svc := s3.New(sess)
    body, _ := json.Marshal(entry)
    _, err := svc.PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
    return err
}

// PutLinkRecordToS3 writes a single ESGLinkRecord to S3. It logs
// success and returns any error from PutObject. The region defaults
// to us-east-1 to match historic behaviour.
func PutLinkRecordToS3(bucket, key string, rec ESGLinkRecord) error {
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
    svc := s3.New(sess)
    body, _ := json.Marshal(rec)
    _, err := svc.PutObject(&s3.PutObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), Body: bytes.NewReader(body)})
    if err == nil {
        log.Printf("✅ Saved JSON to s3://%s/%s", bucket, key)
    }
    return err
}

// LoadJSONFromS3 downloads an object from S3 and returns its bytes.
// It logs a message on success. Errors are propagated.
func LoadJSONFromS3(bucket, key string) ([]byte, error) {
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
    svc := s3.New(sess)
    out, err := svc.GetObject(&s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)})
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

// FileExistsInS3 checks for the existence of a single object in S3.
// It is an alias for CheckIfExists retained for backwards
// compatibility.
func FileExistsInS3(bucket, key string) (bool, error) {
    return CheckIfExists(bucket, key)
}

// NewS3Uploader creates a new AWS S3 uploader for the specified region.
func NewS3Uploader(region string) *s3manager.Uploader {
    sess := session.Must(session.NewSession(&aws.Config{Region: aws.String(region)}))
    return s3manager.NewUploader(sess)
}

// UploadToS3 uploads arbitrary content to S3 using an uploader. The
// content type is set on the object. Errors are returned to the caller.
func UploadToS3(ctx context.Context, uploader *s3manager.Uploader, bucket, key string, reader io.Reader, contentType string) error {
    _, err := uploader.UploadWithContext(ctx, &s3manager.UploadInput{
        Bucket:      aws.String(bucket),
        Key:         aws.String(key),
        Body:        reader,
        ContentType: aws.String(contentType),
    })
    return err
}

// BuildS3Key constructs a hierarchical key for storing ESG reports. It
// prepends "esg-reports" to the ticker and filename. This mirrors
// historic behaviour. If the hierarchy changes update this function.
func BuildS3Key(ticker, filename string) string {
    return filepath.Join("esg-reports", ticker, filename)
}