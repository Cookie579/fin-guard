package main

// kinesis.go provides thin wrappers around the AWS Kinesis client to
// publish records. The functions here are extracted from the original
// utils/kinesis.go so they can be used by both domain discovery and
// ESG scraping code in the unified package.

import (
    "encoding/json"
    "log"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/kinesis"
)

var kinesisClient = kinesis.New(session.Must(session.NewSession()))

// SendToStreamRaw publishes an arbitrary payload to the given Kinesis
// stream. The caller must specify a partition key, typically the
// ticker or some other identifier to ensure ordering. Errors from
// PutRecord are returned to the caller.
func SendToStreamRaw(streamName, partitionKey string, data []byte) error {
    _, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{
        StreamName:   aws.String(streamName),
        PartitionKey: aws.String(partitionKey),
        Data:         data,
    })
    return err
}

// SendLinkRecord publishes an ESGLinkRecord to the specified stream.
// The partition key is the ticker. If the call succeeds a debug
// message is logged. Errors are propagated.
func SendLinkRecord(streamName string, rec ESGLinkRecord) error {
    data, _ := json.Marshal(rec)
    _, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{
        StreamName:   aws.String(streamName),
        PartitionKey: aws.String(rec.Ticker),
        Data:         data,
    })
    if err == nil {
        log.Printf("📤 Sent link to Kinesis for %s", rec.Ticker)
    }
    return err
}