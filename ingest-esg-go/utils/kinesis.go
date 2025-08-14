package main

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var kinesisClient = kinesis.New(session.Must(session.NewSession()))

func SendToStreamRaw(streamName, partitionKey string, data []byte) error {
	_, err := kinesisClient.PutRecord(&kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(partitionKey),
		Data:         data,
	})
	return err
}

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
