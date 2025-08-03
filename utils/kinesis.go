package utils

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

var streamName = "fin-guard-stream"

func PublishToKinesis(filing Filing) error {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"),
	}))

	svc := kinesis.New(sess)

	data, err := json.Marshal(filing)
	if err != nil {
		return err
	}

	out, err := svc.PutRecord(&kinesis.PutRecordInput{
		Data:         data,
		PartitionKey: aws.String(filing.Company),
		StreamName:   aws.String(streamName),
	})

	if err != nil {
		log.Printf("❌ Failed to send to Kinesis: %v", err)
		return err
	}

	log.Printf("✅ Record written to Kinesis")
	log.Printf("  ➤ SequenceNumber: %s", *out.SequenceNumber)
	log.Printf("  ➤ ShardId:        %s", *out.ShardId)

	return nil
}
