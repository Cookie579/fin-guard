package main

import (
	"encoding/json"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

func SendToStream(streamName, ticker string, items []SearchItem) error {
	sess := session.Must(session.NewSession())
	svc := kinesis.New(sess)

	data, err := json.Marshal(struct {
		Ticker  string       `json:"ticker"`
		Results []SearchItem `json:"results"`
	}{
		Ticker:  ticker,
		Results: items,
	})
	if err != nil {
		return err
	}

	_, err = svc.PutRecord(&kinesis.PutRecordInput{
		StreamName:   aws.String(streamName),
		PartitionKey: aws.String(ticker),
		Data:         data,
	})
	if err == nil {
		log.Printf("📤 Sent %d links to Kinesis for %s", len(items), ticker)
	}
	return err
}
