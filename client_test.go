package consumer_test

import (
	"testing"

	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/aws/aws-sdk-go/service/kinesis/kinesisiface"
	"github.com/harlow/kinesis-consumer"
)

func TestKinesisClient_GetRecords_SuccessfullyRun(t *testing.T) {
	kinesisClient := &kinesisClientMock{
		getShardIteratorMock: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           make([]*kinesis.Record, 0),
			}, nil
		},
	}
	kinesisClientOpt := consumer.WithKinesis(kinesisClient)
	c, err := consumer.NewKinesisClient(kinesisClientOpt)
	if err != nil {
		t.Fatalf("New kinesis client error: %v", err)
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	recordsChan, errorsChan, err := c.GetRecords(ctx, "myStream", "shardId-000000000000", "")

	if recordsChan == nil {
		t.Errorf("records channel expected not nil, got %v", recordsChan)
	}
	if errorsChan == nil {
		t.Errorf("errors channel expected not nil, got %v", recordsChan)
	}
	if err != nil {
		t.Errorf("error expected nil, got %v", err)
	}

	cancelFunc()
}

func TestKinesisClient_GetRecords_SuccessfullyRetrievesThreeRecordsAtOnce(t *testing.T) {
	expectedResults := []*kinesis.Record{
		{
			SequenceNumber: aws.String("49578481031144599192696750682534686652010819674221576195"),
		},
		{
			SequenceNumber: aws.String("49578481031144599192696750682534686652010819674221576196"),
		},
		{
			SequenceNumber: aws.String("49578481031144599192696750682534686652010819674221576197"),
		}}
	kinesisClient := &kinesisClientMock{
		getShardIteratorMock: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           expectedResults,
			}, nil
		},
	}
	kinesisClientOpt := consumer.WithKinesis(kinesisClient)
	c, err := consumer.NewKinesisClient(kinesisClientOpt)
	if err != nil {
		t.Fatalf("new kinesis client error: %v", err)
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	recordsChan, _, err := c.GetRecords(ctx, "TestStream", "shardId-000000000000", "")

	if recordsChan == nil {
		t.Fatalf("records channel expected not nil, got %v", recordsChan)
	}
	if err != nil {
		t.Fatalf("error expected nil, got %v", err)
	}
	var results []*consumer.Record
	results = append(results, <-recordsChan, <-recordsChan, <-recordsChan)
	if len(results) != 3 {
		t.Errorf("number of records expected 3, got %v", len(results))
	}
	for i, r := range results {
		if r != expectedResults[i] {
			t.Errorf("record expected %v, got %v", expectedResults[i], r)
		}
	}

	cancelFunc()
}

func TestKinesisClient_GetRecords_ShardIsClosed(t *testing.T) {
	kinesisClient := &kinesisClientMock{
		getShardIteratorMock: func(input *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
			return &kinesis.GetShardIteratorOutput{
				ShardIterator: aws.String("49578481031144599192696750682534686652010819674221576194"),
			}, nil
		},
		getRecordsMock: func(input *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
			return &kinesis.GetRecordsOutput{
				NextShardIterator: nil,
				Records:           make([]*consumer.Record, 0),
			}, nil
		},
	}
	kinesisClientOpt := consumer.WithKinesis(kinesisClient)
	c, err := consumer.NewKinesisClient(kinesisClientOpt)
	if err != nil {
		t.Fatalf("new kinesis client error: %v", err)
	}
	ctx, cancelFunc := context.WithCancel(context.Background())
	_, errorsChan, err := c.GetRecords(ctx, "TestStream", "shardId-000000000000", "")

	if errorsChan == nil {
		t.Fatalf("errors channel expected equals not nil, got %v", errorsChan)
	}
	if err != nil {
		t.Fatalf("error expected, got %v", err)
	}

	err = <-errorsChan
	if err == nil {
		t.Errorf("error expected, got %v", err)
	}

	cancelFunc()
}

type kinesisClientMock struct {
	kinesisiface.KinesisAPI
	getShardIteratorMock func(*kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error)
	getRecordsMock       func(*kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error)
}

func (c *kinesisClientMock) GetRecords(in *kinesis.GetRecordsInput) (*kinesis.GetRecordsOutput, error) {
	return c.getRecordsMock(in)
}

func (c *kinesisClientMock) GetShardIterator(in *kinesis.GetShardIteratorInput) (*kinesis.GetShardIteratorOutput, error) {
	return c.getShardIteratorMock(in)
}
