package connector

import (
	"log"

	apexlog "github.com/apex/log"
	"github.com/apex/log/handlers/discard"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
)

const (
	defaultMaxBatchCount = 1000
)

// NewConsumer creates a new consumer with initialied kinesis connection
func NewConsumer(appName, streamName string, cfg Config) *Consumer {
	if cfg.LogHandler == nil {
		cfg.LogHandler = discard.New()
	}

	if cfg.MaxBatchCount == 0 {
		cfg.MaxBatchCount = defaultMaxBatchCount
	}

	svc := kinesis.New(
		session.New(
			aws.NewConfig().WithMaxRetries(10),
		),
	)

	return &Consumer{
		appName:    appName,
		streamName: streamName,
		svc:        svc,
		cfg:        cfg,
	}
}

type Consumer struct {
	appName    string
	streamName string
	svc        *kinesis.Kinesis
	cfg        Config
}

// Start takes a handler and then loops over each of the shards
// processing each one with the handler.
func (c *Consumer) Start(handler Handler) {
	apexlog.SetHandler(c.cfg.LogHandler)

	resp, err := c.svc.DescribeStream(
		&kinesis.DescribeStreamInput{
			StreamName: aws.String(c.streamName),
		},
	)

	if err != nil {
		log.Fatalf("Error DescribeStream %v", err)
	}

	for _, shard := range resp.StreamDescription.Shards {
		go c.handlerLoop(*shard.ShardId, handler)
	}
}

func (c *Consumer) handlerLoop(shardID string, handler Handler) {
	ctx := apexlog.WithFields(apexlog.Fields{
		"app":    c.appName,
		"stream": c.streamName,
		"shard":  shardID,
	})

	buf := &Buffer{
		MaxBatchCount: c.cfg.MaxBatchCount,
	}

	checkpoint := &Checkpoint{
		AppName:    c.appName,
		StreamName: c.streamName,
	}

	params := &kinesis.GetShardIteratorInput{
		ShardId:    aws.String(shardID),
		StreamName: aws.String(c.streamName),
	}

	if checkpoint.CheckpointExists(shardID) {
		params.ShardIteratorType = aws.String("AFTER_SEQUENCE_NUMBER")
		params.StartingSequenceNumber = aws.String(checkpoint.SequenceNumber())
	} else {
		params.ShardIteratorType = aws.String("TRIM_HORIZON")
	}

	resp, err := c.svc.GetShardIterator(params)
	if err != nil {
		log.Fatalf("Error GetShardIterator %v", err)
	}

	shardIterator := resp.ShardIterator
	ctx.Info("processing")

	for {
		resp, err := c.svc.GetRecords(
			&kinesis.GetRecordsInput{
				ShardIterator: shardIterator,
			},
		)

		if err != nil {
			log.Fatalf("Error GetRecords %v", err)
		}

		if len(resp.Records) > 0 {
			for _, r := range resp.Records {
				buf.AddRecord(r)

				if buf.ShouldFlush() {
					handler.HandleRecords(*buf)
					ctx.WithField("count", buf.RecordCount()).Info("emitted")
					checkpoint.SetCheckpoint(shardID, buf.LastSeq())
					buf.Flush()
				}
			}
		} else if resp.NextShardIterator == aws.String("") || shardIterator == resp.NextShardIterator {
			log.Fatalf("Error NextShardIterator")
		}

		shardIterator = resp.NextShardIterator
	}
}
