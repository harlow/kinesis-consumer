package consumer

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
)

type scanShardRunner struct {
	consumer *Consumer
	shardID  string
	fn       ScanFunc
}

func newScanShardRunner(consumer *Consumer, shardID string, fn ScanFunc) *scanShardRunner {
	return &scanShardRunner{
		consumer: consumer,
		shardID:  shardID,
		fn:       fn,
	}
}

func (r *scanShardRunner) run(ctx context.Context) error {
	lastSeqNum, err := r.loadCheckpoint()
	if err != nil {
		return err
	}

	shardIterator, lastSeqNum, err := r.loadIterator(ctx, lastSeqNum)
	if err != nil {
		return err
	}

	r.consumer.logger.Log("[CONSUMER] start scan:", r.shardID, lastSeqNum)
	defer func() {
		r.consumer.logger.Log("[CONSUMER] stop scan:", r.shardID)
	}()

	scanTicker := time.NewTicker(r.consumer.scanInterval)
	defer scanTicker.Stop()

	for {
		resp, err := r.getRecords(ctx, shardIterator)
		if err != nil {
			shardIterator, lastSeqNum, err = r.refreshIterator(ctx, lastSeqNum, err)
			if err != nil {
				return err
			}
		} else {
			shardIterator, lastSeqNum, err = r.handleResponse(ctx, shardIterator, lastSeqNum, resp)
			if err != nil {
				return err
			}
			if shardIterator == nil {
				return nil
			}
		}

		if !r.waitForNextPoll(ctx, scanTicker) {
			return nil
		}
	}
}

func (r *scanShardRunner) loadCheckpoint() (string, error) {
	lastSeqNum, err := r.consumer.group.GetCheckpoint(r.consumer.streamName, r.shardID)
	if err != nil {
		return "", fmt.Errorf("get checkpoint error: %w", err)
	}
	return lastSeqNum, nil
}

func (r *scanShardRunner) loadIterator(ctx context.Context, lastSeqNum string) (*string, string, error) {
	shardIterator, nextSeqNum, err := r.consumer.getShardIteratorWithCheckpointFallback(ctx, r.consumer.streamName, r.shardID, lastSeqNum)
	if err != nil {
		return nil, lastSeqNum, fmt.Errorf("get shard iterator error: %w", err)
	}
	return shardIterator, nextSeqNum, nil
}

func (r *scanShardRunner) getRecords(ctx context.Context, shardIterator *string) (*kinesis.GetRecordsOutput, error) {
	return r.consumer.client.GetRecords(ctx, &kinesis.GetRecordsInput{
		Limit:         aws.Int32(int32(r.consumer.maxRecords)),
		ShardIterator: shardIterator,
	}, r.consumer.getRecordsOpts...)
}

func (r *scanShardRunner) refreshIterator(ctx context.Context, lastSeqNum string, getRecordsErr error) (*string, string, error) {
	r.consumer.logger.Log("[CONSUMER] get records error:", getRecordsErr.Error())

	if ctx.Err() != nil {
		return nil, lastSeqNum, nil
	}

	if !isRetriableError(getRecordsErr) {
		return nil, lastSeqNum, fmt.Errorf("get records error: %w", getRecordsErr)
	}

	return r.loadIterator(ctx, lastSeqNum)
}

func (r *scanShardRunner) handleResponse(ctx context.Context, shardIterator *string, lastSeqNum string, resp *kinesis.GetRecordsOutput) (*string, string, error) {
	records, err := r.consumer.normalizeRecords(resp.Records)
	if err != nil {
		return nil, lastSeqNum, err
	}

	lastSeqNum, err = r.consumer.processRecords(ctx, r.shardID, records, resp.MillisBehindLatest, r.fn, lastSeqNum)
	if err != nil {
		return nil, lastSeqNum, err
	}

	if isShardClosed(resp.NextShardIterator, shardIterator) {
		if err := r.handleShardClosed(); err != nil {
			return nil, lastSeqNum, err
		}
		return nil, lastSeqNum, nil
	}

	return resp.NextShardIterator, lastSeqNum, nil
}

func (r *scanShardRunner) handleShardClosed() error {
	r.consumer.logger.Log("[CONSUMER] shard closed:", r.shardID)

	if r.consumer.shardClosedHandler == nil {
		return nil
	}
	if err := r.consumer.shardClosedHandler(r.consumer.streamName, r.shardID); err != nil {
		return fmt.Errorf("shard closed handler error: %w", err)
	}
	return nil
}

func (r *scanShardRunner) waitForNextPoll(ctx context.Context, scanTicker *time.Ticker) bool {
	select {
	case <-ctx.Done():
		return false
	case <-scanTicker.C:
		return true
	}
}
