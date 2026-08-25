package consumer

import (
	"context"
	"fmt"
	"log/slog"
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
	lastSeqNum, err := r.loadCheckpoint(ctx)
	if err != nil {
		return err
	}

	shardIterator, lastSeqNum, err := r.loadIterator(ctx, lastSeqNum)
	if err != nil {
		return err
	}

	r.consumer.logger.DebugContext(ctx, "start scan", slog.String("shard-id", r.shardID), slog.String("last-sequence-number", lastSeqNum))
	defer func() {
		r.consumer.logger.DebugContext(ctx, "stop scan", slog.String("shard-id", r.shardID))
	}()

	scanTicker := time.NewTicker(r.consumer.scanInterval)
	defer scanTicker.Stop()
	retryAttempt := 0

	for {
		resp, err := r.getRecords(ctx, shardIterator)
		if err != nil {
			retryAttempt++
			shardIterator, lastSeqNum, retryAttempt, err = r.refreshIterator(ctx, lastSeqNum, err, retryAttempt)
			if err != nil {
				return err
			}
			if shardIterator == nil {
				return nil
			}
		} else {
			shardIterator, lastSeqNum, err = r.handleResponse(ctx, shardIterator, lastSeqNum, resp)
			if err != nil {
				return err
			}
			if shardIterator == nil {
				return nil
			}
			retryAttempt = 0
		}

		if !r.waitForNextPoll(ctx, scanTicker) {
			return nil
		}
	}
}

func (r *scanShardRunner) loadCheckpoint(ctx context.Context) (string, error) {
	lastSeqNum, err := r.consumer.group.GetCheckpoint(ctx, r.consumer.streamName, r.shardID)
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

func (r *scanShardRunner) refreshIterator(ctx context.Context, lastSeqNum string, getRecordsErr error, attempt int) (*string, string, int, error) {
	r.consumer.logger.WarnContext(ctx, "get records error", slog.String("shard-id", r.shardID), slog.String("error", getRecordsErr.Error()))

	if ctx.Err() != nil {
		return nil, lastSeqNum, attempt, nil
	}

	if !isRetriableError(getRecordsErr) {
		return nil, lastSeqNum, attempt, fmt.Errorf("get records error: %w", getRecordsErr)
	}

	if !r.waitForRetry(ctx, getRecordsErr, attempt, "get records") {
		return nil, lastSeqNum, attempt, nil
	}

	for {
		shardIterator, nextSeqNum, err := r.loadIterator(ctx, lastSeqNum)
		if err == nil {
			return shardIterator, nextSeqNum, attempt, nil
		}
		if !isRetriableError(err) {
			return nil, lastSeqNum, attempt, err
		}

		attempt++
		r.consumer.logger.WarnContext(ctx, "get shard iterator retry", slog.String("shard-id", r.shardID), slog.Int("attempt", attempt), slog.String("error", err.Error()))
		if !r.waitForRetry(ctx, err, attempt, "get shard iterator") {
			return nil, lastSeqNum, attempt, nil
		}
	}
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
		if err := r.handleShardClosed(ctx); err != nil {
			return nil, lastSeqNum, err
		}
		return nil, lastSeqNum, nil
	}

	return resp.NextShardIterator, lastSeqNum, nil
}

func (r *scanShardRunner) handleShardClosed(ctx context.Context) error {
	r.consumer.logger.DebugContext(ctx, "shard closed", slog.String("shard-id", r.shardID))

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

func (r *scanShardRunner) waitForRetry(ctx context.Context, err error, attempt int, operation string) bool {
	delay := retryDelay(err, attempt)
	if delay <= 0 {
		return ctx.Err() == nil
	}

	r.consumer.logger.WarnContext(ctx, "retry backoff", slog.String("operation", operation), slog.String("shard-id", r.shardID), slog.Int("attempt", attempt), slog.Duration("delay", delay))
	return r.consumer.retryWait(ctx, delay)
}
