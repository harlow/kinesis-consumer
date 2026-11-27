package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// scanBatchRunner keeps one buffer for each shard. Only the goroutine of that
// shard adds records, flushes the buffer and sets the checkpoint, so the
// batches of a shard are processed and checkpointed in sequence order.
type scanBatchRunner struct {
	consumer *Consumer
	fn       ScanBatchFunc
	cfg      scanBatchConfig
	now      func() time.Time

	// fnMu keeps the callback calls of all shards serialized.
	fnMu sync.Mutex
}

func newScanBatchRunner(consumer *Consumer, fn ScanBatchFunc, cfg scanBatchConfig) *scanBatchRunner {
	return &scanBatchRunner{
		consumer: consumer,
		fn:       fn,
		cfg:      cfg,
		now:      time.Now,
	}
}

func (r *scanBatchRunner) run(ctx context.Context) error {
	return r.consumer.scan(ctx, func(shardCtx context.Context, shardID string) shardProcessor {
		return &shardBatch{runner: r, ctx: shardCtx, shardID: shardID}
	})
}

// shardBatch is the buffer of one shard.
type shardBatch struct {
	runner  *scanBatchRunner
	ctx     context.Context
	shardID string

	records   []*Record
	firstSeen time.Time
}

func (b *shardBatch) process(record *Record) error {
	if len(b.records) == 0 {
		b.firstSeen = b.runner.now()
	}
	b.records = append(b.records, record)
	if len(b.records) >= b.runner.cfg.maxSize {
		if err := b.flush(b.ctx); err != nil {
			return err
		}
	}
	return errRecordBuffered
}

func (b *shardBatch) beforePoll(ctx context.Context) error {
	interval := b.runner.cfg.flushInterval
	if interval <= 0 || len(b.records) == 0 || b.runner.now().Sub(b.firstSeen) < interval {
		return nil
	}
	if err := b.flush(ctx); err != nil {
		return fmt.Errorf("batch flush error: %w", err)
	}
	return nil
}

// finish flushes the records that are left when the shard scan stops. The
// shard context can be cancelled at this point, so it uses a new context.
func (b *shardBatch) finish() error {
	return b.flush(context.Background())
}

func (b *shardBatch) flush(ctx context.Context) error {
	if len(b.records) == 0 {
		return nil
	}
	batch := b.records

	b.runner.fnMu.Lock()
	err := b.runner.fn(batch)
	b.runner.fnMu.Unlock()
	if err != nil {
		return err
	}

	b.records = nil
	last := batch[len(batch)-1]
	return b.runner.consumer.setCheckpointWithRetry(ctx, b.shardID, aws.ToString(last.SequenceNumber))
}
