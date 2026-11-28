package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type scanBatchRunner struct {
	consumer *Consumer
	fn       ScanBatchFunc
	cfg      scanBatchConfig

	// Serialize callbacks and checkpoint writes across shards. Keep the first
	// failure so another pending flush cannot advance past a failed batch.
	mu  sync.Mutex
	err error
}

func newScanBatchRunner(consumer *Consumer, fn ScanBatchFunc, cfg scanBatchConfig) *scanBatchRunner {
	return &scanBatchRunner{consumer: consumer, fn: fn, cfg: cfg}
}

func (r *scanBatchRunner) run(ctx context.Context) error {
	return r.consumer.scan(ctx, func(shardID string, reportError func(error)) shardProcessor {
		return &shardBatch{runner: r, shardID: shardID, reportError: reportError}
	})
}

type shardBatch struct {
	runner      *scanBatchRunner
	shardID     string
	reportError func(error)

	// Hold mu from buffer mutation through the checkpoint so a timer, size
	// flush and shard shutdown can never overtake each other.
	mu      sync.Mutex
	records []*Record
	timer   *time.Timer
}

func (b *shardBatch) process(_ context.Context, record *Record) (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.records = append(b.records, record)
	if len(b.records) >= b.runner.cfg.maxSize {
		return true, b.flushLocked()
	}
	if len(b.records) == 1 && b.runner.cfg.flushInterval > 0 {
		var timer *time.Timer
		timer = time.AfterFunc(b.runner.cfg.flushInterval, func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			// A size or final flush can retire this timer while it waits for mu.
			if b.timer == timer {
				if err := b.flushLocked(); err != nil {
					b.reportError(fmt.Errorf("shard %s batch flush error: %w", b.shardID, err))
				}
			}
		})
		b.timer = timer
	}
	return true, nil
}

func (b *shardBatch) finish(scanErr error) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if scanErr != nil {
		b.stopTimer()
		return scanErr
	}
	return b.flushLocked()
}

func (b *shardBatch) stopTimer() {
	if b.timer != nil {
		b.timer.Stop()
		b.timer = nil
	}
}

func (b *shardBatch) flushLocked() error {
	b.stopTimer()
	r := b.runner
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil || len(b.records) == 0 {
		return r.err
	}

	batch := b.records
	b.records = nil
	seq := aws.ToString(batch[len(batch)-1].SequenceNumber)
	r.err = r.fn(batch)
	if r.err == nil {
		// Complete the bounded checkpoint retries even during lease handoff.
		r.err = r.consumer.setCheckpointWithRetry(context.Background(), b.shardID, seq)
	}
	return r.err
}
