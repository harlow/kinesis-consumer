package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type scanBatchRunner struct {
	consumer *Consumer
	fn       ScanBatchFunc
	cfg      scanBatchConfig

	buffers *scanBatchBuffers

	flushMu sync.Mutex

	asyncErrMu sync.Mutex
	asyncErr   error
}

func newScanBatchRunner(consumer *Consumer, fn ScanBatchFunc, cfg scanBatchConfig) *scanBatchRunner {
	return &scanBatchRunner{
		consumer: consumer,
		fn:       fn,
		cfg:      cfg,
		buffers:  newScanBatchBuffers(),
	}
}

func (r *scanBatchRunner) run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var tickerWG sync.WaitGroup
	if r.cfg.flushInterval > 0 {
		tickerWG.Add(1)
		go func() {
			defer tickerWG.Done()
			r.runFlushTicker(ctx, cancel)
		}()
	}

	scanErr := r.consumer.Scan(ctx, func(record *Record) error {
		shardID, batch := r.buffers.addAndMaybeDrain(record, r.cfg.maxSize)
		if len(batch) > 0 {
			if err := r.flush(ctx, map[string][]*Record{shardID: batch}); err != nil {
				return err
			}
		}
		return ErrSkipCheckpoint
	})

	cancel()
	tickerWG.Wait()

	if err := r.getAsyncErr(); err != nil {
		return err
	}
	if scanErr != nil && !errors.Is(scanErr, context.Canceled) {
		return scanErr
	}

	if err := r.flush(context.Background(), r.buffers.drainAll()); err != nil {
		return err
	}
	if err := r.consumer.flushCheckpoints(); err != nil {
		return fmt.Errorf("checkpoint flush error: %w", err)
	}
	if err := r.getAsyncErr(); err != nil {
		return err
	}

	return scanErr
}

func (r *scanBatchRunner) runFlushTicker(ctx context.Context, cancel context.CancelFunc) {
	ticker := time.NewTicker(r.cfg.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := r.flush(ctx, r.buffers.drainAll()); err != nil {
				r.setAsyncErr(fmt.Errorf("batch flush error: %w", err))
				cancel()
				return
			}
		}
	}
}

func (r *scanBatchRunner) flush(ctx context.Context, batches map[string][]*Record) error {
	if len(batches) == 0 {
		return nil
	}

	r.flushMu.Lock()
	defer r.flushMu.Unlock()

	for shardID, batch := range batches {
		if len(batch) == 0 {
			continue
		}
		if err := r.fn(batch); err != nil {
			return err
		}
		last := batch[len(batch)-1]
		if err := r.consumer.setCheckpointWithRetry(ctx, shardID, aws.ToString(last.SequenceNumber)); err != nil {
			return err
		}
	}

	return nil
}

func (r *scanBatchRunner) setAsyncErr(err error) {
	if err == nil {
		return
	}

	r.asyncErrMu.Lock()
	defer r.asyncErrMu.Unlock()

	if r.asyncErr == nil {
		r.asyncErr = err
	}
}

func (r *scanBatchRunner) getAsyncErr() error {
	r.asyncErrMu.Lock()
	defer r.asyncErrMu.Unlock()

	return r.asyncErr
}

type scanBatchBuffers struct {
	mu      sync.Mutex
	byShard map[string][]*Record
}

func newScanBatchBuffers() *scanBatchBuffers {
	return &scanBatchBuffers{byShard: make(map[string][]*Record)}
}

func (b *scanBatchBuffers) addAndMaybeDrain(r *Record, maxSize int) (string, []*Record) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.byShard[r.ShardID] = append(b.byShard[r.ShardID], r)
	if len(b.byShard[r.ShardID]) < maxSize {
		return "", nil
	}

	shardID := r.ShardID
	batch := b.byShard[shardID]
	delete(b.byShard, shardID)
	return shardID, batch
}

func (b *scanBatchBuffers) drainAll() map[string][]*Record {
	b.mu.Lock()
	defer b.mu.Unlock()

	out := make(map[string][]*Record, len(b.byShard))
	for shardID, batch := range b.byShard {
		if len(batch) == 0 {
			continue
		}
		out[shardID] = batch
	}
	b.byShard = make(map[string][]*Record)
	return out
}
