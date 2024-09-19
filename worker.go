package consumer

import (
	"context"
	"fmt"
)

// Result is the output of the worker. It contains the ID of the worker that processed it, the record itself (mainly to
// maintain the offset that the record has and the error of processing to propagate up.
type Result struct {
	Record
	WorkerName string
	Err        error
}

// WorkerPool allows to parallel process records
type WorkerPool struct {
	name       string
	numWorkers int
	fn         ScanFunc
	recordC    chan Record
	resultC    chan Result
}

// NewWorkerPool returns an instance of WorkerPool
func NewWorkerPool(name string, numWorkers int, fn ScanFunc) *WorkerPool {
	return &WorkerPool{
		name:       fmt.Sprintf("wp-%s", name),
		numWorkers: numWorkers,
		fn:         fn,
		recordC:    make(chan Record, 1),
		resultC:    make(chan Result, 1),
	}
}

// Start spawns the amount of workers specified in numWorkers and starts them.
func (wp *WorkerPool) Start(ctx context.Context) {
	// How do I reopen workers if one fails?
	for i := range wp.numWorkers {
		name := fmt.Sprintf("%s-worker-%d", wp.name, i)
		w := newWorker(name, wp.fn, wp.recordC, wp.resultC)
		w.start(ctx)
	}
}

// Stop stops the WorkerPool by closing the channels used for processing.
func (wp *WorkerPool) Stop() {
	close(wp.recordC)
	close(wp.resultC)
}

// Submit a new Record for processing
func (wp *WorkerPool) Submit(r Record) {
	wp.recordC <- r
}

// Result returns the Result of the Submit-ed Record after it has been processed.
func (wp *WorkerPool) Result() *Result {
	select {
	case r := <-wp.resultC:
		return &r
	default:
		return nil
	}
}

type worker struct {
	name    string
	fn      ScanFunc
	recordC chan Record
	resultC chan Result
}

func newWorker(name string, fn ScanFunc, recordC chan Record, resultC chan Result) *worker {
	return &worker{
		name:    name,
		fn:      fn,
		recordC: recordC,
		resultC: resultC,
	}
}

func (w *worker) start(ctx context.Context) {
	go func(ctx context.Context) {
		for r := range w.recordC {
			select {
			case <-ctx.Done():
				return
			default:
				err := w.fn(&r)
				res := Result{
					Record:     r,
					WorkerName: w.name,
					Err:        err,
				}

				w.resultC <- res
			}
		}
	}(ctx)
}
