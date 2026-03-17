package worker

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blake-golliher/s3lister/internal/store"
)

const batchSize = 5000

// WriterPool reads ObjectRecords from a channel and batch-writes them to Pebble.
type WriterPool struct {
	store   *store.Store
	inCh    <-chan store.ObjectRecord
	workers int
	logger  *log.Logger
	errors  atomic.Int64
}

func NewWriterPool(s *store.Store, in <-chan store.ObjectRecord, workers int, logger *log.Logger) *WriterPool {
	return &WriterPool{
		store:   s,
		inCh:    in,
		workers: workers,
		logger:  logger,
	}
}

func (wp *WriterPool) Errors() int64 {
	return wp.errors.Load()
}

// Run starts writer goroutines. They block on inCh and exit when it closes.
func (wp *WriterPool) Run() {
	start := time.Now()
	wp.logger.Printf("[writer-pool] starting %d writers (batch_size=%d)", wp.workers, batchSize)

	var wg sync.WaitGroup
	for i := 0; i < wp.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			wp.worker(id)
		}(i)
	}

	wg.Wait()
	wp.logger.Printf("[writer-pool] done: %d written, %d errors in %v",
		wp.store.Written(), wp.errors.Load(), time.Since(start))
}

func (wp *WriterPool) worker(id int) {
	workerStart := time.Now()
	batch := make([]store.ObjectRecord, 0, batchSize)
	var totalWritten, totalBatches int64

	flush := func() {
		if len(batch) == 0 {
			return
		}
		batchStart := time.Now()
		if err := wp.store.WriteBatch(batch); err != nil {
			wp.errors.Add(1)
			wp.logger.Printf("[writer-%d] ERROR batch write failed (%d records): %v", id, len(batch), err)
			return
		}
		totalWritten += int64(len(batch))
		totalBatches++
		if elapsed := time.Since(batchStart); elapsed > 100*time.Millisecond {
			wp.logger.Printf("[writer-%d] slow batch: %d records in %v", id, len(batch), elapsed)
		}
		batch = batch[:0]
	}

	for rec := range wp.inCh {
		batch = append(batch, rec)
		if len(batch) >= batchSize {
			flush()
		}
	}
	flush()

	wp.logger.Printf("[writer-%d] done: %d records, %d batches in %v",
		id, totalWritten, totalBatches, time.Since(workerStart))
}
