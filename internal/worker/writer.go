package worker

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blake-golliher/s3lister/internal/model"
	"github.com/blake-golliher/s3lister/internal/pq"
)

// WriterPool consumes record batches from a channel and writes them to Parquet.
// Each worker owns its own part-NNN.parquet file, so writers never contend on a
// shared handle — the pool scales linearly with cores until zstd or disk IO
// saturates.
type WriterPool struct {
	dir     string
	scanID  string
	scanTS  time.Time
	inCh    <-chan []model.ObjectRecord
	workers int
	logger  *log.Logger
	written atomic.Int64
	errors  atomic.Int64
}

func NewWriterPool(dir, scanID string, scanTS time.Time, in <-chan []model.ObjectRecord, workers int, logger *log.Logger) *WriterPool {
	return &WriterPool{
		dir:     dir,
		scanID:  scanID,
		scanTS:  scanTS,
		inCh:    in,
		workers: workers,
		logger:  logger,
	}
}

// Written returns the total records written across all writers so far.
func (wp *WriterPool) Written() int64 { return wp.written.Load() }

// Errors returns the number of write errors encountered.
func (wp *WriterPool) Errors() int64 { return wp.errors.Load() }

// Run starts writer goroutines. They block on inCh and exit when it closes,
// flushing their Parquet footers on the way out.
func (wp *WriterPool) Run() {
	start := time.Now()
	wp.logger.Printf("[writer-pool] starting %d writers -> %s (zstd, %d rows/group)",
		wp.workers, wp.dir, 1_000_000)

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
		wp.written.Load(), wp.errors.Load(), time.Since(start))
}

func (wp *WriterPool) worker(id int) {
	workerStart := time.Now()
	name := fmt.Sprintf("part-%03d.parquet", id)
	path := filepath.Join(wp.dir, name)

	w, err := pq.NewWriter(path, wp.scanID, wp.scanTS)
	if err != nil {
		wp.errors.Add(1)
		wp.logger.Printf("[writer-%d] ERROR creating %s: %v", id, path, err)
		// Keep draining so readers don't deadlock on a full channel.
		for range wp.inCh {
		}
		return
	}

	var count int64
	for batch := range wp.inCh {
		n, err := w.Append(batch)
		if err != nil {
			wp.errors.Add(1)
			wp.logger.Printf("[writer-%d] ERROR append (%d records): %v", id, len(batch), err)
		}
		if n > 0 {
			count += int64(n)
			wp.written.Add(int64(n))
		}
	}

	if err := w.Close(); err != nil {
		wp.errors.Add(1)
		wp.logger.Printf("[writer-%d] ERROR closing %s: %v", id, name, err)
	}

	wp.logger.Printf("[writer-%d] done: %d records -> %s in %v",
		id, count, name, time.Since(workerStart))
}
