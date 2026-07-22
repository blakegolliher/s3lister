package worker

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blake-golliher/s3lister/internal/model"
	"github.com/blake-golliher/s3lister/internal/s3client"
)

const (
	maxSplitDepth   = 8 // generous depth limit for prefix splitting
	stealBackoffMin = 500 * time.Microsecond
	stealBackoffMax = 50 * time.Millisecond
	delimiter       = "/"
	// How many range chunks to split a large flat prefix into.
	rangeSplitFactor = 8
	// Per-page retry policy layered on top of the SDK's own retries. A VIP
	// failover resets every connection at once and drains the SDK's retry
	// budget; without this layer one transient event silently truncates every
	// work item it touches.
	listAttempts   = 6
	listBackoffMin = 200 * time.Millisecond
	listBackoffMax = 5 * time.Second
)

// ReaderPool fans out S3 listing with true work-stealing.
//
// Emission invariant: every object is emitted exactly once. Each WorkItem
// covers a disjoint slice of the keyspace — either "the objects directly at
// one prefix level" (delimiter listing; sub-prefixes become new WorkItems) or
// "a bounded key range under one prefix" (range listing). No code path lists
// the same slice twice.
type ReaderPool struct {
	client   *s3client.FastClient
	bucket   string
	prefix   string
	workers  int
	pageSize int
	outCh   chan<- []model.ObjectRecord
	listed  atomic.Int64
	logger  *log.Logger
	deques  []*Deque
	working atomic.Int32 // workers currently processing an item

	// listErrors counts work items abandoned after all page retries failed.
	// Any non-zero value means the scan is INCOMPLETE: some slice of the
	// keyspace was never listed. Callers must surface this loudly.
	listErrors atomic.Int64
}

// ListErrors returns the number of work items abandoned due to persistent
// listing failures. Non-zero means the output is missing part of the bucket.
func (rp *ReaderPool) ListErrors() int64 {
	return rp.listErrors.Load()
}

// listPageWithRetry fetches one page for the query, retrying with exponential
// jittered backoff. The query's continuation state is owned by the caller and
// only advances on success, so a retry re-requests the same page.
func (rp *ReaderPool) listPageWithRetry(ctx context.Context, q *s3client.ListQuery, page *s3client.ListPage) error {
	backoff := listBackoffMin
	var err error
	for attempt := 0; attempt < listAttempts; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff + jitter):
			}
			if backoff < listBackoffMax {
				backoff *= 2
			}
		}
		if err = rp.client.ListPage(ctx, rp.bucket, q, page); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
	}
	return err
}

func NewReaderPool(client *s3client.FastClient, bucket, prefix string, workers, pageSize int, out chan<- []model.ObjectRecord, logger *log.Logger) *ReaderPool {
	deques := make([]*Deque, workers)
	for i := range deques {
		deques[i] = NewDeque()
	}
	if pageSize <= 0 {
		pageSize = 1000
	}
	return &ReaderPool{
		client:   client,
		bucket:   bucket,
		prefix:   prefix,
		workers:  workers,
		pageSize: pageSize,
		outCh:    out,
		logger:   logger,
		deques:   deques,
	}
}

func (rp *ReaderPool) Listed() int64 {
	return rp.listed.Load()
}

func (rp *ReaderPool) Run(ctx context.Context) error {
	start := time.Now()
	rp.logger.Printf("[reader-pool] starting bucket=%s prefix=%q workers=%d",
		rp.bucket, rp.prefix, rp.workers)

	// Seed with the root prefix. The first worker to process it pushes the
	// top-level sub-prefixes, which idle workers immediately steal — the pool
	// reaches full parallelism within a couple of listing calls.
	rp.deques[0].PushFront(WorkItem{Prefix: rp.prefix, Depth: 0})

	var wg sync.WaitGroup
	for i := 0; i < rp.workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rp.worker(ctx, id)
		}(i)
	}

	wg.Wait()
	rp.logger.Printf("[reader-pool] done: %d objects in %v", rp.listed.Load(), time.Since(start))
	return nil
}

func (rp *ReaderPool) worker(ctx context.Context, id int) {
	workerStart := time.Now()
	myDeque := rp.deques[id]
	var count int64
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))
	// One reusable page per worker: response-body buffer and object slices
	// are recycled across every page this worker fetches.
	page := &s3client.ListPage{}

	defer func() {
		rp.logger.Printf("[reader-%d] done: %d objects in %v", id, count, time.Since(workerStart))
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		item, ok := myDeque.PopFront()
		if ok {
			// working must cover the whole processing window: new work items
			// are pushed while processing, so peers may not conclude the scan
			// is drained until this item is fully handled.
			rp.working.Add(1)
			count += rp.processItem(ctx, id, myDeque, item, page)
			rp.working.Add(-1)
			continue
		}

		if rp.trySteal(id, myDeque, rng) {
			continue
		}

		if rp.spinWaitForWork(ctx, id, myDeque, rng) {
			continue
		}
		return
	}
}

func (rp *ReaderPool) trySteal(myID int, myDeque *Deque, rng *rand.Rand) bool {
	if rp.workers <= 1 {
		return false
	}
	order := rng.Perm(rp.workers)
	for _, victim := range order {
		if victim == myID {
			continue
		}
		stolen, ok := rp.deques[victim].StealBack()
		if ok {
			myDeque.PushBatch(stolen)
			return true
		}
	}
	return false
}

// spinWaitForWork waits for new work to appear. It returns false — letting
// the worker exit — only when the scan is provably drained: no worker is
// processing an item (so nothing new can be pushed) and every deque is empty.
func (rp *ReaderPool) spinWaitForWork(ctx context.Context, id int, myDeque *Deque, rng *rand.Rand) bool {
	backoff := stealBackoffMin

	for {
		if ctx.Err() != nil {
			return false
		}
		if rp.working.Load() == 0 && rp.allDequesEmpty() {
			return false
		}
		time.Sleep(backoff)
		if backoff < stealBackoffMax {
			backoff *= 2
		}

		if myDeque.Len() > 0 {
			return true
		}
		if rp.trySteal(id, myDeque, rng) {
			return true
		}
	}
}

func (rp *ReaderPool) allDequesEmpty() bool {
	for _, d := range rp.deques {
		if d.Len() > 0 {
			return false
		}
	}
	return true
}

func (rp *ReaderPool) processItem(ctx context.Context, id int, myDeque *Deque, item WorkItem, page *s3client.ListPage) int64 {
	// Range-bounded work items are bounded flat listings.
	if item.StartAfter != "" || item.EndAt != "" {
		return rp.listRange(ctx, id, item, page)
	}
	return rp.listLevel(ctx, id, myDeque, item, page)
}

// listLevel lists one prefix level with a delimiter: objects directly at this
// level are emitted (exactly once), and each sub-prefix is pushed as a new
// work item. If the first page shows a large flat prefix — truncated with no
// sub-prefixes — the remainder is carved into range chunks that idle workers
// steal, instead of one worker paginating it alone.
func (rp *ReaderPool) listLevel(ctx context.Context, id int, myDeque *Deque, item WorkItem, page *s3client.ListPage) int64 {
	start := time.Now()
	var count int64

	q := s3client.ListQuery{
		Prefix:  item.Prefix,
		MaxKeys: rp.pageSize,
	}
	// Beyond the split-depth limit, list the whole subtree flat rather than
	// recursing further; the flat path below still range-splits if it's huge.
	if item.Depth < maxSplitDepth {
		q.Delimiter = delimiter
	}

	var prefixes []string
	firstPage := true
	for {
		if ctx.Err() != nil {
			return count
		}
		if err := rp.listPageWithRetry(ctx, &q, page); err != nil {
			if ctx.Err() == nil {
				rp.listErrors.Add(1)
				rp.logger.Printf("[reader-%d] ABANDONED prefix=%q after %d attempts (scan incomplete): %v",
					id, item.Prefix, listAttempts, err)
			}
			return count
		}

		recs := toRecords(page.Objects)
		rp.emitBatch(recs)
		count += int64(len(recs))

		prefixes = append(prefixes, page.CommonPrefixes...)

		// Large flat prefix: no sub-prefixes on a full first page. Carve the
		// tail into disjoint (StartAfter, EndAt] chunks and stop; the
		// chunks cover everything after this page exactly once.
		if firstPage && len(prefixes) == 0 && page.IsTruncated && len(page.Objects) > 0 {
			lastKey := page.Objects[len(page.Objects)-1].Key
			if rangeItems := rp.buildRangeItems(ctx, item.Prefix, lastKey); len(rangeItems) > 1 {
				myDeque.PushBatch(rangeItems)
				rp.logger.Printf("[reader-%d] range-split %q into %d chunks after %q",
					id, item.Prefix, len(rangeItems), lastKey)
				return count
			}
			// Sampling found too few keys to be worth splitting — keep
			// paginating sequentially.
		}
		firstPage = false

		if !page.IsTruncated {
			break
		}
		q.ContinuationToken = page.NextToken
	}

	if len(prefixes) > 0 {
		myDeque.PushBatch(prefixesToWorkItems(prefixes, item.Depth+1))
		rp.logger.Printf("[reader-%d] split %q into %d sub-prefixes (depth=%d)",
			id, item.Prefix, len(prefixes), item.Depth+1)
	}

	if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
		rp.logger.Printf("[reader-%d] prefix=%q %d objects in %v", id, item.Prefix, count, elapsed)
	}
	return count
}

// buildRangeItems samples marker keys beyond startAfter and turns them into
// disjoint range work items that together cover (startAfter, end-of-prefix).
func (rp *ReaderPool) buildRangeItems(ctx context.Context, prefix, startAfter string) []WorkItem {
	markers := rp.sampleRangeMarkers(ctx, prefix, startAfter, rangeSplitFactor)
	if len(markers) == 0 {
		return nil
	}

	var items []WorkItem
	prev := startAfter
	for _, m := range markers {
		if m <= prev {
			continue
		}
		items = append(items, WorkItem{
			Prefix:     prefix,
			StartAfter: prev,
			EndAt:      m,
		})
		prev = m
	}
	// Final open-ended chunk.
	items = append(items, WorkItem{
		Prefix:     prefix,
		StartAfter: prev,
	})
	return items
}

// sampleRangeMarkers probes up to 10 pages beyond startAfter and picks
// evenly-spaced keys to use as range boundaries. The sampled pages are only
// read for their key names; the range workers do the actual emission.
func (rp *ReaderPool) sampleRangeMarkers(ctx context.Context, prefix, startAfter string, n int) []string {
	q := s3client.ListQuery{
		Prefix:     prefix,
		StartAfter: startAfter,
		MaxKeys:    rp.pageSize,
	}
	// Sampling runs while the caller's page still holds live data, so it
	// gets its own scratch page. It's a rare path — one per range split.
	page := &s3client.ListPage{}

	var allKeys []string
	for pages := 0; pages < 10; pages++ {
		// A sampling failure is not a data-loss path — the caller falls back
		// to sequential pagination — so no listErrors here.
		if err := rp.listPageWithRetry(ctx, &q, page); err != nil {
			return nil
		}
		for i := range page.Objects {
			allKeys = append(allKeys, page.Objects[i].Key)
		}
		if !page.IsTruncated {
			break
		}
		q.ContinuationToken = page.NextToken
	}

	if len(allKeys) < n*2 {
		// Not enough keys to make range splitting worthwhile
		return nil
	}

	// Pick evenly-spaced markers
	step := len(allKeys) / n
	markers := make([]string, 0, n-1)
	for i := step; i < len(allKeys); i += step {
		markers = append(markers, allKeys[i])
		if len(markers) >= n-1 {
			break
		}
	}
	return markers
}

// listRange lists objects in a bounded key range under a prefix.
// Lists keys strictly after StartAfter, up to and including EndAt. Emitting
// the boundary key here (and starting the next chunk strictly after it via
// StartAfter) is what makes adjacent chunks partition the keyspace exactly.
func (rp *ReaderPool) listRange(ctx context.Context, id int, item WorkItem, page *s3client.ListPage) int64 {
	start := time.Now()
	var count int64

	q := s3client.ListQuery{
		Prefix:     item.Prefix,
		StartAfter: item.StartAfter,
		MaxKeys:    rp.pageSize,
	}

	for {
		if ctx.Err() != nil {
			return count
		}
		if err := rp.listPageWithRetry(ctx, &q, page); err != nil {
			if ctx.Err() == nil {
				rp.listErrors.Add(1)
				rp.logger.Printf("[reader-%d] ABANDONED range prefix=%q after=%q after %d attempts (scan incomplete): %v",
					id, item.Prefix, item.StartAfter, listAttempts, err)
			}
			return count
		}
		recs := make([]model.ObjectRecord, 0, len(page.Objects))
		for i := range page.Objects {
			key := page.Objects[i].Key
			// Stop at the end boundary, emitting the boundary key itself.
			if item.EndAt != "" && key >= item.EndAt {
				if key == item.EndAt {
					recs = append(recs, toRecord(&page.Objects[i]))
				}
				rp.emitBatch(recs)
				count += int64(len(recs))
				if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
					rp.logger.Printf("[reader-%d] range prefix=%q %d objects in %v",
						id, item.Prefix, count, elapsed)
				}
				return count
			}
			recs = append(recs, toRecord(&page.Objects[i]))
		}
		rp.emitBatch(recs)
		count += int64(len(recs))

		if !page.IsTruncated {
			break
		}
		q.ContinuationToken = page.NextToken
	}

	if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
		rp.logger.Printf("[reader-%d] range prefix=%q %d objects in %v",
			id, item.Prefix, count, elapsed)
	}
	return count
}

func prefixesToWorkItems(prefixes []string, depth int) []WorkItem {
	items := make([]WorkItem, len(prefixes))
	for i, p := range prefixes {
		items[i] = WorkItem{Prefix: p, Depth: depth}
	}
	return items
}

// emitBatch sends a whole page of records downstream in one channel operation
// and accounts for them with a single atomic add. Sending per-page instead of
// per-object cuts channel traffic and atomic contention by ~1000x at scale.
func (rp *ReaderPool) emitBatch(recs []model.ObjectRecord) {
	if len(recs) == 0 {
		return
	}
	rp.outCh <- recs
	rp.listed.Add(int64(len(recs)))
}

// toRecord converts a parsed listing entry into a record. The strings were
// allocated by the parser and are shared, not copied.
func toRecord(obj *s3client.ListedObject) model.ObjectRecord {
	return model.ObjectRecord{
		Key:          obj.Key,
		Size:         obj.Size,
		LastModified: obj.LastModified,
		ETag:         obj.ETag,
		StorageClass: obj.StorageClass,
	}
}

// toRecords converts a page of listing entries into a freshly allocated
// record slice (the records outlive the reused page).
func toRecords(objs []s3client.ListedObject) []model.ObjectRecord {
	recs := make([]model.ObjectRecord, len(objs))
	for i := range objs {
		recs[i] = toRecord(&objs[i])
	}
	return recs
}
