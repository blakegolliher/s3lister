package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/blake-golliher/s3lister/internal/store"
)

const (
	maxSplitDepth    = 8             // generous depth limit for prefix splitting
	stealBackoffMin  = 500 * time.Microsecond
	stealBackoffMax  = 50 * time.Millisecond
	stealGiveUpAfter = 5 * time.Second
	delimiter        = "/"
	// If a flat listing returns this many objects on the first page and has
	// more pages, we probe for range-split markers to parallelize it.
	rangeSplitThreshold = 5000
	// How many range chunks to split a large flat prefix into.
	rangeSplitFactor = 8
)

// ReaderPool fans out S3 listing with true work-stealing.
type ReaderPool struct {
	client  *s3.Client
	bucket  string
	prefix  string
	workers int
	outCh   chan<- store.ObjectRecord
	listed  atomic.Int64
	logger  *log.Logger
	deques  []*Deque
	active  atomic.Int32
}

func NewReaderPool(client *s3.Client, bucket, prefix string, workers int, out chan<- store.ObjectRecord, logger *log.Logger) *ReaderPool {
	deques := make([]*Deque, workers)
	for i := range deques {
		deques[i] = NewDeque()
	}
	return &ReaderPool{
		client:  client,
		bucket:  bucket,
		prefix:  prefix,
		workers: workers,
		outCh:   out,
		logger:  logger,
		deques:  deques,
	}
}

func (rp *ReaderPool) Listed() int64 {
	return rp.listed.Load()
}

func (rp *ReaderPool) Run(ctx context.Context) error {
	start := time.Now()
	rp.logger.Printf("[reader-pool] starting prefix discovery bucket=%s prefix=%s", rp.bucket, rp.prefix)

	seeds, err := rp.discoverPrefixes(ctx)
	if err != nil {
		return fmt.Errorf("prefix discovery failed: %w", err)
	}
	if len(seeds) == 0 {
		seeds = []WorkItem{{Prefix: rp.prefix, Depth: 0}}
	}

	rp.logger.Printf("[reader-pool] %d seed prefixes in %v, starting %d workers",
		len(seeds), time.Since(start), rp.workers)

	for i, seed := range seeds {
		rp.deques[i%rp.workers].PushFront(seed)
	}

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

// listPrefixes does a single delimiter-based listing, returning sub-prefixes
// and emitting any objects found at this level.
func (rp *ReaderPool) listPrefixes(ctx context.Context, prefix string) ([]string, error) {
	var prefixes []string
	paginator := s3.NewListObjectsV2Paginator(rp.client, &s3.ListObjectsV2Input{
		Bucket:    aws.String(rp.bucket),
		Prefix:    aws.String(prefix),
		Delimiter: aws.String(delimiter),
	})
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, cp := range page.CommonPrefixes {
			if cp.Prefix != nil {
				prefixes = append(prefixes, *cp.Prefix)
			}
		}
		for i := range page.Contents {
			rp.emit(&page.Contents[i])
		}
	}
	return prefixes, nil
}

func (rp *ReaderPool) discoverPrefixes(ctx context.Context) ([]WorkItem, error) {
	prefixes, err := rp.listPrefixes(ctx, rp.prefix)
	if err != nil {
		return nil, err
	}

	items := prefixesToWorkItems(prefixes, 0)

	if len(items) > 0 && len(items) < rp.workers*2 {
		expanded := rp.expandSeeds(ctx, items)
		if len(expanded) > len(items) {
			items = expanded
		}
	}
	return items, nil
}

func (rp *ReaderPool) expandSeeds(ctx context.Context, seeds []WorkItem) []WorkItem {
	var mu sync.Mutex
	var result []WorkItem
	var wg sync.WaitGroup

	for _, seed := range seeds {
		wg.Add(1)
		go func(s WorkItem) {
			defer wg.Done()
			prefixes, err := rp.listPrefixes(ctx, s.Prefix)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				rp.logger.Printf("[reader-pool] expand error prefix=%q: %v", s.Prefix, err)
				result = append(result, s)
			} else if len(prefixes) == 0 {
				result = append(result, s)
			} else {
				result = append(result, prefixesToWorkItems(prefixes, s.Depth+1)...)
			}
		}(seed)
	}
	wg.Wait()
	return result
}

func prefixesToWorkItems(prefixes []string, depth int) []WorkItem {
	items := make([]WorkItem, len(prefixes))
	for i, p := range prefixes {
		items[i] = WorkItem{Prefix: p, Depth: depth}
	}
	return items
}

func (rp *ReaderPool) worker(ctx context.Context, id int) {
	workerStart := time.Now()
	myDeque := rp.deques[id]
	var count int64
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)))

	rp.active.Add(1)
	defer func() {
		rp.active.Add(-1)
		rp.logger.Printf("[reader-%d] done: %d objects in %v", id, count, time.Since(workerStart))
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		item, ok := myDeque.PopFront()
		if ok {
			count += rp.processItem(ctx, id, myDeque, item)
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

func (rp *ReaderPool) spinWaitForWork(ctx context.Context, id int, myDeque *Deque, rng *rand.Rand) bool {
	backoff := stealBackoffMin
	deadline := time.Now().Add(stealGiveUpAfter)

	for time.Now().Before(deadline) {
		if ctx.Err() != nil {
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
		if rp.active.Load() <= 1 && rp.allDequesEmpty() {
			return false
		}
	}
	return false
}

func (rp *ReaderPool) allDequesEmpty() bool {
	for _, d := range rp.deques {
		if d.Len() > 0 {
			return false
		}
	}
	return true
}

func (rp *ReaderPool) processItem(ctx context.Context, id int, myDeque *Deque, item WorkItem) int64 {
	// If this is a range-bounded work item, list it directly
	if item.StartAfter != "" || item.EndBefore != "" {
		return rp.listRange(ctx, id, item)
	}

	// Try to split into sub-prefixes for parallelism
	if item.Depth < maxSplitDepth {
		prefixes, err := rp.listPrefixes(ctx, item.Prefix)
		if err == nil && len(prefixes) > 1 {
			myDeque.PushBatch(prefixesToWorkItems(prefixes, item.Depth+1))
			rp.logger.Printf("[reader-%d] split %q into %d sub-prefixes (depth=%d)",
				id, item.Prefix, len(prefixes), item.Depth+1)
			return 0
		}
	}

	// Flat listing — but try range-splitting if it's huge
	return rp.listFlatOrSplit(ctx, id, myDeque, item)
}

// listFlatOrSplit lists the first page. If there are more pages (indicating a
// large flat prefix), it samples evenly-spaced keys to create range-bounded
// work items that other workers can steal.
func (rp *ReaderPool) listFlatOrSplit(ctx context.Context, id int, myDeque *Deque, item WorkItem) int64 {
	start := time.Now()
	var count int64

	// List first page
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(rp.bucket),
		Prefix:  aws.String(item.Prefix),
		MaxKeys: aws.Int32(1000),
	}
	resp, err := rp.client.ListObjectsV2(ctx, input)
	if err != nil {
		rp.logger.Printf("[reader-%d] list error prefix=%q: %v", id, item.Prefix, err)
		return 0
	}

	for i := range resp.Contents {
		rp.emit(&resp.Contents[i])
		count++
	}

	// If there's no more data, we're done
	if !aws.ToBool(resp.IsTruncated) {
		if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
			rp.logger.Printf("[reader-%d] prefix=%q %d objects in %v", id, item.Prefix, count, elapsed)
		}
		return count
	}

	// There's more data. Try range-splitting to parallelize this flat prefix.
	// Sample keys by skipping ahead with MaxKeys=1 at intervals.
	markers := rp.sampleRangeMarkers(ctx, item.Prefix, rangeSplitFactor)
	if len(markers) > 0 {
		rp.logger.Printf("[reader-%d] range-split %q into %d chunks (sampled %d markers)",
			id, item.Prefix, len(markers)+1, len(markers))

		// Build range work items:
		// chunk 0: StartAfter=lastKeyFromPage1, EndBefore=markers[0]
		// chunk 1: StartAfter=markers[0], EndBefore=markers[1]
		// ...
		// chunk N: StartAfter=markers[N-1], EndBefore=""
		lastKey := ""
		if len(resp.Contents) > 0 {
			lastKey = *resp.Contents[len(resp.Contents)-1].Key
		}

		var rangeItems []WorkItem
		prev := lastKey
		for _, m := range markers {
			// Skip markers that aren't past our current position
			if m <= prev {
				continue
			}
			rangeItems = append(rangeItems, WorkItem{
				Prefix:     item.Prefix,
				StartAfter: prev,
				EndBefore:  m,
			})
			prev = m
		}
		// Final open-ended chunk
		rangeItems = append(rangeItems, WorkItem{
			Prefix:     item.Prefix,
			StartAfter: prev,
		})

		if len(rangeItems) > 1 {
			myDeque.PushBatch(rangeItems)
			return count
		}
	}

	// Fallback: couldn't range-split, just continue listing sequentially
	count += rp.listContinuation(ctx, id, item.Prefix, resp.NextContinuationToken, start)
	return count
}

// sampleRangeMarkers uses a fast skip-ahead technique to find evenly-spaced
// keys across a flat prefix. It issues N small requests with StartAfter to
// probe the keyspace.
func (rp *ReaderPool) sampleRangeMarkers(ctx context.Context, prefix string, n int) []string {
	// First, estimate total count with a single request getting max keys
	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(rp.bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1000),
	}

	// Walk through pages to collect evenly-spaced markers.
	// We'll list up to 10 pages (10K keys) and sample from what we find.
	var allKeys []string
	pages := 0
	paginator := s3.NewListObjectsV2Paginator(rp.client, input)
	for paginator.HasMorePages() && pages < 10 {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil
		}
		for i := range page.Contents {
			if page.Contents[i].Key != nil {
				allKeys = append(allKeys, *page.Contents[i].Key)
			}
		}
		pages++
		if !aws.ToBool(page.IsTruncated) {
			break
		}
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

// listContinuation continues a paginated listing from a continuation token.
func (rp *ReaderPool) listContinuation(ctx context.Context, id int, prefix string, token *string, start time.Time) int64 {
	var count int64

	paginator := s3.NewListObjectsV2Paginator(rp.client, &s3.ListObjectsV2Input{
		Bucket:            aws.String(rp.bucket),
		Prefix:            aws.String(prefix),
		ContinuationToken: token,
	})

	for paginator.HasMorePages() {
		if ctx.Err() != nil {
			return count
		}
		page, err := paginator.NextPage(ctx)
		if err != nil {
			rp.logger.Printf("[reader-%d] list error prefix=%q: %v", id, prefix, err)
			return count
		}
		for i := range page.Contents {
			rp.emit(&page.Contents[i])
			count++
		}
	}

	if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
		rp.logger.Printf("[reader-%d] prefix=%q %d objects in %v (continuation)", id, prefix, count, elapsed)
	}
	return count
}

// listRange lists objects in a bounded key range under a prefix.
// Uses StartAfter and stops when hitting EndBefore.
func (rp *ReaderPool) listRange(ctx context.Context, id int, item WorkItem) int64 {
	start := time.Now()
	var count int64

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(rp.bucket),
		Prefix:  aws.String(item.Prefix),
		MaxKeys: aws.Int32(1000),
	}
	if item.StartAfter != "" {
		input.StartAfter = aws.String(item.StartAfter)
	}

	paginator := s3.NewListObjectsV2Paginator(rp.client, input)

	for paginator.HasMorePages() {
		if ctx.Err() != nil {
			return count
		}
		page, err := paginator.NextPage(ctx)
		if err != nil {
			rp.logger.Printf("[reader-%d] range list error prefix=%q: %v", id, item.Prefix, err)
			return count
		}
		for i := range page.Contents {
			key := page.Contents[i].Key
			if key == nil {
				continue
			}
			// Stop if we've reached the end boundary
			if item.EndBefore != "" && *key >= item.EndBefore {
				if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
					rp.logger.Printf("[reader-%d] range prefix=%q %d objects in %v",
						id, item.Prefix, count, elapsed)
				}
				return count
			}
			rp.emit(&page.Contents[i])
			count++
		}
	}

	if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
		rp.logger.Printf("[reader-%d] range prefix=%q %d objects in %v",
			id, item.Prefix, count, elapsed)
	}
	return count
}

func (rp *ReaderPool) emit(obj *s3types.Object) {
	if obj.Key == nil {
		return
	}
	rec := store.ObjectRecord{
		Key:  *obj.Key,
		Size: aws.ToInt64(obj.Size),
	}
	if obj.LastModified != nil {
		rec.LastModified = *obj.LastModified
	}
	rp.outCh <- rec
	rp.listed.Add(1)
}
