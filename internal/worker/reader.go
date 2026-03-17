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
	maxSplitDepth    = 4
	stealBackoffMin  = 500 * time.Microsecond
	stealBackoffMax  = 50 * time.Millisecond
	stealGiveUpAfter = 3 * time.Second
	delimiter        = "/"
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

	// Expand if we have fewer prefixes than workers so every worker starts busy
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

// spinWaitForWork uses exponential backoff instead of fixed sleep.
// Returns true if work became available.
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
		// If we're the only active worker and nothing is queued anywhere, bail
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
	// Try to split for more parallelism before flat-listing
	if item.Depth < maxSplitDepth {
		prefixes, err := rp.listPrefixes(ctx, item.Prefix)
		if err == nil && len(prefixes) > 1 {
			myDeque.PushBatch(prefixesToWorkItems(prefixes, item.Depth+1))
			rp.logger.Printf("[reader-%d] split %q into %d sub-prefixes (depth=%d)",
				id, item.Prefix, len(prefixes), item.Depth+1)
			return 0
		}
	}

	// Flat listing
	start := time.Now()
	var count int64

	paginator := s3.NewListObjectsV2Paginator(rp.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(rp.bucket),
		Prefix: aws.String(item.Prefix),
	})

	for paginator.HasMorePages() {
		if ctx.Err() != nil {
			return count
		}
		page, err := paginator.NextPage(ctx)
		if err != nil {
			rp.logger.Printf("[reader-%d] list error prefix=%q: %v", id, item.Prefix, err)
			return count
		}
		for i := range page.Contents {
			rp.emit(&page.Contents[i])
			count++
		}
	}

	if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
		rp.logger.Printf("[reader-%d] prefix=%q %d objects in %v", id, item.Prefix, count, elapsed)
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
