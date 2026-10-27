package worker

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
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
// covers a disjoint slice of the keyspace: one prefix level, or the key range
// (StartAfter, EndAt] of one level once it has been split. An item emits the
// objects in its slice and pushes each sub-prefix in its slice as a new
// WorkItem. Split points never contain the delimiter, so no sub-prefix
// straddles two slices. No code path lists the same slice twice.
type ReaderPool struct {
	client   *s3client.FastClient
	bucket   string
	prefix   string
	workers  int
	pageSize int
	outCh    chan<- []model.ObjectRecord
	listed   atomic.Int64
	logger   *log.Logger
	deques   []*Deque
	working  atomic.Int32 // workers currently processing an item
	idle     atomic.Int32 // workers waiting for work; busy ones split for them
	splits   atomic.Int64 // ranges split off for idle workers

	// listErrors counts work items abandoned after all page retries failed;
	// denied counts those refused with HTTP 403, which are never retried. Any
	// non-zero value means the scan is INCOMPLETE: some slice of the keyspace
	// was never listed. Callers must surface both loudly.
	listErrors atomic.Int64
	denied     atomic.Int64
}

// ListErrors returns the number of work items abandoned after exhausting
// their retries. Non-zero means the output is missing part of the bucket.
func (rp *ReaderPool) ListErrors() int64 {
	return rp.listErrors.Load()
}

// Denied returns the number of work items refused with HTTP 403: the
// credentials cannot list them, so a re-run won't either. Non-zero means the
// output is missing part of the bucket.
func (rp *ReaderPool) Denied() int64 {
	return rp.denied.Load()
}

// Splits returns how many ranges were split off for idle workers.
func (rp *ReaderPool) Splits() int64 {
	return rp.splits.Load()
}

// listPageWithRetry fetches one page for the query, retrying with exponential
// jittered backoff and logging every failed attempt. A 403 comes back at
// once: retrying cannot fix a refusal. The query's continuation state is
// owned by the caller and only advances on success, so a retry re-requests
// the same page.
func (rp *ReaderPool) listPageWithRetry(ctx context.Context, id int, q *s3client.ListQuery, page *s3client.ListPage) error {
	backoff := listBackoffMin
	for attempt := 1; ; attempt++ {
		start := time.Now()
		err := rp.client.ListPage(ctx, rp.bucket, q, page)
		if err == nil || ctx.Err() != nil || s3client.Permanent(err) || attempt == listAttempts {
			return err
		}
		wait := backoff + time.Duration(rand.Int63n(int64(backoff)))
		rp.logger.Printf("[reader-%d] LIST attempt %d/%d failed prefix=%q start_after=%q token=%q in %v, retrying in %v: %v",
			id, attempt, listAttempts, q.Prefix, q.StartAfter, q.ContinuationToken,
			time.Since(start).Round(time.Millisecond), wait.Round(time.Millisecond), err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
		if backoff < listBackoffMax {
			backoff *= 2
		}
	}
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
	rp.logger.Printf("[reader-pool] done: %d objects in %v (%d range splits)",
		rp.listed.Load(), time.Since(start), rp.splits.Load())
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
	rp.idle.Add(1)
	defer rp.idle.Add(-1)
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

// processItem lists one work item's slice of a level: its objects are
// emitted, its sub-prefixes become new work items as each page reveals them,
// and pages continue until the slice is exhausted. While other readers sit
// idle, the unlisted remainder is split and its far half handed off, so a
// level of millions of objects is shared by every free reader instead of
// being paged through by one.
func (rp *ReaderPool) processItem(ctx context.Context, id int, myDeque *Deque, item WorkItem, page *s3client.ListPage) int64 {
	start := time.Now()
	var count int64

	q := s3client.ListQuery{
		Prefix:     item.Prefix,
		StartAfter: item.StartAfter,
		MaxKeys:    rp.pageSize,
	}
	// Beyond the split-depth limit, list the whole subtree flat rather than
	// recursing further.
	if item.Depth < maxSplitDepth {
		q.Delimiter = delimiter
	}
	end := item.EndAt       // inclusive; "" means the end of the prefix. Shrinks on split.
	last := item.StartAfter // the last entry listed so far

	for {
		if ctx.Err() != nil {
			return count
		}
		if err := rp.listPageWithRetry(ctx, id, &q, page); err != nil {
			if ctx.Err() == nil {
				rp.reportLost(id, item.Prefix, last, end, err)
			}
			return count
		}

		objs, prefixes := page.Objects, page.CommonPrefixes
		done := !page.IsTruncated
		if end != "" {
			// Keep only entries up to the boundary; anything past it belongs
			// to the item the far half was handed to.
			n, m := 0, 0
			for n < len(objs) && objs[n].Key <= end {
				n++
			}
			for m < len(prefixes) && prefixes[m] <= end {
				m++
			}
			if n < len(objs) || m < len(prefixes) {
				done = true
			}
			objs, prefixes = objs[:n], prefixes[:m]
		}

		recs := toRecords(objs)
		rp.emitBatch(recs)
		count += int64(len(recs))
		if len(prefixes) > 0 {
			myDeque.PushBatch(prefixesToWorkItems(prefixes, item.Depth+1))
			rp.logger.Printf("[reader-%d] split %q into %d sub-prefixes (depth=%d)",
				id, item.Prefix, len(prefixes), item.Depth+1)
		}
		if done {
			break
		}
		last = lastEntry(last, objs, prefixes)
		q.ContinuationToken = page.NextToken
		rp.maybeSplit(myDeque, item, last, &end)
	}

	if elapsed := time.Since(start); count > 1000 || elapsed > 2*time.Second {
		if item.StartAfter != "" {
			rp.logger.Printf("[reader-%d] range prefix=%q after=%q %d objects in %v",
				id, item.Prefix, item.StartAfter, count, elapsed)
		} else {
			rp.logger.Printf("[reader-%d] prefix=%q %d objects in %v", id, item.Prefix, count, elapsed)
		}
	}
	return count
}

// maybeSplit hands the far half of the unlisted range (last, *end] to idle
// readers. It splits only while someone is idle and this reader has nothing
// queued for them to steal, so a busy scan never fragments. The cut is a
// synthetic key near the lexicographic midpoint, so finding it costs no
// requests. If it lands in empty keyspace the thief finishes in one request
// and goes idle again, and the next cut here lands closer: the halving homes
// in on wherever the keys actually are.
func (rp *ReaderPool) maybeSplit(myDeque *Deque, item WorkItem, last string, end *string) {
	if last == "" || rp.idle.Load() == 0 || myDeque.Len() > 0 {
		return
	}
	lo := last
	if strings.HasSuffix(lo, delimiter) {
		// Past a sub-prefix means past every key under it: "a/b/" -> "a/b0"
		// ('0' is the byte after '/').
		lo = strings.TrimSuffix(lo, delimiter) + "0"
	}
	hi := *end
	if hi == "" {
		hi = item.Prefix + string(rune(splitCeil))
	}
	mid, ok := splitPoint(lo, hi)
	if !ok || !strings.HasPrefix(mid, item.Prefix) {
		return
	}
	myDeque.PushFront(WorkItem{Prefix: item.Prefix, Depth: item.Depth, StartAfter: mid, EndAt: *end})
	*end = mid
	rp.splits.Add(1)
}

// splitAlphabet is the bytes split points are built from, in order: '0'..'~'
// without '<' and '>'. Leaving out the '/' delimiter and everything sorting
// below it means a split point never falls inside a sub-prefix (each
// sub-prefix lies wholly on one side of it). '<' and '>' are out because VAST
// echoes start-after back into its XML response unescaped, and a '<' there
// makes the page unparseable. Keys that continue with bytes above '~'
// (non-ASCII names) sort past every split point, so they stay in a level's
// last range rather than being subdivided.
var splitAlphabet = func() []byte {
	var a []byte
	for c := byte('0'); c <= '~'; c++ {
		if c != '<' && c != '>' {
			a = append(a, c)
		}
	}
	return a
}()

const (
	// splitCeil stands in for "end of prefix": it sorts above every byte
	// of splitAlphabet.
	splitCeil   = 0x7f
	splitDigits = 6
)

// splitPoint returns a key strictly between lo and hi, near their
// lexicographic midpoint. ok is false when the two are too close for a
// split-point key to fit between them.
func splitPoint(lo, hi string) (string, bool) {
	if lo >= hi {
		return "", false
	}
	i := 0
	for i < len(lo) && i < len(hi) && lo[i] == hi[i] {
		i++
	}
	// Read the bytes after the common prefix as digits in base
	// len(splitAlphabet)+1 — a byte's digit is how many alphabet bytes sort
	// below it, the top digit meaning "above the alphabet" — and average
	// the two numbers.
	ceil := int64(len(splitAlphabet))
	base := ceil + 1
	digit := func(s string, j int) int64 {
		if j >= len(s) {
			return 0
		}
		return int64(sort.Search(len(splitAlphabet), func(k int) bool { return splitAlphabet[k] >= s[j] }))
	}
	var a, b int64
	for j := 0; j < splitDigits; j++ {
		a = a*base + digit(lo, i+j)
		b = b*base + digit(hi, i+j)
	}
	m := a + (b-a)/2
	ds := make([]int64, splitDigits)
	for j := splitDigits - 1; j >= 0; j-- {
		ds[j] = m % base
		m /= base
	}
	var buf []byte
	for _, d := range ds {
		if d == ceil {
			break // "above the alphabet" only means end of prefix; never send it
		}
		buf = append(buf, splitAlphabet[d])
	}
	// Trailing lowest bytes add length but almost no information.
	n := len(buf)
	for n > 0 && buf[n-1] == splitAlphabet[0] {
		n--
	}
	// The digit mapping makes the arithmetic approximate; the comparison is
	// what guarantees the result.
	for _, cand := range []string{hi[:i] + string(buf[:n]), hi[:i] + string(buf)} {
		if lo < cand && cand < hi {
			return cand, true
		}
	}
	return "", false
}

// lastEntry returns the greater of last and a page's final entry, which may
// be an object or a sub-prefix.
func lastEntry(last string, objs []s3client.ListedObject, prefixes []string) string {
	if n := len(objs); n > 0 && objs[n-1].Key > last {
		last = objs[n-1].Key
	}
	if n := len(prefixes); n > 0 && prefixes[n-1] > last {
		last = prefixes[n-1]
	}
	return last
}

// reportLost records the unlisted remainder of a work item. Either way the
// scan is incomplete; the log line says whether a re-run could help.
func (rp *ReaderPool) reportLost(id int, prefix, last, end string, err error) {
	what := fmt.Sprintf("prefix=%q", prefix)
	if last != "" || end != "" {
		what += fmt.Sprintf(" after=%q end=%q", last, end)
	}
	if s3client.Permanent(err) {
		rp.denied.Add(1)
		rp.logger.Printf("[reader-%d] DENIED %s (scan incomplete, not retried): %v", id, what, err)
		return
	}
	rp.listErrors.Add(1)
	rp.logger.Printf("[reader-%d] ABANDONED %s after %d attempts (scan incomplete): %v",
		id, what, listAttempts, err)
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
