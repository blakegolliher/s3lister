package worker

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blake-golliher/s3lister/internal/model"
)

// TaggerPool sits between the reader and writer pools when tag collection is
// enabled. It consumes the reader's record batches, fetches each object's tags
// with GetObjectTagging, and forwards the enriched batches to the writers.
//
// Tags cost one API call per object — a ~1000x request amplification over
// listing — which is why the stage is opt-in and why it runs a wide pool of
// its own: throughput is workers / per-request latency, independent of how
// fast the listing side goes.
type TaggerPool struct {
	client  *s3.Client
	bucket  string
	workers int
	in      <-chan []model.ObjectRecord
	out     chan<- []model.ObjectRecord
	logger  *log.Logger

	// fetchOne is the per-object fetch; a field so tests can stub S3 out.
	fetchOne func(ctx context.Context, key string) (map[string]string, error)

	// Retry policy — the reader's list-retry constants by default; fields so
	// tests can shrink the backoff span.
	attempts   int
	backoffMin time.Duration
	backoffMax time.Duration

	tagged    atomic.Int64
	tagErrors atomic.Int64
	retries   atomic.Int64
}

func NewTaggerPool(client *s3.Client, bucket string, workers int, in <-chan []model.ObjectRecord, out chan<- []model.ObjectRecord, logger *log.Logger) *TaggerPool {
	tp := &TaggerPool{
		client:     client,
		bucket:     bucket,
		workers:    workers,
		in:         in,
		out:        out,
		logger:     logger,
		attempts:   listAttempts,
		backoffMin: listBackoffMin,
		backoffMax: listBackoffMax,
	}
	tp.fetchOne = tp.fetchFromS3
	return tp
}

// Tagged returns the number of objects whose tags were fetched successfully.
func (tp *TaggerPool) Tagged() int64 { return tp.tagged.Load() }

// TagErrors returns the number of objects whose tag fetch failed after all
// retries. Those rows carry NULL tags (tag_count = -1) in the output.
func (tp *TaggerPool) TagErrors() int64 { return tp.tagErrors.Load() }

// Retries returns the total number of retry attempts across all fetches — the
// live signal that the endpoint is throttling or erroring under tag load even
// when every fetch eventually succeeds.
func (tp *TaggerPool) Retries() int64 { return tp.retries.Load() }

// Run blocks until the input channel closes and every batch has been
// forwarded, then closes the output channel to release the writers.
func (tp *TaggerPool) Run(ctx context.Context) {
	start := time.Now()
	tp.logger.Printf("[tagger-pool] starting %d tag workers (one GetObjectTagging per object)", tp.workers)

	var wg sync.WaitGroup
	for i := 0; i < tp.workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tp.worker(ctx)
		}()
	}
	wg.Wait()
	close(tp.out)

	tp.logger.Printf("[tagger-pool] done: %d tagged, %d errors in %v",
		tp.tagged.Load(), tp.tagErrors.Load(), time.Since(start))
}

func (tp *TaggerPool) worker(ctx context.Context) {
	for batch := range tp.in {
		for i := range batch {
			// On shutdown, keep draining and forwarding so the pipeline
			// unwinds, but don't count or log the canceled fetches — an
			// interrupt is not a million tag failures.
			if ctx.Err() != nil {
				continue
			}
			tags, err := tp.fetchWithRetry(ctx, batch[i].Key)
			if err != nil {
				if ctx.Err() != nil {
					continue
				}
				tp.tagErrors.Add(1)
				tp.logger.Printf("[tagger] TAG FAILED key=%s: %v (row will have NULL tags)", batch[i].Key, err)
				continue
			}
			batch[i].Tags = tags
			tp.tagged.Add(1)
		}
		tp.out <- batch
	}
}

// fetchWithRetry mirrors the reader's page-retry policy: exponential jittered
// backoff on top of the SDK's own retries, bailing out early on cancellation.
func (tp *TaggerPool) fetchWithRetry(ctx context.Context, key string) (map[string]string, error) {
	backoff := tp.backoffMin
	var err error
	for attempt := 0; attempt < tp.attempts; attempt++ {
		if attempt > 0 {
			tp.retries.Add(1)
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff + jitter):
			}
			if backoff < tp.backoffMax {
				backoff *= 2
			}
		}
		var tags map[string]string
		tags, err = tp.fetchOne(ctx, key)
		if err == nil {
			return tags, nil
		}
		if ctx.Err() != nil {
			return nil, err
		}
	}
	return nil, err
}

// fetchFromS3 returns a non-nil map on success — even when the object has no
// tags — so the nil map stays reserved for "not collected / fetch failed".
func (tp *TaggerPool) fetchFromS3(ctx context.Context, key string) (map[string]string, error) {
	out, err := tp.client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(tp.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(out.TagSet))
	for _, t := range out.TagSet {
		tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}
	return tags, nil
}
