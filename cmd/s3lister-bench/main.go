// s3lister-bench populates S3 buckets with synthetic objects for benchmarking
// s3lister. It is a standalone tool so the main binary stays focused on its
// job; see bench-readme.md for the methodology and how to reproduce our
// published numbers.
package main

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/url"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/blake-golliher/s3lister/internal/config"
	"github.com/blake-golliher/s3lister/internal/s3client"
)

// Extensions rotated across hierarchical keys so the extension column in the
// scan output looks like a real bucket.
var populateExts = []string{"log", "jpg", "json", "parquet", "csv", "bin", "txt", "gz", "png", "pdf"}

// populateChunk is how many object indices a worker claims at a time. Small
// enough that the resume watermark stays tight, large enough that the atomic
// dispatch counter is never contended.
const populateChunk = 1000

// The -tags layout, like the key layout, is a pure function of the object
// index so every distribution is known in advance and a `scan -tags` result
// can be verified to exact counts:
//
//   - object i carries i%5 tags (0-4), so a fifth of the bucket sits in each
//     tag_count bucket and the mean is exactly 2 tags/object
//   - tag j of object i uses key benchTagKeys[(i+j)%8], so keys vary from
//     object to object and each key appears on exactly count/4 objects
//   - values cycle per 40-index block: benchTagVals[k][(i/40)%4], so each
//     key=value pair appears on exactly count/16 objects
//
// (The exact-count claims hold when -count is divisible by 160; the whole tag
// set is a function of i mod 160, which also lets the encoded strings be
// precomputed once.) See bench-readme.md for the verification queries.
var benchTagKeys = []string{"env", "team", "project", "tier", "owner", "app", "cost-center", "retention"}

var benchTagVals = [][]string{
	{"prod", "staging", "dev", "qa"},             // env
	{"core", "data", "infra", "ml"},              // team
	{"apollo", "borealis", "cascade", "dune"},    // project
	{"hot", "warm", "cold", "archive"},           // tier
	{"alice", "bob", "carol", "dan"},             // owner
	{"ingest", "etl", "serving", "backup"},       // app
	{"cc-1001", "cc-1002", "cc-2001", "cc-3001"}, // cost-center
	{"30d", "90d", "1y", "forever"},              // retention
}

// benchTagging returns the URL-encoded tag set for object index i ("" when the
// object has none), in the form PutObject's Tagging parameter wants.
func benchTagging(i int64) string {
	n := i % 5
	if n == 0 {
		return ""
	}
	v := url.Values{}
	vi := (i / 40) % 4
	for j := int64(0); j < n; j++ {
		k := (i + j) % 8
		v.Set(benchTagKeys[k], benchTagVals[k][vi])
	}
	return v.Encode()
}

// buildTaggingTable precomputes all 160 distinct encoded tag strings so the
// PUT hot path does a table lookup instead of URL-encoding per object.
func buildTaggingTable() []string {
	t := make([]string, 160)
	for i := range t {
		t[i] = benchTagging(int64(i))
	}
	return t
}

// putAttempts and putBackoffMin shape the per-key retry loop on top of the
// SDK's own retries. The SDK's client-side retry budget drains instantly
// during a mass event (e.g. VIPs migrating between nodes reset every
// connection at once), so without this layer a few seconds of cluster blip
// becomes thousands of "failed" keys. Six attempts with exponential jittered
// backoff spans roughly 12 seconds - enough to ride out a VIP failover.
const (
	putAttempts   = 6
	putBackoffMin = 200 * time.Millisecond
	putBackoffMax = 5 * time.Second
)

// maxFinalErrors aborts the run when this many keys have failed even after
// per-key retries - at that point the outage is sustained, not transient.
const maxFinalErrors = 1000

// benchKey deterministically maps an object index to a key. The layout mixes:
//   - hierarchical keys: data/dNNN/sNNN/obj-NNNNNNNNNNNN.ext — exercises
//     prefix discovery and gives the scan wide parallelism
//   - flat keys: flat/fNN/o-NNNNNNNNNNNN — a few huge flat prefixes that
//     exercise the range-splitting path
//
// Being a pure function of the index, the same flags always produce the same
// keyspace, so an interrupted populate can resume with -start and re-PUTs are
// idempotent overwrites.
func benchKey(i int64, d1, d2, flatPct, flatDirs int64) string {
	if flatPct > 0 && i%100 < flatPct {
		fd := (i / 100) % flatDirs
		return fmt.Sprintf("flat/f%02d/o-%012d", fd, i)
	}
	a := i % d1
	b := (i / d1) % d2
	ext := populateExts[i%int64(len(populateExts))]
	return fmt.Sprintf("data/d%03d/s%03d/obj-%012d.%s", a, b, i, ext)
}

func main() {
	fs := flag.NewFlagSet("s3lister-bench", flag.ExitOnError)
	configPath := fs.String("config", "config.toml", "path to s3lister config file (endpoint/credentials)")
	bucket := fs.String("bucket", "", "target bucket (required; created if missing)")
	count := fs.Int64("count", 0, "total number of objects the bucket should contain (required)")
	start := fs.Int64("start", 0, "starting object index, for resuming an interrupted run")
	workers := fs.Int("workers", 256, "concurrent PUT workers")
	size := fs.Int64("size", 0, "object body size in bytes (0 = empty objects, fastest)")
	dirs1 := fs.Int64("dirs1", 64, "first-level directory fanout")
	dirs2 := fs.Int64("dirs2", 64, "second-level directory fanout")
	flatPct := fs.Int64("flat-pct", 10, "percent of objects placed in large flat prefixes (0-100)")
	flatDirs := fs.Int64("flat-dirs", 16, "number of flat prefixes")
	tags := fs.Bool("tags", false, "attach a deterministic variety of tags (0-4 per object) for testing scan -tags")
	logPath := fs.String("log", "./s3lister-bench.log", "log file path")
	failedPath := fs.String("failed-keys", "./s3lister-bench.failed", "file recording every key that failed after all retries")
	verbose := fs.Bool("verbose", false, "verbose output: log to stderr")
	fs.Parse(os.Args[1:])

	if *bucket == "" || *count <= 0 {
		fmt.Fprintf(os.Stderr, "s3lister-bench: -bucket and -count are required\n\n")
		fs.Usage()
		os.Exit(1)
	}
	if *flatPct < 0 || *flatPct > 100 {
		fatal(nil, "-flat-pct must be 0-100")
	}
	if *dirs1 < 1 || *dirs2 < 1 || *flatDirs < 1 {
		fatal(nil, "-dirs1, -dirs2 and -flat-dirs must be >= 1")
	}

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(nil, "%v", err)
	}

	logger, logFile := setupLogger(*logPath, *verbose)
	defer logFile.Close()

	logger.Printf("=== s3lister-bench populate starting ===")
	logger.Printf("config: bucket=%s endpoint=%s count=%d start=%d workers=%d size=%d dirs=%dx%d flat=%d%%/%d tags=%v",
		*bucket, cfg.S3.Endpoint, *count, *start, *workers, *size, *dirs1, *dirs2, *flatPct, *flatDirs, *tags)
	if *tags && *count%160 != 0 {
		logger.Printf("note: -count %d is not divisible by 160, so the per-key/per-value tag counts will be near-exact rather than exact (see bench-readme.md)", *count)
	}

	client, err := s3client.NewClient(&cfg.S3, false, logger)
	if err != nil {
		fatal(logger, "%v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ensureBucket(ctx, client, *bucket, cfg.S3.Region); err != nil {
		fatal(logger, "cannot create bucket %s: %v", *bucket, err)
	}
	logger.Printf("bucket %q ready", *bucket)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig := <-sigCh
		logger.Printf("received %v, stopping workers", sig)
		fmt.Fprintf(os.Stderr, "\nReceived %v, stopping...\n", sig)
		cancel()
	}()

	var body []byte
	if *size > 0 {
		body = bytes.Repeat([]byte{'x'}, int(*size))
	}

	p := &populator{
		client:     client,
		bucket:     *bucket,
		body:       body,
		end:        *count,
		d1:         *dirs1,
		d2:         *dirs2,
		flatPct:    *flatPct,
		flatDirs:   *flatDirs,
		logger:     logger,
		cancel:     cancel,
		failedPath: *failedPath,
		inflight:   make(map[int64]struct{}),
	}
	if *tags {
		p.tagging = buildTaggingTable()
	}
	defer p.closeFailedKeys()
	p.next.Store(*start)

	totalStart := time.Now()

	stopProgress := make(chan struct{})
	var progressWG sync.WaitGroup
	progressWG.Add(1)
	go func() {
		defer progressWG.Done()
		p.progressLoop(totalStart, *start, stopProgress)
	}()

	var wg sync.WaitGroup
	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.worker(ctx)
		}()
	}
	wg.Wait()
	close(stopProgress)
	progressWG.Wait()

	created := p.created.Load()
	errCount := p.errors.Load()
	elapsed := time.Since(totalStart)
	rate := float64(created) / elapsed.Seconds()

	fmt.Fprintf(os.Stderr, "\n")
	if ctx.Err() != nil || errCount > 0 {
		resume := p.resumePoint()
		logger.Printf("populate interrupted: created=%d errors=%d elapsed=%v resume_at=%d",
			created, errCount, elapsed, resume)
		fmt.Fprintf(os.Stderr, "Interrupted. %d objects PUT in %v (%.0f/s), %d errors\n",
			created, elapsed.Round(time.Second), rate, errCount)
		fmt.Fprintf(os.Stderr, "Resume with:  -start %d\n", resume)
		if errCount > 0 {
			p.closeFailedKeys()
			tagFlag := ""
			if *tags {
				tagFlag = " -tags"
			}
			fmt.Fprintf(os.Stderr, "Failed keys recorded in:  %s\n", *failedPath)
			fmt.Fprintf(os.Stderr, "Re-PUT just those (instead of resuming) with:\n")
			fmt.Fprintf(os.Stderr, "  grep -o '[0-9]\\{12\\}' %s | while read i; do %s -bucket %s%s -count $((10#$i + 1)) -start $((10#$i)) -workers 1; done\n",
				*failedPath, os.Args[0], *bucket, tagFlag)
		}
		os.Exit(1)
	}

	logger.Printf("=== populate complete === objects=%d errors=%d elapsed=%v rate=%.0f/s",
		created, errCount, elapsed, rate)
	fmt.Fprintf(os.Stderr, "Done! %d objects PUT in %v\n", created, elapsed.Round(time.Second))
	fmt.Fprintf(os.Stderr, "  avg %.0f/s   bucket: %s   total objects: %d\n", rate, *bucket, *count)
}

func fatal(logger *log.Logger, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if logger != nil {
		logger.Printf("FATAL: %s", msg)
	}
	fmt.Fprintf(os.Stderr, "FATAL: %s\n", msg)
	os.Exit(1)
}

// setupLogger creates the file logger, teeing to stderr in verbose mode.
func setupLogger(logPath string, verbose bool) (*log.Logger, *os.File) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fatal(nil, "cannot open log file %s: %v", logPath, err)
	}
	flags := log.LstdFlags | log.Lmicroseconds
	if verbose {
		return log.New(io.MultiWriter(logFile, os.Stderr), "", flags), logFile
	}
	return log.New(logFile, "", flags), logFile
}

// ensureBucket creates the bucket, tolerating it already existing.
func ensureBucket(ctx context.Context, client *s3.Client, bucket, region string) error {
	input := &s3.CreateBucketInput{Bucket: aws.String(bucket)}
	if region != "" && region != "us-east-1" {
		input.CreateBucketConfiguration = &s3types.CreateBucketConfiguration{
			LocationConstraint: s3types.BucketLocationConstraint(region),
		}
	}
	_, err := client.CreateBucket(ctx, input)
	if err == nil {
		return nil
	}
	var owned *s3types.BucketAlreadyOwnedByYou
	var exists *s3types.BucketAlreadyExists
	if errors.As(err, &owned) || errors.As(err, &exists) {
		return nil
	}
	return err
}

type populator struct {
	client   *s3.Client
	bucket   string
	body     []byte
	end      int64
	d1, d2   int64
	flatPct  int64
	flatDirs int64
	tagging  []string // 160 precomputed tag strings, indexed by i%160; nil = no tags
	logger   *log.Logger
	cancel   context.CancelFunc // stops every worker when the error cap trips

	next    atomic.Int64 // next unclaimed object index
	created atomic.Int64
	errors  atomic.Int64 // keys that failed even after per-key retries

	mu         sync.Mutex
	inflight   map[int64]struct{} // chunk start indices claimed but not finished
	failedPath string             // sidecar file for finally-failed keys ("" = disabled)
	failedF    *os.File           // opened lazily on first failure
}

// recordFailedKey appends a finally-failed key to the sidecar file so the
// exact set of missing objects survives the run (the log only samples the
// first few). The file is created lazily so clean runs leave nothing behind.
func (p *populator) recordFailedKey(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failedPath == "" {
		return
	}
	if p.failedF == nil {
		f, err := os.OpenFile(p.failedPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			p.logger.Printf("[populate] cannot open failed-keys file %s: %v", p.failedPath, err)
			p.failedPath = ""
			return
		}
		p.failedF = f
	}
	fmt.Fprintln(p.failedF, key)
}

func (p *populator) closeFailedKeys() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failedF != nil {
		p.failedF.Close()
		p.failedF = nil
	}
}

// worker claims chunks of indices and PUTs each key until the range is
// exhausted, the context is cancelled, or too many keys fail outright.
func (p *populator) worker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}
		chunkStart := p.next.Add(populateChunk) - populateChunk
		if chunkStart >= p.end {
			return
		}
		chunkEnd := chunkStart + populateChunk
		if chunkEnd > p.end {
			chunkEnd = p.end
		}

		p.mu.Lock()
		p.inflight[chunkStart] = struct{}{}
		p.mu.Unlock()

		failed := false
		for i := chunkStart; i < chunkEnd; i++ {
			if ctx.Err() != nil {
				return // chunk stays in-flight → resume point stays safe
			}
			key := benchKey(i, p.d1, p.d2, p.flatPct, p.flatDirs)
			if err := p.putWithRetry(ctx, i, key); err != nil {
				if ctx.Err() != nil {
					return
				}
				failed = true
				p.recordFailedKey(key)
				n := p.errors.Add(1)
				if n <= 20 {
					p.logger.Printf("[populate] PUT failed after %d attempts key=%s: %v",
						putAttempts, key, err)
				}
				if n == maxFinalErrors {
					p.logger.Printf("[populate] %d keys failed despite retries, aborting", n)
					fmt.Fprintf(os.Stderr, "\nToo many PUT failures (%d), aborting — see log\n", n)
					p.cancel()
					return
				}
				continue
			}
			p.created.Add(1)
		}

		// A chunk leaves the resume window only if every key in it succeeded;
		// chunks with failures stay in-flight so the printed -start provably
		// re-covers every lost key.
		if !failed {
			p.mu.Lock()
			delete(p.inflight, chunkStart)
			p.mu.Unlock()
		}
	}
}

// putWithRetry layers exponential jittered backoff on top of the SDK's own
// retries, so transient events (VIP failover, node restart) are absorbed
// instead of surfacing as failed keys.
func (p *populator) putWithRetry(ctx context.Context, i int64, key string) error {
	backoff := putBackoffMin
	var err error
	for attempt := 0; attempt < putAttempts; attempt++ {
		if attempt > 0 {
			jitter := time.Duration(rand.Int63n(int64(backoff)))
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff + jitter):
			}
			if backoff < putBackoffMax {
				backoff *= 2
			}
		}
		if err = p.put(ctx, i, key); err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return err
		}
	}
	return err
}

func (p *populator) put(ctx context.Context, i int64, key string) error {
	input := &s3.PutObjectInput{
		Bucket:        aws.String(p.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(p.body),
		ContentLength: aws.Int64(int64(len(p.body))),
	}
	if p.tagging != nil {
		if t := p.tagging[i%160]; t != "" {
			input.Tagging = aws.String(t)
		}
	}
	_, err := p.client.PutObject(ctx, input)
	return err
}

// resumePoint returns the lowest object index that is not guaranteed written:
// the smallest chunk still in flight, or the dispatch position if none are.
// Restarting from it re-PUTs at most a few chunks, which is idempotent.
func (p *populator) resumePoint() int64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	resume := p.next.Load()
	if resume > p.end {
		resume = p.end
	}
	for start := range p.inflight {
		if start < resume {
			resume = start
		}
	}
	return resume
}

// progressLoop renders a single-line progress display once per second and
// writes a status line to the log every 30s.
func (p *populator) progressLoop(totalStart time.Time, startIdx int64, stop <-chan struct{}) {
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()
	logTick := time.NewTicker(30 * time.Second)
	defer logTick.Stop()

	// Sliding window for the recent rate.
	lastCreated := int64(0)
	lastTime := totalStart

	for {
		select {
		case <-stop:
			return
		case <-tick.C:
			created := p.created.Load()
			now := time.Now()
			recent := float64(created-lastCreated) / now.Sub(lastTime).Seconds()
			lastCreated, lastTime = created, now

			done := startIdx + created
			remaining := p.end - done
			eta := "-"
			if recent > 0 && remaining > 0 {
				eta = (time.Duration(float64(remaining)/recent) * time.Second).Round(time.Second).String()
			}
			fmt.Fprintf(os.Stderr, "\r  %s / %s objs  %s/s  ETA %s   ",
				comma(done), comma(p.end), comma(int64(recent)), eta)
		case <-logTick.C:
			created := p.created.Load()
			elapsed := time.Since(totalStart)
			p.logger.Printf("[populate] progress created=%d errors=%d rate=%.0f/s elapsed=%v",
				created, p.errors.Load(), float64(created)/elapsed.Seconds(), elapsed.Round(time.Second))
		}
	}
}

// comma formats an integer with thousands separators.
func comma(n int64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}
