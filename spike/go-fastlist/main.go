// go-fastlist is the Go side of the Rust spike comparison (see
// spike/README.md). It drives s3lister's FastClient flat-out — no writers, no
// work-stealing, no Parquet — doing exactly the same work as
// spike/rust-fastlist's wire modes: ListObjectsV2 pagination over the bench
// layout's data/dNNN/sNNN/ prefixes, or GetObjectTagging over the bench
// layout's deterministic keys.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blake-golliher/s3lister/internal/config"
	"github.com/blake-golliher/s3lister/internal/s3client"
)

var populateExts = []string{"log", "jpg", "json", "parquet", "csv", "bin", "txt", "gz", "png", "pdf"}

// benchKey mirrors s3lister-bench's default index->key mapping.
func benchKey(i int64) string {
	if i%100 < 10 {
		return fmt.Sprintf("flat/f%02d/o-%012d", (i/100)%16, i)
	}
	return fmt.Sprintf("data/d%03d/s%03d/obj-%012d.%s",
		i%64, (i/64)%64, i, populateExts[i%10])
}

func main() {
	configPath := flag.String("config", "config.toml", "s3lister config file (endpoint/credentials)")
	bucket := flag.String("bucket", "", "bucket (required)")
	mode := flag.String("mode", "list", "list or tags")
	workers := flag.Int("workers", 256, "concurrent workers")
	seconds := flag.Int("seconds", 60, "run duration")
	count := flag.Int64("count", 100_000_000, "object count in the bucket (tags mode key range)")
	flag.Parse()

	if *bucket == "" {
		fmt.Fprintln(os.Stderr, "-bucket is required")
		os.Exit(1)
	}
	cfg, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	logger := log.New(io.Discard, "", 0)
	fast := s3client.NewFast(&cfg.S3, false, log.New(os.Stderr, "", 0))
	_ = logger

	deadline := time.Now().Add(time.Duration(*seconds) * time.Second)
	ctx, cancel := context.WithDeadline(context.Background(), deadline)
	defer cancel()

	var counter, done, errors atomic.Int64
	var wg sync.WaitGroup

	unit := "objs"
	switch *mode {
	case "list":
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				page := &s3client.ListPage{}
				for time.Now().Before(deadline) {
					idx := counter.Add(1) % 4096
					q := s3client.ListQuery{
						Prefix:  fmt.Sprintf("data/d%03d/s%03d/", idx/64, idx%64),
						MaxKeys: 1000,
					}
					for {
						if err := fast.ListPage(ctx, *bucket, &q, page); err != nil {
							if ctx.Err() == nil {
								errors.Add(1)
							}
							break
						}
						done.Add(int64(len(page.Objects)))
						if !page.IsTruncated || !time.Now().Before(deadline) {
							break
						}
						q.ContinuationToken = page.NextToken
					}
				}
			}()
		}
	case "tags":
		unit = "tags"
		for w := 0; w < *workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for time.Now().Before(deadline) {
					i := counter.Add(1) % *count
					if _, err := fast.GetTagging(ctx, *bucket, benchKey(i)); err != nil {
						if ctx.Err() == nil {
							errors.Add(1)
						}
						continue
					}
					done.Add(1)
				}
			}()
		}
	default:
		fmt.Fprintln(os.Stderr, "-mode must be list or tags")
		os.Exit(1)
	}

	start := time.Now()
	go func() {
		last := int64(0)
		lastT := start
		for time.Now().Before(deadline) {
			time.Sleep(5 * time.Second)
			now := time.Now()
			c := done.Load()
			fmt.Printf("%12d %s  %9.0f/s  errors=%d\n",
				c, unit, float64(c-last)/now.Sub(lastT).Seconds(), errors.Load())
			last, lastT = c, now
		}
	}()

	wg.Wait()
	total := done.Load()
	secs := time.Since(start).Seconds()
	fmt.Printf("TOTAL: %d %s in %.1fs = %.0f/s  errors=%d\n",
		total, unit, secs, float64(total)/secs, errors.Load())
}
