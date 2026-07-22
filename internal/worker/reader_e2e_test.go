package worker

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/blake-golliher/s3lister/internal/config"
	"github.com/blake-golliher/s3lister/internal/model"
	"github.com/blake-golliher/s3lister/internal/s3client"
)

// fakeS3 serves ListObjectsV2 over a sorted in-memory key list with real S3
// semantics: prefix filtering, delimiter grouping into CommonPrefixes,
// start-after, continuation tokens, and max-keys accounting that counts keys
// and common prefixes alike. It is the ground truth the reader pipeline —
// fast client, parser, work-stealing, range-splitting — is verified against.
func fakeS3(t *testing.T, keys []string) *httptest.Server {
	t.Helper()
	sorted := append([]string(nil), keys...)
	sort.Strings(sorted)

	esc := func(s string) string {
		s = strings.ReplaceAll(s, "&", "&amp;")
		s = strings.ReplaceAll(s, "<", "&lt;")
		s = strings.ReplaceAll(s, ">", "&gt;")
		return s
	}

	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if q.Get("list-type") != "2" {
			http.Error(w, "not a v2 listing", http.StatusBadRequest)
			return
		}
		prefix := q.Get("prefix")
		delim := q.Get("delimiter")
		after := q.Get("start-after")
		if tok := q.Get("continuation-token"); tok != "" {
			after = tok // token wins over start-after, as on AWS
		}
		maxKeys := 1000
		if mk := q.Get("max-keys"); mk != "" {
			maxKeys, _ = strconv.Atoi(mk)
		}

		// First candidate strictly greater than `after` and within prefix.
		i := sort.Search(len(sorted), func(i int) bool { return sorted[i] > after })

		var contents, prefixes []string
		lastConsumed := ""
		entries := 0
		truncated := false
		for ; i < len(sorted); i++ {
			k := sorted[i]
			if !strings.HasPrefix(k, prefix) {
				if k > prefix+"\xff" {
					break
				}
				continue
			}
			if entries == maxKeys {
				truncated = true
				break
			}
			rest := k[len(prefix):]
			if delim != "" {
				if j := strings.Index(rest, delim); j >= 0 {
					cp := prefix + rest[:j+1]
					prefixes = append(prefixes, cp)
					// Consume every key under this common prefix.
					for i < len(sorted) && strings.HasPrefix(sorted[i], cp) {
						lastConsumed = sorted[i]
						i++
					}
					i--
					entries++
					continue
				}
			}
			contents = append(contents, k)
			lastConsumed = k
			entries++
		}

		var sb strings.Builder
		sb.WriteString(`<?xml version="1.0" encoding="UTF-8"?><ListBucketResult>`)
		fmt.Fprintf(&sb, "<IsTruncated>%v</IsTruncated>", truncated)
		if truncated {
			fmt.Fprintf(&sb, "<NextContinuationToken>%s</NextContinuationToken>", esc(lastConsumed))
		}
		for _, k := range contents {
			fmt.Fprintf(&sb, "<Contents><Key>%s</Key><LastModified>2026-07-21T00:00:00.000Z</LastModified><ETag>&quot;%x&quot;</ETag><Size>%d</Size><StorageClass>STANDARD</StorageClass></Contents>",
				esc(k), len(k), len(k))
		}
		for _, p := range prefixes {
			fmt.Fprintf(&sb, "<CommonPrefixes><Prefix>%s</Prefix></CommonPrefixes>", esc(p))
		}
		sb.WriteString(`</ListBucketResult>`)
		io.WriteString(w, sb.String())
	}))
}

// TestReaderPoolExactlyOnce runs the full reader pipeline against a fake
// bucket whose layout exercises every code path — delimiter recursion, mixed
// files-and-dirs levels, a flat prefix large enough to force range-splitting,
// and keys needing XML escaping — and demands the output be exactly the key
// set: nothing missing, nothing duplicated.
func TestReaderPoolExactlyOnce(t *testing.T) {
	var keys []string
	// Hierarchical: 8 dirs x 2 subdirs x 150 objects.
	for d := 0; d < 8; d++ {
		for s := 0; s < 2; s++ {
			for i := 0; i < 150; i++ {
				keys = append(keys, fmt.Sprintf("data/d%02d/s%d/obj-%05d.log", d, s, i))
			}
		}
	}
	// Files directly at a level that also has subdirectories.
	for i := 0; i < 25; i++ {
		keys = append(keys, fmt.Sprintf("data/top-%03d.txt", i))
	}
	// One flat prefix big enough to trigger range-splitting (> one page,
	// no delimiters below it).
	for i := 0; i < 15000; i++ {
		keys = append(keys, fmt.Sprintf("flat/f00/o-%07d", i))
	}
	// Top-level keys and awkward names.
	keys = append(keys, "README.md", "a&b <weird>.txt", "z-last")

	srv := fakeS3(t, keys)
	defer srv.Close()

	fast := s3client.NewFast(&config.S3Config{
		AccessKey: "k", SecretKey: "s", Endpoint: srv.URL, Region: "us-east-1",
	}, false, log.New(io.Discard, "", 0))

	out := make(chan []model.ObjectRecord, len(keys)/100+16)
	rp := NewReaderPool(fast, "bucket", "", 8, 1000, out, log.New(io.Discard, "", 0))

	done := make(chan struct{})
	seen := make(map[string]int)
	go func() {
		defer close(done)
		for batch := range out {
			for _, r := range batch {
				seen[r.Key]++
			}
		}
	}()

	if err := rp.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(out)
	<-done

	if rp.ListErrors() != 0 {
		t.Fatalf("list errors: %d", rp.ListErrors())
	}
	if len(seen) != len(keys) {
		t.Errorf("saw %d distinct keys, want %d", len(seen), len(keys))
	}
	for _, k := range keys {
		switch seen[k] {
		case 1:
		case 0:
			t.Errorf("MISSING key %q", k)
		default:
			t.Errorf("DUPLICATE key %q emitted %d times", k, seen[k])
		}
	}
	if rp.Listed() != int64(len(keys)) {
		t.Errorf("Listed() = %d, want %d", rp.Listed(), len(keys))
	}
}

// TestReaderPoolPrefixScoped verifies prefix scoping still holds on the fast
// path: only the subtree is emitted, exactly once.
func TestReaderPoolPrefixScoped(t *testing.T) {
	var keys, want []string
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("in/scope/obj-%04d", i)
		keys = append(keys, k)
		want = append(want, k)
	}
	for i := 0; i < 500; i++ {
		keys = append(keys, fmt.Sprintf("out/of/scope-%04d", i))
	}

	srv := fakeS3(t, keys)
	defer srv.Close()

	fast := s3client.NewFast(&config.S3Config{
		AccessKey: "k", SecretKey: "s", Endpoint: srv.URL, Region: "us-east-1",
	}, false, log.New(io.Discard, "", 0))

	out := make(chan []model.ObjectRecord, 64)
	// Odd page size: exactly-once must hold regardless of page boundaries.
	rp := NewReaderPool(fast, "bucket", "in/", 4, 97, out, log.New(io.Discard, "", 0))

	done := make(chan struct{})
	seen := make(map[string]int)
	go func() {
		defer close(done)
		for batch := range out {
			for _, r := range batch {
				seen[r.Key]++
			}
		}
	}()
	if err := rp.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	close(out)
	<-done

	if len(seen) != len(want) {
		t.Errorf("saw %d keys, want %d", len(seen), len(want))
	}
	for _, k := range want {
		if seen[k] != 1 {
			t.Errorf("key %q seen %d times", k, seen[k])
		}
	}
}
