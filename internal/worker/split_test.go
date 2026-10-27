package worker

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/blake-golliher/s3lister/internal/config"
	"github.com/blake-golliher/s3lister/internal/model"
	"github.com/blake-golliher/s3lister/internal/s3client"
)

// runPool scans everything handler serves and returns how many times each
// key was emitted.
func runPool(t *testing.T, handler http.Handler, prefix string, workers, pageSize int, logger *log.Logger) (map[string]int, *ReaderPool) {
	t.Helper()
	srv := httptest.NewServer(handler)
	defer srv.Close()
	if logger == nil {
		logger = log.New(io.Discard, "", 0)
	}
	fast := s3client.NewFast(&config.S3Config{
		AccessKey: "k", SecretKey: "s", Endpoint: srv.URL, Region: "us-east-1",
	}, false, log.New(io.Discard, "", 0))

	out := make(chan []model.ObjectRecord, 1024)
	rp := NewReaderPool(fast, "bucket", prefix, workers, pageSize, out, logger)
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
	return seen, rp
}

func checkExactlyOnce(t *testing.T, seen map[string]int, want []string) {
	t.Helper()
	wantSet := make(map[string]bool, len(want))
	for _, k := range want {
		wantSet[k] = true
		switch seen[k] {
		case 1:
		case 0:
			t.Errorf("MISSING key %q", k)
		default:
			t.Errorf("DUPLICATE key %q emitted %d times", k, seen[k])
		}
	}
	for k := range seen {
		if !wantSet[k] {
			t.Errorf("UNEXPECTED key %q", k)
		}
	}
}

func TestSplitPointProperties(t *testing.T) {
	// Bytes that matter: the delimiter, bytes on either side of it and of
	// the split alphabet's edges, and multi-byte UTF-8.
	alphabet := []string{"/", ".", " ", "-", "0", "1", "5", "9", "<", "=", ">", "A", "_", "a", "f", "z", "~", "\x7f", "é", "日"}
	rng := rand.New(rand.NewSource(1))
	randKey := func() string {
		var sb strings.Builder
		sb.WriteString("p/")
		for n := rng.Intn(8); n >= 0; n-- {
			sb.WriteString(alphabet[rng.Intn(len(alphabet))])
		}
		return sb.String()
	}
	splits := 0
	for i := 0; i < 200000; i++ {
		lo, hi := randKey(), randKey()
		if lo > hi {
			lo, hi = hi, lo
		}
		mid, ok := splitPoint(lo, hi)
		if !ok {
			continue
		}
		splits++
		if !(lo < mid && mid < hi) {
			t.Fatalf("splitPoint(%q, %q) = %q, not strictly between", lo, hi, mid)
		}
		common := 0
		for common < len(lo) && common < len(hi) && lo[common] == hi[common] {
			common++
		}
		for _, c := range []byte(mid[common:]) {
			if bytes.IndexByte(splitAlphabet, c) < 0 {
				t.Fatalf("splitPoint(%q, %q) = %q: byte %q outside the split alphabet", lo, hi, mid, c)
			}
		}
	}
	if splits < 1000 {
		t.Errorf("only %d of the random pairs split; the generator isn't exercising splitPoint", splits)
	}
	for _, c := range []struct{ lo, hi string }{
		{"a", "a"}, {"b", "a"}, {"a", "a0"}, {"p/x", "p/x0"},
	} {
		if mid, ok := splitPoint(c.lo, c.hi); ok {
			t.Errorf("splitPoint(%q, %q) = %q, want no split", c.lo, c.hi, mid)
		}
	}
	// Pairs with room for a cut, where the midpoint's second digit lands
	// on "above the alphabet". splitPoint used to drop that digit and cut
	// below lo, then give up on every later page of the range.
	for _, c := range []struct{ lo, hi string }{
		{"p/d_970_101/f_000999.c", "p/eQZXW9"},
		{"p/d_", "p/eQ"},
	} {
		mid, ok := splitPoint(c.lo, c.hi)
		if !ok || !(c.lo < mid && mid < c.hi) {
			t.Errorf("splitPoint(%q, %q) = %q, %v; want a cut strictly between", c.lo, c.hi, mid, ok)
		}
	}
}

// TestSplitPointConverges replays the open-ended case from real scans: a
// reader past the first page of a multi-million-object directory, splitting
// against "end of prefix". Cuts that land above the last key hand off empty
// ranges; the halving must reach the real keys within a handful of those.
func TestSplitPointConverges(t *testing.T) {
	for _, c := range []struct{ prefix, lo, maxKey string }{
		{"web/d_864_7/", "f_000999.css", "f_999999.xml"},
		// Nine levels deep, 2.5M objects, names past f_999999 growing a
		// seventh digit.
		{"scratch/d_225_6/d_705_10/d_707_11/d_623_15/d_127_18/d_370_28/d_026_54/d_970_101/",
			"f_000999.c", "f_999999.sh"},
	} {
		lo, maxKey, hi := c.prefix+c.lo, c.prefix+c.maxKey, c.prefix+"\x7f"
		for wasted := 0; ; wasted++ {
			if wasted > 20 {
				t.Fatalf("%s: still no cut inside the keys after %d empty ranges (hi=%q)", c.prefix, wasted, hi)
			}
			mid, ok := splitPoint(lo, hi)
			if !ok {
				t.Fatalf("splitPoint(%q, %q) gave up", lo, hi)
			}
			if mid <= maxKey {
				t.Logf("%s: cut %q after %d empty ranges", c.prefix, mid, wasted)
				break
			}
			hi = mid
		}
	}
}

// TestReaderPoolSplitsDeepDirectory: one huge directory far below the top
// of the tree must be shared by every free reader. Listing used to go flat
// past depth 8, and a flat range whose keys all sit under one sub-prefix
// cannot be cut (split points never contain the delimiter), so a single
// reader paged through it alone while the rest sat idle.
func TestReaderPoolSplitsDeepDirectory(t *testing.T) {
	var keys []string
	for i := 0; i < 30000; i++ {
		keys = append(keys, fmt.Sprintf("s/1/2/3/4/5/6/7/8/d_970_101/f_%06d.c", i*83))
	}
	keys = append(keys, "s/1/2/3/4/5/6/7/8/d_111_5/f_000001.c")

	var logBuf syncBuffer
	seen, rp := runPool(t, fakeS3Handler(keys), "", 16, 100, log.New(&logBuf, "", 0))
	checkExactlyOnce(t, seen, keys)

	// Every reader logs its total on exit. Split-off ranges that land past
	// the last key are cheap and expected, so count objects, not splits.
	busiest := 0
	for _, line := range strings.Split(logBuf.String(), "\n") {
		var id, n int
		if _, err := fmt.Sscanf(line, "[reader-%d] done: %d objects", &id, &n); err == nil && n > busiest {
			busiest = n
		}
	}
	if busiest > len(keys)/3 {
		t.Errorf("one reader listed %d of %d objects (%d splits); the directory was not shared",
			busiest, len(keys), rp.Splits())
	}
	t.Logf("busiest reader listed %d of %d objects, %d splits", busiest, len(keys), rp.Splits())
}

// TestReaderPoolSplitsExactlyOnce forces heavy splitting — tiny pages, many
// workers, big levels — across every layout that makes split points tricky,
// and demands every key exactly once.
func TestReaderPoolSplitsExactlyOnce(t *testing.T) {
	var keys []string
	add := func(format string, n int) {
		for i := 0; i < n; i++ {
			keys = append(keys, fmt.Sprintf(format, i))
		}
	}
	// A level holding both files and sub-prefixes, sorting on both sides of
	// the files, plus names that sort right around a sub-prefix: "sub",
	// "sub-x", "sub.txt", "sub/…", and "sub0" (the byte after "sub/").
	add("mix/f-%05d.txt", 3000)
	add("mix/a-dir/x-%03d", 100)
	add("mix/z-dir/deep/y-%03d", 200)
	add("mix/f-00500/inner-%02d", 20)
	add("mix/sub/c-%03d", 300)
	keys = append(keys, "mix/f-00500", "mix/sub", "mix/sub-x", "mix/sub.txt", "mix/sub0", "mix/sub0z")
	// A big flat directory.
	add("flat/f00/o-%07d", 20000)
	// Deep levels, where listing used to go flat.
	add("deep/1/2/3/4/5/6/7/8/9/k-%05d", 3000)
	add("deep/1/2/3/4/5/6/7/8/a/b/k-%03d", 300)
	// Names outside the split alphabet: spaces, tildes, non-ASCII.
	add("uni/a-%04d", 1500)
	add("uni/ü-%04d", 1500)
	add("uni/日本-%04d", 1500)
	add("sp/a b %04d", 1200)
	add("tilde/~%04d", 1200)
	// A folder-marker object ending in the delimiter.
	keys = append(keys, "mk/dir/", "mk/dir/x", "README.md", "a&b <weird>.txt", "z-last")

	handler := fakeS3Handler(keys)
	var totalSplits int64
	for run := 0; run < 3; run++ {
		seen, rp := runPool(t, handler, "", 16, 13, nil)
		if rp.ListErrors() != 0 || rp.Denied() != 0 {
			t.Fatalf("run %d: list errors %d, denied %d", run, rp.ListErrors(), rp.Denied())
		}
		checkExactlyOnce(t, seen, keys)
		if t.Failed() {
			t.Fatalf("run %d failed after %d splits", run, rp.Splits())
		}
		totalSplits += rp.Splits()
	}
	if totalSplits == 0 {
		t.Error("no range was ever split; the test isn't exercising splitting")
	}
	t.Logf("%d splits over 3 runs", totalSplits)
}

// TestReaderPoolDeniedNotRetried: a 403 is final. The prefix is requested
// once, counted as denied rather than abandoned, and the rest of the bucket
// still lists exactly once.
func TestReaderPoolDeniedNotRetried(t *testing.T) {
	var keys, want []string
	for i := 0; i < 300; i++ {
		keys = append(keys, fmt.Sprintf("deny/secret-%03d", i))
	}
	for i := 0; i < 3000; i++ {
		k := fmt.Sprintf("ok/obj-%05d", i)
		keys = append(keys, k)
		want = append(want, k)
	}

	var deniedReqs atomic.Int64
	inner := fakeS3Handler(keys)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Query().Get("prefix"), "deny/") {
			deniedReqs.Add(1)
			w.WriteHeader(http.StatusForbidden)
			io.WriteString(w, `<Error><Code>AccessDenied</Code><Message>Access Denied</Message><RequestId>req-42</RequestId></Error>`)
			return
		}
		inner(w, r)
	})

	var logBuf syncBuffer
	seen, rp := runPool(t, handler, "", 8, 100, log.New(&logBuf, "", 0))

	checkExactlyOnce(t, seen, want)
	if rp.Denied() != 1 || rp.ListErrors() != 0 {
		t.Errorf("denied=%d list_errors=%d, want 1/0", rp.Denied(), rp.ListErrors())
	}
	if n := deniedReqs.Load(); n != 1 {
		t.Errorf("denied prefix requested %d times, want 1 (403 must not be retried)", n)
	}
	logs := logBuf.String()
	if !strings.Contains(logs, `DENIED prefix="deny/"`) || !strings.Contains(logs, "request_id=req-42") {
		t.Errorf("log lacks the DENIED line with its request id:\n%s", logs)
	}
	if strings.Contains(logs, "ABANDONED") {
		t.Errorf("a 403 was reported as ABANDONED:\n%s", logs)
	}
}

// TestReaderPoolRetriesTransientAndLogs: a 503 is retried, the scan loses
// nothing, and the failed attempt is in the log.
func TestReaderPoolRetriesTransientAndLogs(t *testing.T) {
	var keys []string
	for i := 0; i < 500; i++ {
		keys = append(keys, fmt.Sprintf("flaky/obj-%04d", i))
	}
	var failed atomic.Bool
	inner := fakeS3Handler(keys)
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("prefix") == "flaky/" && failed.CompareAndSwap(false, true) {
			w.WriteHeader(http.StatusServiceUnavailable)
			io.WriteString(w, `<Error><Code>SlowDown</Code><Message>busy</Message><RequestId>req-7</RequestId></Error>`)
			return
		}
		inner(w, r)
	})

	var logBuf syncBuffer
	seen, rp := runPool(t, handler, "", 4, 100, log.New(&logBuf, "", 0))

	checkExactlyOnce(t, seen, keys)
	if rp.Denied() != 0 || rp.ListErrors() != 0 {
		t.Errorf("denied=%d list_errors=%d, want 0/0", rp.Denied(), rp.ListErrors())
	}
	if logs := logBuf.String(); !strings.Contains(logs, "LIST attempt 1/6 failed") || !strings.Contains(logs, "request_id=req-7") {
		t.Errorf("log lacks the failed attempt:\n%s", logs)
	}
}

// syncBuffer is a bytes.Buffer safe for the pool's concurrent loggers.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}
