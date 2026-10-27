package s3client

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLatBucketBounds(t *testing.T) {
	// Buckets must tile the line: each one's lower bound is the previous
	// one's upper bound, and every latency lands inside its bucket.
	rng := rand.New(rand.NewSource(1))
	for i := 0; i < 100000; i++ {
		d := time.Duration(rng.Int63n(int64(200 * time.Second)))
		b := latBucket(d)
		lower := time.Duration(0)
		if b > 0 {
			lower = latUpper(b - 1)
		}
		us := d.Truncate(time.Microsecond)
		if us < lower || us >= latUpper(b) {
			t.Fatalf("%v landed in bucket %d = [%v, %v)", d, b, lower, latUpper(b))
		}
		if b > 0 && float64(latUpper(b)-lower)/float64(lower) > 0.26 {
			t.Fatalf("bucket %d [%v, %v) is wider than the promised resolution", b, lower, latUpper(b))
		}
	}
}

func TestOpStatsQuantilesAndWindow(t *testing.T) {
	var s OpStats
	for i := 0; i < 90; i++ {
		s.record(time.Millisecond, 200)
	}
	for i := 0; i < 9; i++ {
		s.record(100*time.Millisecond, 503)
	}
	s.record(0, 0) // transport failure

	var prev StatsSnapshot
	w := s.Window(&prev)
	if w.Requests != 100 || w.Status(200) != 90 || w.Status(503) != 9 || w.NetErrors != 1 {
		t.Fatalf("counts: reqs=%d 200=%d 503=%d net=%d", w.Requests, w.Status(200), w.Status(503), w.NetErrors)
	}
	if p50 := w.Quantile(0.5); p50 < time.Millisecond || p50 > 1250*time.Microsecond {
		t.Errorf("p50 = %v, want ~1ms", p50)
	}
	if p99 := w.Quantile(0.99); p99 < 100*time.Millisecond*8/10 || p99 > 100*time.Millisecond {
		t.Errorf("p99 = %v, want ~100ms", p99)
	}
	if w.Max != 100*time.Millisecond {
		t.Errorf("max = %v, want exactly 100ms", w.Max)
	}
	if got := w.String(); !strings.Contains(got, "reqs=100") || !strings.Contains(got, "errs=503:9,net:1") {
		t.Errorf("String() = %q", got)
	}

	// The next window holds only what came after.
	s.record(5*time.Millisecond, 403)
	w = s.Window(&prev)
	if w.Requests != 1 || w.Status(403) != 1 || w.Status(200) != 0 || w.Max != 5*time.Millisecond {
		t.Errorf("second window: reqs=%d 403=%d 200=%d max=%v", w.Requests, w.Status(403), w.Status(200), w.Max)
	}
	if total := s.Snapshot(); total.Requests != 101 || total.Max != 100*time.Millisecond {
		t.Errorf("cumulative: reqs=%d max=%v", total.Requests, total.Max)
	}
}

func TestFastStatusErrorDetail(t *testing.T) {
	status := http.StatusForbidden
	body := `<?xml version="1.0"?><Error><Code>AccessDenied</Code><Message>Access Denied</Message><Resource>b</Resource><RequestId>1708100711ec3</RequestId></Error>`
	fc, _ := newTestFast(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(status)
		io.WriteString(w, body)
	})
	list := func() error {
		var page ListPage
		return fc.ListPage(context.Background(), "b", &ListQuery{MaxKeys: 10}, &page)
	}

	err := list()
	var se *S3Error
	if !errors.As(err, &se) || se.Status != 403 || se.Code != "AccessDenied" || se.RequestID != "1708100711ec3" {
		t.Fatalf("403: got %#v (%v)", se, err)
	}
	if want := "HTTP 403: s3 error AccessDenied: Access Denied (request_id=1708100711ec3)"; err.Error() != want {
		t.Errorf("message = %q, want %q", err.Error(), want)
	}
	if !Permanent(err) {
		t.Error("403 must be permanent")
	}

	// A 403 without an S3 body (a proxy, say) is still a 403.
	body = "<html>forbidden</html>"
	if err := list(); !Permanent(err) || !strings.Contains(err.Error(), "HTTP 403") {
		t.Errorf("bare 403: permanent=%v err=%v", Permanent(err), err)
	}

	status = http.StatusServiceUnavailable
	body = `<Error><Code>SlowDown</Code><Message>busy</Message></Error>`
	if err := list(); Permanent(err) {
		t.Errorf("503 must be retryable: %v", err)
	}

	// VAST echoes start-after unescaped: a '<' in it breaks the XML.
	status = http.StatusOK
	body = `<ListBucketResult><StartAfter>p/f<x</StartAfter><IsTruncated>false</IsTruncated></ListBucketResult>`
	if err := list(); err == nil || Permanent(err) {
		t.Errorf("malformed 200 must be a retryable error: %v", err)
	}

	snap := fc.ListStats().Snapshot()
	if snap.Requests != 4 || snap.Status(403) != 2 || snap.Status(503) != 1 || snap.Unparsed != 1 {
		t.Errorf("list stats: reqs=%d 403=%d 503=%d unparsed=%d",
			snap.Requests, snap.Status(403), snap.Status(503), snap.Unparsed)
	}
	if got := snap.String(); !strings.Contains(got, "errs=403:2,503:1,unparsed:1") {
		t.Errorf("String() = %q", got)
	}
	if fc.TagStats().Snapshot().Requests != 0 {
		t.Error("list requests leaked into tag stats")
	}
}

func TestFastTransportErrorNotPermanent(t *testing.T) {
	fc, srv := newTestFast(t, func(w http.ResponseWriter, r *http.Request) {})
	srv.Close()
	var page ListPage
	err := fc.ListPage(context.Background(), "b", &ListQuery{MaxKeys: 10}, &page)
	if err == nil || Permanent(err) {
		t.Errorf("connection refused: err=%v permanent=%v", err, Permanent(err))
	}
	if snap := fc.ListStats().Snapshot(); snap.NetErrors != 1 {
		t.Errorf("net errors = %d, want 1", snap.NetErrors)
	}
}
