package worker

import (
	"context"
	"errors"
	"io"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blake-golliher/s3lister/internal/model"
)

func newTestTagger(in chan []model.ObjectRecord, out chan []model.ObjectRecord,
	fetch func(ctx context.Context, key string) (map[string]string, error)) *TaggerPool {
	tp := NewTaggerPool(nil, "bucket", 4, in, out, log.New(io.Discard, "", 0))
	tp.attempts = 3
	tp.backoffMin = time.Millisecond
	tp.backoffMax = 4 * time.Millisecond
	tp.fetchOne = fetch
	return tp
}

// drain collects every record forwarded to out until the tagger closes it.
func drain(out chan []model.ObjectRecord) map[string]model.ObjectRecord {
	got := make(map[string]model.ObjectRecord)
	for batch := range out {
		for _, r := range batch {
			got[r.Key] = r
		}
	}
	return got
}

func TestTaggerEnrichesBatches(t *testing.T) {
	in := make(chan []model.ObjectRecord, 4)
	out := make(chan []model.ObjectRecord, 4)
	tp := newTestTagger(in, out, func(_ context.Context, key string) (map[string]string, error) {
		if key == "tagged" {
			return map[string]string{"env": "prod"}, nil
		}
		return map[string]string{}, nil
	})

	in <- []model.ObjectRecord{{Key: "tagged"}, {Key: "untagged"}}
	in <- []model.ObjectRecord{{Key: "also-untagged"}}
	close(in)
	go tp.Run(context.Background())
	got := drain(out)

	if len(got) != 3 {
		t.Fatalf("forwarded %d records, want 3", len(got))
	}
	if got["tagged"].Tags["env"] != "prod" {
		t.Errorf("tagged record: %v", got["tagged"].Tags)
	}
	// Fetched-but-empty must be non-nil so it lands as {} not NULL.
	if got["untagged"].Tags == nil || len(got["untagged"].Tags) != 0 {
		t.Errorf("untagged record should carry a non-nil empty map, got %v", got["untagged"].Tags)
	}
	if tp.Tagged() != 3 || tp.TagErrors() != 0 {
		t.Errorf("counters: tagged=%d errors=%d, want 3/0", tp.Tagged(), tp.TagErrors())
	}
}

func TestTaggerRetriesTransientFailures(t *testing.T) {
	var calls atomic.Int64
	in := make(chan []model.ObjectRecord, 1)
	out := make(chan []model.ObjectRecord, 1)
	tp := newTestTagger(in, out, func(_ context.Context, key string) (map[string]string, error) {
		if calls.Add(1) <= 2 {
			return nil, errors.New("connection reset")
		}
		return map[string]string{"ok": "yes"}, nil
	})

	in <- []model.ObjectRecord{{Key: "flaky"}}
	close(in)
	go tp.Run(context.Background())
	got := drain(out)

	if got["flaky"].Tags["ok"] != "yes" {
		t.Errorf("expected tags after retries, got %v", got["flaky"].Tags)
	}
	if tp.TagErrors() != 0 {
		t.Errorf("transient failure must not count as a tag error, got %d", tp.TagErrors())
	}
}

func TestTaggerInterruptIsNotCountedAsFailure(t *testing.T) {
	in := make(chan []model.ObjectRecord, 1)
	out := make(chan []model.ObjectRecord, 1)
	tp := newTestTagger(in, out, func(ctx context.Context, key string) (map[string]string, error) {
		return nil, ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // scan already shutting down when the batch is processed

	in <- []model.ObjectRecord{{Key: "a"}, {Key: "b"}, {Key: "c"}}
	close(in)
	go tp.Run(ctx)
	got := drain(out)

	if len(got) != 3 {
		t.Fatalf("shutdown must still forward the batch, got %d records", len(got))
	}
	if tp.TagErrors() != 0 {
		t.Errorf("canceled fetches counted as tag errors: %d", tp.TagErrors())
	}
	for k, r := range got {
		if r.Tags != nil {
			t.Errorf("record %q should be forwarded untagged, got %v", k, r.Tags)
		}
	}
}

func TestTaggerCountsRetries(t *testing.T) {
	var calls atomic.Int64
	in := make(chan []model.ObjectRecord, 1)
	out := make(chan []model.ObjectRecord, 1)
	tp := newTestTagger(in, out, func(_ context.Context, key string) (map[string]string, error) {
		if calls.Add(1) <= 2 {
			return nil, errors.New("503 SlowDown")
		}
		return map[string]string{}, nil
	})

	in <- []model.ObjectRecord{{Key: "throttled"}}
	close(in)
	go tp.Run(context.Background())
	drain(out)

	if tp.Retries() != 2 {
		t.Errorf("retries = %d, want 2", tp.Retries())
	}
}

func TestTaggerNullTagsOnPermanentFailure(t *testing.T) {
	in := make(chan []model.ObjectRecord, 1)
	out := make(chan []model.ObjectRecord, 1)
	tp := newTestTagger(in, out, func(_ context.Context, key string) (map[string]string, error) {
		if key == "bad" {
			return nil, errors.New("access denied")
		}
		return map[string]string{}, nil
	})

	in <- []model.ObjectRecord{{Key: "bad"}, {Key: "good"}}
	close(in)
	go tp.Run(context.Background())
	got := drain(out)

	if len(got) != 2 {
		t.Fatalf("failed record must still be forwarded; got %d records", len(got))
	}
	if got["bad"].Tags != nil {
		t.Errorf("failed fetch must leave Tags nil (NULL in output), got %v", got["bad"].Tags)
	}
	if got["good"].Tags == nil {
		t.Errorf("good record lost its tags")
	}
	if tp.TagErrors() != 1 || tp.Tagged() != 1 {
		t.Errorf("counters: tagged=%d errors=%d, want 1/1", tp.Tagged(), tp.TagErrors())
	}
}
