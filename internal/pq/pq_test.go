package pq

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blake-golliher/s3lister/internal/model"
	"github.com/parquet-go/parquet-go"
)

func TestWriteReadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "part-000.parquet")

	scanTS := time.Date(2026, 7, 14, 12, 0, 0, 0, time.UTC)
	lm := time.Date(2025, 1, 2, 3, 4, 5, 0, time.UTC)

	recs := []model.ObjectRecord{
		{Key: "data/2024/report.csv", Size: 100, LastModified: lm, ETag: `"abc123"`, StorageClass: "STANDARD"},
		{Key: "top.txt", Size: 5, LastModified: lm, ETag: "noquotes", StorageClass: ""},
		{Key: "a/b/c/.hidden", Size: 0, LastModified: lm},
		{Key: "logs/app.log.gz", Size: 999, LastModified: lm, StorageClass: "GLACIER"},
	}

	w, err := NewWriter(path, "scan-test", scanTS)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	n, err := w.Append(recs)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if n != len(recs) {
		t.Fatalf("Append wrote %d, want %d", n, len(recs))
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Read back.
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	r := parquet.NewGenericReader[Row](pf)
	defer r.Close()
	got := make([]Row, len(recs))
	rn, err := r.Read(got)
	if rn != len(recs) {
		t.Fatalf("read %d rows, want %d (err=%v)", rn, len(recs), err)
	}

	// Verify derived columns on the first record.
	want := Row{
		Key: "data/2024/report.csv", ObjectName: "report.csv", Extension: "csv",
		ParentPrefix: "data/2024", Depth: 2, SizeBytes: 100,
		ETag: "abc123", StorageClass: "STANDARD", ScanID: "scan-test",
	}
	g := got[0]
	if g.Key != want.Key || g.ObjectName != want.ObjectName || g.Extension != want.Extension ||
		g.ParentPrefix != want.ParentPrefix || g.Depth != want.Depth || g.SizeBytes != want.SizeBytes {
		t.Errorf("derived fields mismatch:\n got %+v\nwant %+v", g, want)
	}
	if g.ETag != "abc123" {
		t.Errorf("etag quotes not stripped: %q", g.ETag)
	}
	if !g.LastModified.Equal(lm) {
		t.Errorf("last_modified round-trip: got %v want %v", g.LastModified.UTC(), lm)
	}
	if !g.ScanTimestamp.Equal(scanTS) {
		t.Errorf("scan_timestamp round-trip: got %v want %v", g.ScanTimestamp.UTC(), scanTS)
	}

	// Top-level object: no parent, extension txt.
	if got[1].ParentPrefix != "" || got[1].ObjectName != "top.txt" || got[1].Depth != 0 {
		t.Errorf("top-level record wrong: %+v", got[1])
	}
	// Dotfile ".hidden" has no extension (leading dot only).
	if got[2].Extension != "" || got[2].ObjectName != ".hidden" || got[2].Depth != 3 {
		t.Errorf("dotfile record wrong: %+v", got[2])
	}
	// Multi-dot: app.log.gz -> extension gz.
	if got[3].Extension != "gz" || got[3].StorageClass != "GLACIER" {
		t.Errorf("multi-dot record wrong: %+v", got[3])
	}
}

func TestTagsThreeState(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "part-000.parquet")

	recs := []model.ObjectRecord{
		{Key: "a/no-collect"},                       // tags not collected -> NULL / -1
		{Key: "a/no-tags", Tags: map[string]string{}}, // collected, none -> 0
		{Key: "a/tagged", Tags: map[string]string{"env": "prod", "team": "storage"}},
	}

	w, err := NewWriter(path, "scan-test", time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.Append(recs); err != nil {
		t.Fatalf("Append: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}

	r := parquet.NewGenericReader[Row](pf)
	defer r.Close()
	got := make([]Row, len(recs))
	if rn, err := r.Read(got); rn != len(recs) {
		t.Fatalf("read %d rows, want %d (err=%v)", rn, len(recs), err)
	}

	if got[0].TagCount != -1 || len(got[0].Tags) != 0 {
		t.Errorf("not-collected row: tag_count=%d tags=%v, want -1 and empty", got[0].TagCount, got[0].Tags)
	}
	if got[1].TagCount != 0 || len(got[1].Tags) != 0 {
		t.Errorf("no-tags row: tag_count=%d tags=%v, want 0 and empty", got[1].TagCount, got[1].Tags)
	}
	if got[2].TagCount != 2 || got[2].Tags["env"] != "prod" || got[2].Tags["team"] != "storage" {
		t.Errorf("tagged row round-trip wrong: tag_count=%d tags=%v", got[2].TagCount, got[2].Tags)
	}
}

func TestRowGroupFlushBoundsMemory(t *testing.T) {
	// Write more than one row group's worth to exercise the auto-flush path.
	dir := t.TempDir()
	path := filepath.Join(dir, "part-000.parquet")
	w, err := NewWriter(path, "scan-test", time.Unix(0, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}

	const total = 2500
	batch := make([]model.ObjectRecord, 500)
	written := 0
	for i := 0; i < total/len(batch); i++ {
		for j := range batch {
			batch[j] = model.ObjectRecord{Key: "k/obj", Size: int64(j)}
		}
		n, err := w.Append(batch)
		if err != nil {
			t.Fatalf("Append: %v", err)
		}
		written += n
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if written != total {
		t.Fatalf("wrote %d want %d", written, total)
	}

	f, _ := os.Open(path)
	defer f.Close()
	fi, _ := f.Stat()
	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		t.Fatal(err)
	}
	if pf.NumRows() != total {
		t.Fatalf("parquet NumRows=%d want %d", pf.NumRows(), total)
	}
}
