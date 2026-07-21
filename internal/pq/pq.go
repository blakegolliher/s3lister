// Package pq streams S3 object records into Parquet files that DuckDB (and any
// Arrow/Parquet reader) can query directly — no export step required.
//
// Each writer owns exactly one output file, so a pool of writers produces
// part-000.parquet, part-001.parquet, ... with zero cross-writer contention.
// DuckDB reads the whole set as a single table:
//
//	SELECT count(*), sum(size_bytes) FROM 'out/*.parquet';
package pq

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/blake-golliher/s3lister/internal/model"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

// rowsPerRowGroup bounds each row group (and therefore the writer's in-memory
// footprint) to a fixed row count. The writer auto-flushes a row group on reach
// so memory stays flat even across billions of objects. Matches nfs-walker.
const rowsPerRowGroup = 1_000_000

// Row is the on-disk Parquet schema — an analytics-ready, columnar layout
// modeled on nfs-walker. Derived columns (object_name/extension/parent_prefix/
// depth) are precomputed so DuckDB queries get predicate pushdown without
// per-row string parsing. Timestamps are UTC-normalized TIMESTAMP(MICROS),
// which DuckDB reads as native TIMESTAMP.
type Row struct {
	Key           string    `parquet:"key"`
	ObjectName    string    `parquet:"object_name"`
	Extension     string    `parquet:"extension"`
	ParentPrefix  string    `parquet:"parent_prefix"`
	Depth         int32     `parquet:"depth"`
	SizeBytes     int64     `parquet:"size_bytes"`
	LastModified  time.Time `parquet:"last_modified,timestamp(microsecond)"`
	ETag          string    `parquet:"etag"`
	StorageClass  string    `parquet:"storage_class"`
	// Tags is a Parquet MAP column filled by the opt-in tag-collection stage.
	// TagCount disambiguates its three states: -1 = tags were not collected
	// for this row (no -tags flag, or the fetch failed after retries),
	// 0 = collected and the object has none, N = collected with N tags.
	Tags          map[string]string `parquet:"tags,optional"`
	TagCount      int32             `parquet:"tag_count"`
	ScanID        string            `parquet:"scan_id"`
	ScanTimestamp time.Time         `parquet:"scan_timestamp,timestamp(microsecond)"`
}

// Writer wraps a single Parquet output file. It is NOT safe for concurrent use;
// give each goroutine its own Writer (that is the whole point — one file per
// writer means no shared lock).
type Writer struct {
	f      *os.File
	bw     *bufio.Writer
	pw     *parquet.GenericWriter[Row]
	scanID string
	scanTS time.Time
	buf    []Row // reused across batches to avoid per-batch allocation
}

// NewWriter creates a Parquet file at path using zstd (level 3) compression.
func NewWriter(path, scanID string, scanTS time.Time) (*Writer, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("create parquet file %s: %w", path, err)
	}
	// A large buffered writer keeps the underlying file IO in big sequential
	// chunks; the Parquet writer only needs a plain io.Writer (no seeking).
	bw := bufio.NewWriterSize(f, 4<<20)

	// One encoder core per writer — we already parallelize at the pool level,
	// so letting each codec spawn its own worker pool would oversubscribe CPU.
	codec := &zstd.Codec{Level: zstd.SpeedDefault, Concurrency: 1}

	pw := parquet.NewGenericWriter[Row](bw,
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(rowsPerRowGroup),
	)

	return &Writer{f: f, bw: bw, pw: pw, scanID: scanID, scanTS: scanTS}, nil
}

// Append converts a batch of object records into Parquet rows and writes them.
// Returns the number of rows written.
func (w *Writer) Append(recs []model.ObjectRecord) (int, error) {
	if cap(w.buf) < len(recs) {
		w.buf = make([]Row, len(recs))
	} else {
		w.buf = w.buf[:len(recs)]
	}
	for i := range recs {
		w.buf[i] = deriveRow(&recs[i], w.scanID, w.scanTS)
	}
	return w.pw.Write(w.buf)
}

// Close flushes the final row group, writes the Parquet footer, and closes the
// underlying file.
func (w *Writer) Close() error {
	if err := w.pw.Close(); err != nil {
		w.f.Close()
		return fmt.Errorf("close parquet writer: %w", err)
	}
	if err := w.bw.Flush(); err != nil {
		w.f.Close()
		return fmt.Errorf("flush buffer: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}
	return nil
}

// deriveRow computes the analytics columns from an object key. This runs on the
// (CPU-bound) writer side; it is cheap string slicing next to zstd compression.
func deriveRow(r *model.ObjectRecord, scanID string, scanTS time.Time) Row {
	name := r.Key
	parent := ""
	if idx := strings.LastIndexByte(r.Key, '/'); idx >= 0 {
		name = r.Key[idx+1:]
		parent = r.Key[:idx]
	}

	ext := ""
	// dot > 0 so dotfiles like ".bashrc" are not treated as all-extension.
	if dot := strings.LastIndexByte(name, '.'); dot > 0 {
		ext = name[dot+1:]
	}

	tagCount := int32(-1)
	if r.Tags != nil {
		tagCount = int32(len(r.Tags))
	}

	return Row{
		Key:           r.Key,
		ObjectName:    name,
		Extension:     ext,
		ParentPrefix:  parent,
		Depth:         int32(strings.Count(r.Key, "/")),
		SizeBytes:     r.Size,
		LastModified:  r.LastModified,
		ETag:          strings.Trim(r.ETag, `"`),
		StorageClass:  r.StorageClass,
		Tags:          r.Tags,
		TagCount:      tagCount,
		ScanID:        scanID,
		ScanTimestamp: scanTS,
	}
}
