package store

import (
	"encoding/binary"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble"
)

// pebbleLogger adapts a stdlib *log.Logger to pebble.Logger interface.
type pebbleLogger struct {
	l *log.Logger
}

func (p *pebbleLogger) Infof(format string, args ...interface{})  { p.l.Printf(format, args...) }
func (p *pebbleLogger) Errorf(format string, args ...interface{}) { p.l.Printf("ERROR: "+format, args...) }
func (p *pebbleLogger) Fatalf(format string, args ...interface{}) { p.l.Fatalf(format, args...) }

// ObjectRecord is what gets written to Pebble.
type ObjectRecord struct {
	Key          string
	Size         int64
	LastModified time.Time
}

// Store wraps Pebble with batch write support.
type Store struct {
	db      *pebble.DB
	written atomic.Int64
}

func Open(path string, logger *log.Logger) (*Store, error) {
	opts := &pebble.Options{
		// Performance tuning for write-heavy workload
		MemTableSize:                128 * 1024 * 1024, // 128MB memtable
		MemTableStopWritesThreshold: 4,
		MaxConcurrentCompactions:    func() int { return 4 },
		L0CompactionThreshold:       8,
		L0StopWritesThreshold:       36,
		// Disable WAL for max throughput (we can re-scan S3 on crash)
		DisableWAL: true,
		// Suppress Pebble's internal logging unless we provide one
		Logger: &pebbleLogger{l: logger},
	}

	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db at %s: %w", path, err)
	}

	return &Store{db: db}, nil
}

// WriteBatch writes a batch of records to Pebble.
// Key format: object key (string)
// Value format: [8 bytes size LE][8 bytes unix timestamp nanos LE]
func (s *Store) WriteBatch(records []ObjectRecord) error {
	batch := s.db.NewBatch()
	defer batch.Close()

	for i := range records {
		key := []byte(records[i].Key)
		val := make([]byte, 16)
		binary.LittleEndian.PutUint64(val[0:8], uint64(records[i].Size))
		binary.LittleEndian.PutUint64(val[8:16], uint64(records[i].LastModified.UnixNano()))
		if err := batch.Set(key, val, nil); err != nil {
			return fmt.Errorf("pebble batch set failed: %w", err)
		}
	}

	if err := batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("pebble batch commit failed: %w", err)
	}

	s.written.Add(int64(len(records)))
	return nil
}

// Written returns total records written.
func (s *Store) Written() int64 {
	return s.written.Load()
}

// OpenReadOnly opens the Pebble database in read-only mode for export.
func OpenReadOnly(path string, logger *log.Logger) (*Store, error) {
	opts := &pebble.Options{
		ReadOnly: true,
		Logger:   &pebbleLogger{l: logger},
	}
	db, err := pebble.Open(path, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to open pebble db (read-only) at %s: %w", path, err)
	}
	return &Store{db: db}, nil
}

// Iterate calls fn for every record in the database. Iteration is in key order.
func (s *Store) Iterate(fn func(rec ObjectRecord) error) error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return fmt.Errorf("failed to create iterator: %w", err)
	}
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		val := iter.Value()
		if len(val) < 16 {
			continue
		}
		size := int64(binary.LittleEndian.Uint64(val[0:8]))
		nanos := int64(binary.LittleEndian.Uint64(val[8:16]))

		rec := ObjectRecord{
			Key:          string(iter.Key()),
			Size:         size,
			LastModified: time.Unix(0, nanos),
		}
		if err := fn(rec); err != nil {
			return err
		}
	}

	return iter.Error()
}

func (s *Store) Close() {
	s.db.Close()
}
