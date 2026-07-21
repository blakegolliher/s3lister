// Package model holds the shared record type that flows from the S3 reader
// pool to the Parquet writer pool. It deliberately carries no storage
// dependencies so both sides can import it without coupling.
package model

import "time"

// ObjectRecord is a single S3 object captured during listing. Readers emit
// these in per-page batches over a channel; writers convert them into Parquet
// rows. Keep this struct small — billions of them flow through the pipeline.
type ObjectRecord struct {
	Key          string
	Size         int64
	LastModified time.Time
	ETag         string
	StorageClass string

	// Tags is nil when tag collection is off (or the fetch failed) and
	// non-nil — possibly empty — when GetObjectTagging succeeded. The
	// nil/empty distinction is meaningful and flows through to the output.
	Tags map[string]string
}
