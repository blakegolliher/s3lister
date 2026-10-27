package s3client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	v4 "github.com/aws/aws-sdk-go-v2/aws/signer/v4"
	"github.com/blake-golliher/s3lister/internal/config"
)

// FastClient issues the scan's two hot-path requests — ListObjectsV2 and
// GetObjectTagging — without the AWS SDK's middleware stack or encoding/xml.
// Requests are built and SigV4-signed directly and responses parsed by the
// purpose-built scanner in fastxml.go. Everything cold (HeadBucket,
// CreateBucket, PutObject) stays on the SDK client.
//
// It shares the same transport construction as the SDK client, including the
// round-robin dialer, so connection spreading across VIPs is identical.
type FastClient struct {
	http    *http.Client
	base    string // endpoint, no trailing slash (path-style addressing)
	region  string
	creds   aws.Credentials
	signer  *v4.Signer
	tagBufs sync.Pool

	listStats OpStats
	tagStats  OpStats
}

// sha256 of an empty payload — every request we send has no body.
const emptyPayloadSHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

// ListQuery holds the ListObjectsV2 parameters the scan uses.
type ListQuery struct {
	Prefix            string
	Delimiter         string
	StartAfter        string
	ContinuationToken string
	MaxKeys           int
}

// NewFast builds a FastClient from the same config the SDK client uses.
func NewFast(cfg *config.S3Config, verbose bool, logger *log.Logger) *FastClient {
	fc := &FastClient{
		http:   newHTTPClient(verbose, logger),
		base:   strings.TrimRight(cfg.Endpoint, "/"),
		region: cfg.Region,
		creds: aws.Credentials{
			AccessKeyID:     cfg.AccessKey,
			SecretAccessKey: cfg.SecretKey,
		},
		// S3 signs the URI path exactly as sent, without re-escaping.
		signer: v4.NewSigner(func(o *v4.SignerOptions) { o.DisableURIPathEscaping = true }),
	}
	fc.tagBufs.New = func() any { b := make([]byte, 0, 4096); return &b }
	return fc
}

// ListStats is the latency and status accounting for every ListPage request.
func (c *FastClient) ListStats() *OpStats { return &c.listStats }

// TagStats is the latency and status accounting for every GetTagging request.
func (c *FastClient) TagStats() *OpStats { return &c.tagStats }

// ListPage fetches one ListObjectsV2 page into page, reusing page's buffers.
// Retry policy belongs to the caller; Permanent says which errors not to retry.
func (c *FastClient) ListPage(ctx context.Context, bucket string, q *ListQuery, page *ListPage) error {
	var sb strings.Builder
	sb.Grow(len(c.base) + len(bucket) + len(q.Prefix)*3 + 96)
	sb.WriteString(c.base)
	sb.WriteByte('/')
	sb.WriteString(bucket)
	sb.WriteString("?list-type=2")
	if q.MaxKeys > 0 {
		sb.WriteString("&max-keys=")
		sb.WriteString(strconv.Itoa(q.MaxKeys))
	}
	if q.Prefix != "" {
		sb.WriteString("&prefix=")
		writeEscaped(&sb, q.Prefix, false)
	}
	if q.Delimiter != "" {
		sb.WriteString("&delimiter=")
		writeEscaped(&sb, q.Delimiter, false)
	}
	if q.StartAfter != "" {
		sb.WriteString("&start-after=")
		writeEscaped(&sb, q.StartAfter, false)
	}
	if q.ContinuationToken != "" {
		sb.WriteString("&continuation-token=")
		writeEscaped(&sb, q.ContinuationToken, false)
	}

	body, status, err := c.do(ctx, sb.String(), page.body, &c.listStats)
	page.body = body
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return statusError(status, body)
	}
	if err := parseListPage(body, page); err != nil {
		c.listStats.unparsed.Add(1)
		return err
	}
	return nil
}

// GetTagging fetches an object's tags. The returned map is non-nil on
// success, even when the object has no tags.
func (c *FastClient) GetTagging(ctx context.Context, bucket, key string) (map[string]string, error) {
	bp := c.tagBufs.Get().(*[]byte)
	defer c.tagBufs.Put(bp)

	var sb strings.Builder
	sb.Grow(len(c.base) + len(bucket) + len(key)*3 + 16)
	sb.WriteString(c.base)
	sb.WriteByte('/')
	sb.WriteString(bucket)
	sb.WriteByte('/')
	writeEscaped(&sb, key, true)
	sb.WriteString("?tagging=")

	body, status, err := c.do(ctx, sb.String(), (*bp)[:0], &c.tagStats)
	*bp = body
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, statusError(status, body)
	}
	tags, err := parseTagging(body)
	if err != nil {
		c.tagStats.unparsed.Add(1)
	}
	return tags, err
}

// do signs and executes a body-less GET, reading the response into buf, and
// records the request's latency (through the last body byte) and outcome in
// stats. Requests cut short by cancellation are not recorded.
func (c *FastClient) do(ctx context.Context, url string, buf []byte, stats *OpStats) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return buf, 0, err
	}
	req.Header.Set("X-Amz-Content-Sha256", emptyPayloadSHA)
	if err := c.signer.SignHTTP(ctx, c.creds, req, emptyPayloadSHA, "s3", c.region, time.Now().UTC()); err != nil {
		return buf, 0, fmt.Errorf("sign request: %w", err)
	}

	start := time.Now()
	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() == nil {
			stats.record(time.Since(start), 0)
		}
		return buf, 0, err
	}
	defer resp.Body.Close()
	buf, err = readInto(buf[:0], resp.Body)
	if ctx.Err() == nil {
		status := resp.StatusCode
		if err != nil {
			status = 0 // the response never arrived whole
		}
		stats.record(time.Since(start), status)
	}
	return buf, resp.StatusCode, err
}

// S3Error is an S3 error response. Status is 0 when the <Error> body came
// with HTTP 200. RequestID is what the endpoint's own logs are keyed by, so
// it travels with every failure the scan reports.
type S3Error struct {
	Status    int
	Code      string
	Message   string
	RequestID string
}

func (e *S3Error) Error() string {
	var sb strings.Builder
	if e.Status != 0 {
		fmt.Fprintf(&sb, "HTTP %d: ", e.Status)
	}
	if e.Code != "" {
		fmt.Fprintf(&sb, "s3 error %s: %s", e.Code, e.Message)
	} else {
		sb.WriteString(e.Message)
	}
	if e.RequestID != "" {
		fmt.Fprintf(&sb, " (request_id=%s)", e.RequestID)
	}
	return sb.String()
}

// Permanent reports whether err is a refusal that retrying cannot fix. Only
// HTTP 403 qualifies: the endpoint understood the request and said no —
// AccessDenied, but also bad credentials or a skewed clock, none of which
// clear up inside a retry budget. Everything else (5xx, throttling, timeouts,
// connection resets) is treated as transient.
func Permanent(err error) bool {
	var se *S3Error
	return errors.As(err, &se) && se.Status == http.StatusForbidden
}

func statusError(status int, body []byte) error {
	se := parseErrorBody(body)
	if se == nil {
		se = &S3Error{Message: fmt.Sprintf("unexpected response body %q", bodyHead(body))}
	}
	se.Status = status
	return se
}

// readInto reads all of r into buf, growing it as needed and returning the
// (possibly reallocated) buffer so callers can reuse it across requests.
func readInto(buf []byte, r io.Reader) ([]byte, error) {
	for {
		if len(buf) == cap(buf) {
			buf = append(buf, 0)[:len(buf)]
		}
		n, err := r.Read(buf[len(buf):cap(buf)])
		buf = buf[:len(buf)+n]
		if err == io.EOF {
			return buf, nil
		}
		if err != nil {
			return buf, err
		}
	}
}

const upperhex = "0123456789ABCDEF"

// writeEscaped writes s percent-encoded per RFC 3986: every byte outside the
// unreserved set is escaped (plus '/' kept literal in path mode). Strict
// encoding means the bytes on the wire equal SigV4's canonical form, so the
// signature can never disagree with what the server canonicalizes.
func writeEscaped(sb *strings.Builder, s string, keepSlash bool) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9',
			c == '-' || c == '.' || c == '_' || c == '~':
			sb.WriteByte(c)
		case c == '/' && keepSlash:
			sb.WriteByte(c)
		default:
			sb.WriteByte('%')
			sb.WriteByte(upperhex[c>>4])
			sb.WriteByte(upperhex[c&0xF])
		}
	}
}
