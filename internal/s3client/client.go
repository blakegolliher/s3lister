package s3client

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/blake-golliher/s3lister/internal/config"
)

func New(cfg *config.S3Config, verbose bool, logger *log.Logger) (*s3.Client, error) {
	transport := &http.Transport{
		MaxIdleConns:          500,
		MaxIdleConnsPerHost:   500,
		IdleConnTimeout:       90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		TLSHandshakeTimeout:  10 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		DisableCompression:    true,
	}

	var httpTransport http.RoundTripper = transport
	if verbose {
		httpTransport = &loggingTransport{inner: transport, logger: logger}
	}

	httpClient := &http.Client{
		Transport: httpTransport,
		Timeout:   120 * time.Second,
	}

	creds := credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")

	client := s3.New(s3.Options{
		Region:       cfg.Region,
		Credentials:  creds,
		HTTPClient:   httpClient,
		BaseEndpoint: aws.String(cfg.Endpoint),
		// Path-style required for most non-AWS S3 endpoints (MinIO, VAST, Ceph, etc)
		UsePathStyle: true,
	})

	if verbose {
		logger.Printf("[s3] connecting to endpoint=%s region=%s bucket=%s path_style=true",
			cfg.Endpoint, cfg.Region, cfg.Bucket)
	}

	// HeadBucket instead of ListBuckets — works on endpoints that don't
	// support ListBuckets and only requires access to the configured bucket
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if verbose {
		logger.Printf("[s3] checking bucket access: HeadBucket %q", cfg.Bucket)
	}

	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(cfg.Bucket),
	})
	if err != nil {
		return nil, fmt.Errorf("S3 connectivity check failed (endpoint=%s bucket=%s): %w",
			cfg.Endpoint, cfg.Bucket, err)
	}

	if verbose {
		logger.Printf("[s3] bucket %q accessible, connection OK", cfg.Bucket)
	}

	return client, nil
}

// loggingTransport wraps an http.RoundTripper and logs requests in verbose mode.
type loggingTransport struct {
	inner  http.RoundTripper
	logger *log.Logger
}

func (t *loggingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	start := time.Now()
	t.logger.Printf("[http] -> %s %s", req.Method, req.URL)

	resp, err := t.inner.RoundTrip(req)

	elapsed := time.Since(start)
	if err != nil {
		t.logger.Printf("[http] <- %s %s ERROR %v (%v)", req.Method, req.URL, err, elapsed)
	} else {
		size := resp.ContentLength
		if size < 0 {
			// Try reading the body length hint
			size = 0
		}
		t.logger.Printf("[http] <- %s %s %d (%v, %d bytes)", req.Method, req.URL, resp.StatusCode, elapsed, size)
	}
	return resp, err
}

// Ensure loggingTransport implements RoundTripper
var _ http.RoundTripper = (*loggingTransport)(nil)

// discardLogger returns a logger that writes nowhere (for Pebble).
func DiscardLogger() *log.Logger {
	return log.New(io.Discard, "", 0)
}
