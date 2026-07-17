package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"syscall"
	"time"

	"github.com/blake-golliher/s3lister/internal/config"
	"github.com/blake-golliher/s3lister/internal/model"
	"github.com/blake-golliher/s3lister/internal/pq"
	"github.com/blake-golliher/s3lister/internal/progress"
	"github.com/blake-golliher/s3lister/internal/s3client"
	"github.com/blake-golliher/s3lister/internal/worker"
	"github.com/parquet-go/parquet-go"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
	}
	switch os.Args[1] {
	case "scan":
		runScan(os.Args[2:])
	case "export-csv":
		runExportCSV(os.Args[2:])
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", os.Args[1])
		printUsage()
	}
}

func printUsage() {
	fmt.Fprintf(os.Stderr, `Usage: s3lister <command> [options]

Commands:
  scan         Scan S3 bucket and write Parquet files (DuckDB-queryable)
  export-csv   Export the Parquet dataset to a single CSV file

Run 's3lister <command> -help' for command-specific options.
`)
	os.Exit(1)
}

func fatal(logger *log.Logger, format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	if logger != nil {
		logger.Printf("FATAL: %s", msg)
	}
	fmt.Fprintf(os.Stderr, "FATAL: %s\n", msg)
	os.Exit(1)
}

// setupLogger creates the file logger and optionally a verbose logger that
// tees to both the file and stderr.
func setupLogger(logPath string, verbose bool) (*log.Logger, *log.Logger, *os.File) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fatal(nil, "cannot open log file %s: %v", logPath, err)
	}

	flags := log.LstdFlags | log.Lmicroseconds
	fileLogger := log.New(logFile, "", flags)

	if verbose {
		tee := io.MultiWriter(logFile, os.Stderr)
		verboseLogger := log.New(tee, "", flags)
		return verboseLogger, fileLogger, logFile
	}

	return fileLogger, fileLogger, logFile
}

func runScan(args []string) {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	configPath := fs.String("config", "config.toml", "path to config file")
	readers := fs.Int("readers", 0, "override number of reader threads")
	writers := fs.Int("writers", 0, "override number of writer threads")
	bucket := fs.String("bucket", "", "override bucket from config")
	output := fs.String("output", "", "override output directory from config")
	verbose := fs.Bool("verbose", false, "verbose output: log to stderr and trace HTTP requests")
	fs.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		fatal(nil, "%v", err)
	}
	if *readers > 0 {
		cfg.Workers.Readers = *readers
	}
	if *writers > 0 {
		cfg.Workers.Writers = *writers
	}
	if *bucket != "" {
		cfg.S3.Bucket = *bucket
	}
	if *output != "" {
		cfg.Storage.OutputDir = *output
	}

	logger, _, logFile := setupLogger(cfg.Logging.LogFile, *verbose)
	defer logFile.Close()

	logger.Printf("=== s3lister scan starting ===")
	logger.Printf("config: bucket=%s prefix=%q endpoint=%s readers=%d writers=%d queue=%d output=%s",
		cfg.S3.Bucket, cfg.S3.Prefix, cfg.S3.Endpoint,
		cfg.Workers.Readers, cfg.Workers.Writers, cfg.Workers.QueueSize, cfg.Storage.OutputDir)

	totalStart := time.Now()
	scanID := fmt.Sprintf("scan-%s", totalStart.UTC().Format("20060102T150405Z"))
	scanTS := totalStart.UTC()

	// Prepare a clean output directory for this scan's Parquet part files.
	if err := os.MkdirAll(cfg.Storage.OutputDir, 0755); err != nil {
		fatal(logger, "cannot create output dir %s: %v", cfg.Storage.OutputDir, err)
	}
	if err := clearParquetParts(cfg.Storage.OutputDir); err != nil {
		fatal(logger, "cannot clear old parquet parts in %s: %v", cfg.Storage.OutputDir, err)
	}

	client, err := s3client.New(&cfg.S3, *verbose, logger)
	if err != nil {
		fatal(logger, "%v", err)
	}
	logger.Printf("s3 connected in %v (scan_id=%s)", time.Since(totalStart), scanID)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)
	go func() {
		sig := <-sigCh
		logger.Printf("received %v, shutting down", sig)
		fmt.Fprintf(os.Stderr, "\nReceived %v, shutting down...\n", sig)
		cancel()
	}()

	// The channel now carries per-page record batches rather than single
	// records, so its capacity is measured in batches. Derive it from the
	// configured (record-oriented) queue size, keeping enough slack for every
	// reader to stay unblocked.
	chanCap := cfg.Workers.QueueSize / 1000
	if min := cfg.Workers.Readers * 4; chanCap < min {
		chanCap = min
	}
	if chanCap < 256 {
		chanCap = 256
	}
	recordCh := make(chan []model.ObjectRecord, chanCap)

	writerPool := worker.NewWriterPool(cfg.Storage.OutputDir, scanID, scanTS, recordCh, cfg.Workers.Writers, logger)
	writerDone := make(chan struct{})
	go func() {
		writerPool.Run()
		close(writerDone)
	}()

	readerPool := worker.NewReaderPool(client, cfg.S3.Bucket, cfg.S3.Prefix, cfg.Workers.Readers, recordCh, logger)

	// Progress: animate the bar on a fast ticker, log a status line every 5s.
	meter := progress.New()
	stopProgress := make(chan struct{})
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		logTick := time.NewTicker(5 * time.Second)
		defer logTick.Stop()
		for {
			select {
			case <-stopProgress:
				return
			case <-tick.C:
				meter.Render(readerPool.Listed(), writerPool.Written())
			case <-logTick.C:
				listed := readerPool.Listed()
				written := writerPool.Written()
				elapsed := time.Since(totalStart)
				logger.Printf("[progress] listed=%d written=%d queued=%d rate=%.0f/s elapsed=%v",
					listed, written, listed-written, float64(written)/elapsed.Seconds(),
					elapsed.Round(time.Millisecond))
			}
		}
	}()

	listStart := time.Now()
	if err := readerPool.Run(ctx); err != nil {
		logger.Printf("reader pool error: %v", err)
		fmt.Fprintf(os.Stderr, "\nReader error: %v\n", err)
	}
	close(recordCh)
	logger.Printf("listing done in %v, flushing writers...", time.Since(listStart))

	<-writerDone
	close(stopProgress)
	meter.Clear()

	// Final statistics.
	totalElapsed := time.Since(totalStart)
	totalObjects := writerPool.Written()
	writeErrors := writerPool.Errors()
	avgRate := float64(totalObjects) / totalElapsed.Seconds()
	peakRate := meter.PeakRate()
	outBytes, outFiles := dirSize(cfg.Storage.OutputDir)

	logger.Printf("=== scan complete ===")
	logger.Printf("stats: scan_id=%s objects=%d errors=%d elapsed=%v avg_rate=%.0f/s peak_rate=%.0f/s",
		scanID, totalObjects, writeErrors, totalElapsed, avgRate, peakRate)
	logger.Printf("stats: readers=%d writers=%d output_files=%d output_bytes=%d (%s) bytes_per_object=%.1f",
		cfg.Workers.Readers, cfg.Workers.Writers, outFiles, outBytes, humanBytes(outBytes),
		safeDiv(float64(outBytes), float64(totalObjects)))

	fmt.Fprintf(os.Stderr, "\nDone! %d objects in %v\n", totalObjects, totalElapsed.Round(time.Millisecond))
	fmt.Fprintf(os.Stderr, "  avg %.0f/s   peak %.0f/s\n", avgRate, peakRate)
	fmt.Fprintf(os.Stderr, "  output: %s  (%d files, %s)\n", cfg.Storage.OutputDir, outFiles, humanBytes(outBytes))
	if writeErrors > 0 {
		fmt.Fprintf(os.Stderr, "  WARNING: %d write errors occurred, check log\n", writeErrors)
	}
	fmt.Fprintf(os.Stderr, "  log: %s\n", cfg.Logging.LogFile)
	fmt.Fprintf(os.Stderr, "\nQuery it with DuckDB:\n")
	fmt.Fprintf(os.Stderr, "  duckdb -c \"SELECT count(*), sum(size_bytes) FROM '%s/*.parquet'\"\n",
		cfg.Storage.OutputDir)
}

// clearParquetParts removes any part-*.parquet files left from a previous scan
// so the new dataset is not mixed with stale rows.
func clearParquetParts(dir string) error {
	matches, err := filepath.Glob(filepath.Join(dir, "part-*.parquet"))
	if err != nil {
		return err
	}
	for _, m := range matches {
		if err := os.Remove(m); err != nil {
			return err
		}
	}
	return nil
}

// dirSize returns total bytes and file count of the parquet parts in dir.
func dirSize(dir string) (bytes int64, files int) {
	matches, _ := filepath.Glob(filepath.Join(dir, "part-*.parquet"))
	for _, m := range matches {
		if fi, err := os.Stat(m); err == nil {
			bytes += fi.Size()
			files++
		}
	}
	return bytes, files
}

func safeDiv(a, b float64) float64 {
	if b == 0 {
		return 0
	}
	return a / b
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	div, exp := int64(unit), 0
	for x := n / unit; x >= unit; x /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %ciB", float64(n)/float64(div), "KMGTPE"[exp])
}

func runExportCSV(args []string) {
	fs := flag.NewFlagSet("export-csv", flag.ExitOnError)
	inDir := fs.String("in", "./s3lister_out", "path to the Parquet output directory")
	outPath := fs.String("out", "s3objects.csv", "output CSV file path")
	fs.Parse(args)

	matches, err := filepath.Glob(filepath.Join(*inDir, "part-*.parquet"))
	if err != nil {
		fatal(nil, "glob failed: %v", err)
	}
	if len(matches) == 0 {
		fatal(nil, "no part-*.parquet files found in %s", *inDir)
	}
	sort.Strings(matches)
	fmt.Fprintf(os.Stderr, "Exporting %d parquet file(s) from %s\n", len(matches), *inDir)

	f, err := os.Create(*outPath)
	if err != nil {
		fatal(nil, "cannot create %s: %v", *outPath, err)
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 4*1024*1024)
	w := csv.NewWriter(bw)
	w.Write([]string{"key", "size_bytes", "last_modified", "storage_class", "etag"})

	var count int64
	start := time.Now()
	lastReport := start

	for _, part := range matches {
		if err := exportPart(part, w, &count, start, &lastReport); err != nil {
			fatal(nil, "export failed on %s: %v", part, err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		fatal(nil, "csv flush error: %v", err)
	}
	if err := bw.Flush(); err != nil {
		fatal(nil, "buffer flush error: %v", err)
	}

	rate := float64(count) / time.Since(start).Seconds()
	fmt.Fprintf(os.Stderr, "\nDone! %d records -> %s (%.0f rec/s)\n", count, *outPath, rate)
}

// exportPart streams one parquet file's rows into the CSV writer.
func exportPart(path string, w *csv.Writer, count *int64, start time.Time, lastReport *time.Time) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	pf, err := parquet.OpenFile(f, fi.Size())
	if err != nil {
		return err
	}

	reader := parquet.NewGenericReader[pq.Row](pf)
	defer reader.Close()

	buf := make([]pq.Row, 4096)
	for {
		n, err := reader.Read(buf)
		for i := 0; i < n; i++ {
			r := &buf[i]
			if werr := w.Write([]string{
				r.Key,
				strconv.FormatInt(r.SizeBytes, 10),
				r.LastModified.UTC().Format(time.RFC3339),
				r.StorageClass,
				r.ETag,
			}); werr != nil {
				return werr
			}
			*count++
			if now := time.Now(); now.Sub(*lastReport) >= 30*time.Second {
				rate := float64(*count) / now.Sub(start).Seconds()
				fmt.Fprintf(os.Stderr, "  exported %d records (%.0f rec/s)\n", *count, rate)
				*lastReport = now
			}
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}
