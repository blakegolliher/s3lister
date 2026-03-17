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
	"strconv"
	"syscall"
	"time"

	"github.com/blake-golliher/s3lister/internal/config"
	"github.com/blake-golliher/s3lister/internal/s3client"
	"github.com/blake-golliher/s3lister/internal/store"
	"github.com/blake-golliher/s3lister/internal/worker"
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
  scan         Scan S3 bucket and store results in Pebble
  export-csv   Export Pebble database to CSV

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
		// Tee to both file and stderr
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

	logger, fileLogger, logFile := setupLogger(cfg.Logging.LogFile, *verbose)
	defer logFile.Close()

	logger.Printf("=== s3lister scan starting ===")
	logger.Printf("config: bucket=%s prefix=%q endpoint=%s readers=%d writers=%d queue=%d",
		cfg.S3.Bucket, cfg.S3.Prefix, cfg.S3.Endpoint, cfg.Workers.Readers, cfg.Workers.Writers, cfg.Workers.QueueSize)

	totalStart := time.Now()

	// Use a quiet pebble logger (only file, never stderr) unless verbose
	pebbleLogger := log.New(io.Discard, "", 0)
	if *verbose {
		pebbleLogger = fileLogger
	}

	db, err := store.Open(cfg.Storage.DBPath, pebbleLogger)
	if err != nil {
		fatal(logger, "%v", err)
	}
	defer db.Close()
	logger.Printf("pebble opened in %v", time.Since(totalStart))

	client, err := s3client.New(&cfg.S3, *verbose, logger)
	if err != nil {
		fatal(logger, "%v", err)
	}
	logger.Printf("s3 connected in %v", time.Since(totalStart))

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

	recordCh := make(chan store.ObjectRecord, cfg.Workers.QueueSize)

	writerPool := worker.NewWriterPool(db, recordCh, cfg.Workers.Writers, logger)
	writerDone := make(chan struct{})
	go func() {
		writerPool.Run()
		close(writerDone)
	}()

	readerPool := worker.NewReaderPool(client, cfg.S3.Bucket, cfg.S3.Prefix, cfg.Workers.Readers, recordCh, logger)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				listed := readerPool.Listed()
				written := db.Written()
				elapsed := time.Since(totalStart)
				rate := float64(written) / elapsed.Seconds()
				logger.Printf("[progress] listed=%d written=%d queued=%d rate=%.0f/s elapsed=%v",
					listed, written, listed-written, rate, elapsed.Round(time.Millisecond))
				fmt.Fprintf(os.Stderr, "\r  listed=%d  written=%d  queued=%d  rate=%.0f/s  elapsed=%v   ",
					listed, written, listed-written, rate, elapsed.Round(time.Millisecond))
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

	totalElapsed := time.Since(totalStart)
	totalObjects := db.Written()
	writeErrors := writerPool.Errors()
	rate := float64(totalObjects) / totalElapsed.Seconds()

	logger.Printf("=== scan complete: %d objects, %d errors, %v, %.0f/s ===",
		totalObjects, writeErrors, totalElapsed, rate)

	fmt.Fprintf(os.Stderr, "\n\nDone! %d objects in %v (%.0f/s)\n",
		totalObjects, totalElapsed.Round(time.Millisecond), rate)
	if writeErrors > 0 {
		fmt.Fprintf(os.Stderr, "WARNING: %d write errors occurred, check log\n", writeErrors)
	}
	fmt.Fprintf(os.Stderr, "Database: %s\nLog: %s\n", cfg.Storage.DBPath, cfg.Logging.LogFile)
}

func runExportCSV(args []string) {
	fs := flag.NewFlagSet("export-csv", flag.ExitOnError)
	dbPath := fs.String("db", "./s3lister.db", "path to Pebble database")
	outPath := fs.String("out", "s3objects.csv", "output CSV file path")
	verbose := fs.Bool("verbose", false, "verbose output")
	fs.Parse(args)

	fmt.Fprintf(os.Stderr, "Opening database: %s\n", *dbPath)

	pebbleLogger := log.New(io.Discard, "", 0)
	if *verbose {
		pebbleLogger = log.New(os.Stderr, "", log.LstdFlags|log.Lmicroseconds)
	}

	db, err := store.OpenReadOnly(*dbPath, pebbleLogger)
	if err != nil {
		fatal(nil, "%v", err)
	}
	defer db.Close()

	f, err := os.Create(*outPath)
	if err != nil {
		fatal(nil, "cannot create %s: %v", *outPath, err)
	}
	defer f.Close()

	bw := bufio.NewWriterSize(f, 4*1024*1024) // 4MB buffer
	w := csv.NewWriter(bw)
	w.Write([]string{"key", "size_bytes", "last_modified"})

	var count int64
	start := time.Now()
	lastReport := start

	err = db.Iterate(func(rec store.ObjectRecord) error {
		if err := w.Write([]string{
			rec.Key,
			strconv.FormatInt(rec.Size, 10),
			rec.LastModified.UTC().Format(time.RFC3339),
		}); err != nil {
			return err
		}

		count++
		if now := time.Now(); now.Sub(lastReport) >= 30*time.Second {
			rate := float64(count) / now.Sub(start).Seconds()
			fmt.Fprintf(os.Stderr, "  exported %d records (%.0f rec/s)\n", count, rate)
			lastReport = now
		}
		return nil
	})
	if err != nil {
		fatal(nil, "export failed: %v", err)
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
