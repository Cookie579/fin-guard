package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
)

// stocktwitsArgs captures the set of command line flags accepted by the
// stocktwits subcommand. See README or --help for details on each option.
type stocktwitsArgs struct {
	Ticker   string
	MaxPages int
	SinceID  int64
	UntilID  int64
}

// youtubeArgs captures the set of command line flags accepted by the
// youtube subcommand. See README or --help for details on each option.
type youtubeArgs struct {
	Ticker     string
	MaxVideos  int
	ExtraPages int
	DelayMS    int
}

// usage prints a helpful usage string to stderr and exits the program.
func usage() {
	fmt.Fprintf(os.Stderr, `Usage:
    ingest-social-go <command> [flags]

Commands:
    stocktwits   Collect StockTwits messages for a single ticker
    youtube      Collect YouTube video metadata and comments for a single ticker

Use "ingest-social-go <command> -h" to see available flags.
`)
	os.Exit(1)
}

// main is the entrypoint for the ingest-social-go binary. It dispatches to
// subcommands based on the first argument. Any unrecognised commands or
// missing required flags cause the program to print usage and exit.
func main() {
	if len(os.Args) < 2 {
		usage()
		return
	}

	cmd := os.Args[1]
	// Create a cancellable context that is cancelled when SIGINT or SIGTERM
	// is received. This allows the runners to gracefully shut down.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigc
		cancel()
	}()

	// Generate a unique job identifier and derive the ingest date (UTC)
	jobID := uuid.New().String()
	ingestDate := time.Now().UTC().Format(IngestDateFormat)

	// We do not load configuration here because API keys are read on demand
	// from config.json via loadFlatConfig() in the runners. A config
	// pointer is no longer needed by the runner signatures.

	switch cmd {
	case "stocktwits":
		fs := flag.NewFlagSet("stocktwits", flag.ExitOnError)
		args := &stocktwitsArgs{}
		fs.StringVar(&args.Ticker, "ticker", "", "Stock ticker symbol (required)")
		fs.IntVar(&args.MaxPages, "max-pages", 0, "max pages to fetch (0 = default logic)")
		fs.Int64Var(&args.SinceID, "since-id", 0, "only fetch messages newer than this ID")
		fs.Int64Var(&args.UntilID, "until-id", 0, "stop fetching when encountering this ID")
		fs.Usage = func() {
			fmt.Fprintf(os.Stderr, "Collect StockTwits messages for a single ticker.\n\n")
			fs.PrintDefaults()
		}
		if err := fs.Parse(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		if args.Ticker == "" {
			fs.Usage()
			os.Exit(1)
		}
		// Print JOB_STARTED marker prior to any work. Uppercase ticker for consistency.
		fmt.Printf("JOB_STARTED job_id=%s source=STOCKTWITS ticker=%s\n", jobID, strings.ToUpper(args.Ticker))
		// Run the StockTwits job.
		if err := RunStocktwitsJob(ctx, jobID, ingestDate, args); err != nil {
			// Determine exit code based on error type.
			switch {
			case errors.Is(err, ErrLockNotAcquired):
				fmt.Fprintf(os.Stderr, "stocktwits: lock not acquired: %v\n", err)
				os.Exit(2)
			case strings.Contains(err.Error(), "terminated"):
				os.Exit(130)
			default:
				fmt.Fprintf(os.Stderr, "stocktwits job failed: %v\n", err)
				os.Exit(1)
			}
		}
	case "youtube":
		fs := flag.NewFlagSet("youtube", flag.ExitOnError)
		args := &youtubeArgs{}
		fs.StringVar(&args.Ticker, "ticker", "", "Stock ticker symbol (required)")
		fs.IntVar(&args.MaxVideos, "max-videos", 10, "max videos to process (default 10)")
		fs.IntVar(&args.ExtraPages, "extra-pages", 0, "number of extra comment pages (0 = none)")
		fs.IntVar(&args.DelayMS, "delay-ms", 50, "delay between API calls in ms (0-250)")
		fs.Usage = func() {
			fmt.Fprintf(os.Stderr, "Collect YouTube videos and comments for a single ticker.\n\n")
			fs.PrintDefaults()
		}
		if err := fs.Parse(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
		if args.Ticker == "" {
			fs.Usage()
			os.Exit(1)
		}
		// Print JOB_STARTED marker for YouTube.
		fmt.Printf("JOB_STARTED job_id=%s source=YOUTUBE ticker=%s\n", jobID, strings.ToUpper(args.Ticker))
		if err := RunYouTubeJob(ctx, jobID, ingestDate, args); err != nil {
			switch {
			case errors.Is(err, ErrLockNotAcquired):
				fmt.Fprintf(os.Stderr, "youtube: lock not acquired: %v\n", err)
				os.Exit(2)
			case strings.Contains(err.Error(), "terminated"):
				os.Exit(130)
			default:
				fmt.Fprintf(os.Stderr, "youtube job failed: %v\n", err)
				os.Exit(1)
			}
		}
	default:
		usage()
	}
}
