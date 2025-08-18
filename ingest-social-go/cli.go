package main

import (
    "context"
    "flag"
    "fmt"
    "log"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/google/uuid"
    "github.com/Cookie579/fin-guard/config"
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

    // Load API keys from config.json located in the parent directory. The
    // configuration contains YouTube and StockTwits API keys. If the file
    // cannot be loaded the program exits.
    cfg, err := config.Load("../config.json")
    if err != nil {
        log.Fatalf("failed to load config.json: %v", err)
    }

    // Generate a unique job identifier and derive the ingest date (UTC)
    jobID := uuid.New().String()
    ingestDate := time.Now().UTC().Format(IngestDateFormat)

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
        // Start the StockTwits job. runnerStocktwits.go implements the
        // RunStocktwitsJob function. Any returned error will cause the
        // program to exit with a non-zero status.
        if err := RunStocktwitsJob(ctx, cfg, jobID, ingestDate, args); err != nil {
            log.Fatalf("stocktwits job failed: %v", err)
        }
    case "youtube":
        fs := flag.NewFlagSet("youtube", flag.ExitOnError)
        args := &youtubeArgs{}
        fs.StringVar(&args.Ticker, "ticker", "", "Stock ticker symbol (required)")
        fs.IntVar(&args.MaxVideos, "max-videos", 100, "max videos to process")
        fs.IntVar(&args.ExtraPages, "extra-pages", -1, "number of extra comment pages (-1 = all)")
        fs.IntVar(&args.DelayMS, "delay-ms", 0, "delay between API calls in ms (0-250)")
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
        if err := RunYouTubeJob(ctx, cfg, jobID, ingestDate, args); err != nil {
            log.Fatalf("youtube job failed: %v", err)
        }
    default:
        usage()
    }
}