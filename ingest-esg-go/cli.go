package main

import (
    "context"
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

// esgArgs captures the set of command line flags accepted by the esg
// subcommand. Additional flags can be added as needed to expose new
// functionality. See README or --help for details on each option.
type esgArgs struct {
    Tickers     string
    Mode        string
    Bucket      string
    Stream      string
    Blacklist   string
    Concurrency int
    Region      string
}

// ingestDateFormat defines the format used to stamp the ingest date on
// every record. The value matches YYYY‑MM‑DD.
const ingestDateFormat = "2006-01-02"

// usage prints a helpful usage string to stderr and exits the program.
func usage() {
    fmt.Fprintf(os.Stderr, `Usage:
    ingest-esg-go <command> [flags]

Commands:
    esg   Collect ESG links and/or domains for one or more tickers

Use "ingest-esg-go <command> -h" to see available flags.
`)
    os.Exit(1)
}

// main dispatches to subcommands based on the first argument. Unknown
// commands or missing required flags cause the program to print usage and
// exit with a non‑zero status code. The esg subcommand launches the
// ESG ingest job.
func main() {
    if len(os.Args) < 2 {
        usage()
        return
    }
    cmd := os.Args[1]
    switch cmd {
    case "esg":
        fs := flag.NewFlagSet("esg", flag.ExitOnError)
        args := &esgArgs{}
        fs.StringVar(&args.Tickers, "tickers", "", "Comma‑separated list of stock tickers (required)")
        fs.StringVar(&args.Mode, "mode", "esg+domain", "Run mode: domain | esg | esg+domain")
        fs.StringVar(&args.Bucket, "bucket", "fin-processed-esg-reports", "S3 bucket for JSON output")
        fs.StringVar(&args.Stream, "stream", "", "Kinesis stream for link records (optional; if empty uses config.json)")
        fs.StringVar(&args.Blacklist, "blacklist", "blacklist.txt", "Path to blacklist file")
        fs.IntVar(&args.Concurrency, "concurrency", 1, "Number of tickers to process in parallel")
        fs.StringVar(&args.Region, "region", os.Getenv("AWS_REGION"), "AWS region override (optional)")
        fs.Usage = func() {
            fmt.Fprintf(os.Stderr, "Collect ESG links and/or domains for one or more tickers.\n\n")
            fs.PrintDefaults()
        }
        if err := fs.Parse(os.Args[2:]); err != nil {
            log.Fatal(err)
        }
        if args.Tickers == "" {
            fs.Usage()
            os.Exit(1)
        }
        // Set up a cancellable context that is cancelled on SIGINT or SIGTERM.
        ctx, cancel := context.WithCancel(context.Background())
        defer cancel()
        sigc := make(chan os.Signal, 1)
        signal.Notify(sigc, syscall.SIGINT, syscall.SIGTERM)
        go func() {
            <-sigc
            cancel()
        }()
        // Generate a unique job identifier and derive the ingest date (UTC).
        jobID := uuid.New().String()
        ingestDate := time.Now().UTC().Format(ingestDateFormat)
        // Split and sanitize tickers
        list := strings.Split(args.Tickers, ",")
        for i := range list {
            list[i] = strings.ToUpper(strings.TrimSpace(list[i]))
        }
        // Process each ticker sequentially. Errors are logged but do not
        // abort the entire run unless the context is cancelled by a
        // signal.
        for _, t := range list {
            select {
            case <-ctx.Done():
                return
            default:
            }
            log.Printf("JOB_STARTED job_id=%s source=esg ticker=%s", jobID, t)
            if err := RunESGJob(ctx, jobID, ingestDate, t, args); err != nil {
                fmt.Fprintf(os.Stderr, "esg job failed for %s: %v\n", t, err)
            }
        }
    default:
        usage()
    }
}