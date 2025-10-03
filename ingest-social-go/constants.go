package main

// constants.go holds all of the infrastructure constants for the ingest-social-go
// collector. These values are hard-coded according to the specification
// provided by the user. None of these values are loaded from environment
// variables or from the config file. If you need to adjust regions or
// table/stream names you should do so here.

const (
	// AwsRegion is the AWS region used for all API calls. In most cases you
	// will not need to change this unless you deploy your infrastructure to
	// another region.
	AwsRegion = "us-east-1"

	// KinesisStreamName is the name of the Kinesis stream that receives all
	// emitted social records and metadata events. The stream must already
	// exist in your AWS account.
	KinesisStreamName = "fin-guard-stream"

	// DdbStateTable is the DynamoDB table used to persist per‐ticker state
	// such as the last processed StockTwits message ID or the last YouTube
	// search timestamp. See ddb.go for the exact schema keys.
	DdbStateTable = "social_state"

	// DdbLocksTable is the DynamoDB table used for distributed locking.
	// Each job acquires a row for the source/ticker pair it is processing
	// and periodically heartbeats to extend its lease. Stale locks are
	// automatically reaped using DynamoDB TTL on the expires_at column.
	DdbLocksTable = "social_locks"

	// DdbYTIdsTable is the DynamoDB table that stores every discovered
	// YouTube video ID, partitioned by ticker/year. This table prevents
	// duplicate harvests and enables idempotent insert behaviour.
	DdbYTIdsTable = "social_youtube_video_ids"

	// DdbStocktwitsSeenTable is the DynamoDB table that stores every
	// processed StockTwits message ID per ticker. Each row uses a
	// composite key of pk=stocktwits#<TICKER> and message_id. A TTL
	// attribute is set on each row so that old IDs are automatically
	// expired. This table is used by the StockTwits runner to skip
	// duplicate messages when they have already been ingested under
	// another ticker or in a previous run.
	DdbStocktwitsSeenTable = "social_stocktwits_seen"

	// StocktwitsSeenTTLDays controls how many days seen StockTwits
	// message identifiers will live in the DdbStocktwitsSeenTable. A
	// moderate TTL allows enough time for backfill windows to advance
	// while automatically pruning very old message IDs. The TTL is
	// translated into seconds when computing the expiration timestamp.
	StocktwitsSeenTTLDays = 14

	// LockHeartbeatSeconds controls how often a running job should heartbeat
	// to the lock table. If a job fails to heartbeat before its lock expires
	// another collector may acquire the lock. See LockTTLSeconds.
	LockHeartbeatSeconds = 60

	// LockTTLSeconds is the number of seconds after which an unrefreshed
	// lock expires. The TTL attribute on the DynamoDB table should be
	// configured to auto-delete expired locks.
	LockTTLSeconds = 120

	// KinMaxRecords is the maximum number of records to accumulate in a
	// single PutRecords call to Kinesis. AWS currently caps this at 500
	// records, however we leave a small margin for overhead and to stay
	// within Firehose size limits.
	KinMaxRecords = 400

	// KinMaxBytes is the maximum number of bytes to buffer before flushing
	// a batch to Kinesis. The PutRecords API allows up to 5 MiB per call.
	// We conservatively cap this at 4 MiB to leave room for partition keys
	// and request overhead.
	KinMaxBytes = 4_000_000

	// KinFlushEveryMs controls how often the publisher flushes any
	// accumulated records, even if neither the record count nor byte
	// thresholds have been exceeded. Keeping this value reasonably low
	// ensures timely delivery for sporadic workloads.
	KinFlushEveryMs = 500

	// ShardsMod is used when computing partition keys. A simple FNV‑1a
	// checksum of the job ID and record key is taken, then modded by this
	// value to produce a suffix. Having this value higher than your actual
	// shard count can help spread writes evenly across shards.
	ShardsMod = 16

	// IngestDateFormat defines the format used to stamp the ingest date on
	// every record and metadata event. This uses the Go reference time to
	// match YYYY‑MM‑DD.
	IngestDateFormat = "2006-01-02"

	// YtDefaultSearchPageSize is the default number of results returned
	// per YouTube search API call when the caller does not override
	// pagination. The YouTube Data API allows up to 50 per page.
	YtDefaultSearchPageSize = 50

	// YtDefaultCommentPageSize is the default number of comments returned
	// per comment threads API call. The YouTube Data API caps this at 100.
	YtDefaultCommentPageSize = 100

	// StageRaw indicates that a record belongs to the RAW data stage. All
	// comment, message and video metadata records emitted by this collector
	// should include this value in their "stage" field. It allows the data
	// lake to partition raw records separately from control plane events.
	StageRaw = "RAW"

	// StageEvents indicates that a record is a control-plane event (job
	// lifecycle or batch progress). Metadata events emitted by the runners
	// should include this value in their "stage" field so that they can be
	// routed to a separate stage prefix in S3. If you prefer to keep
	// events in RAW alongside data records, set StageEvents = StageRaw.
	StageEvents = "EVENTS"
)
