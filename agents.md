# Agents Notes: `kinesis-consumer`

## Repository Snapshot
- Language: Go (`go 1.13` in `go.mod`)
- Module: `github.com/harlow/kinesis-consumer`
- Purpose: lightweight Kinesis consumer with pluggable checkpoint stores
- AWS SDK: v2 (`github.com/aws/aws-sdk-go-v2/...`)

## Core Architecture
- Main consumer type: `Consumer` in `consumer.go`
- Constructor: `New(streamName string, opts ...Option)` in `consumer.go`
- Default behavior:
  - Shard iterator type defaults to `LATEST`
  - `Store`, `Logger`, and `Counter` are no-op by default
  - Scan interval default is `250ms`
  - Max records per `GetRecords` defaults to `10000`
  - Group defaults to `AllGroup` (consumes all shards, discovers new shards)
- Main scan APIs:
  - `Scan(ctx, fn)` scans all shards concurrently
  - `ScanShard(ctx, shardID, fn)` scans one shard

## Shard / Group Behavior
- `AllGroup` (`allgroup.go`) polls `ListShards` every 30 seconds.
- It tracks parent shard dependencies and waits for parent close before processing child shards.
- If group implements `CloseableGroup`, `Consumer` calls `CloseShard` after shard processing exits.
- Closed shard behavior:
  - Detected when next shard iterator is `nil` or unchanged.
  - Optional callback: `WithShardClosedHandler(...)`.

## Checkpointing
- Interfaces:
  - `Store` in `store.go`
  - `Group` in `group.go`
- Built-in stores:
  - Memory: `store/memory` (in-process only)
  - Redis: `store/redis`
  - DynamoDB: `store/ddb`
  - Postgres: `store/postgres`
  - MySQL: `store/mysql`
- Important persistence detail:
  - DDB/Postgres/MySQL stores buffer checkpoints in memory and flush periodically (default 1 minute).
  - Call `Shutdown()` on those stores in graceful shutdown paths to flush pending checkpoints.
  - Redis and memory writes happen immediately.

## Consumer Options (high-value)
- `WithClient(...)`
- `WithStore(...)`
- `WithGroup(...)`
- `WithLogger(...)`
- `WithCounter(...)`
- `WithShardIteratorType(...)`
- `WithTimestamp(...)` (used with `AT_TIMESTAMP` start behavior)
- `WithScanInterval(...)`
- `WithMaxRecords(...)`
- `WithGetRecordsOptions(...)`
- `WithAggregation(true)` to deaggregate KPL records (`internal/deaggregator`)
- `WithShardClosedHandler(...)`

## Error / Retry Behavior
- Retries `GetRecords` loop for:
  - `ExpiredIteratorException`
  - `ProvisionedThroughputExceededException`
- On retriable errors it rebuilds shard iterator from the last checkpointed sequence.
- Non-retriable errors stop scan and return up.

## Local Development and Examples
- Run all tests:
  - `go test ./...`
- This repo currently passes tests locally with default setup.
- Example programs are in `examples/`:
  - `examples/producer`
  - `examples/consumer`
  - `examples/consumer-redis`
  - `examples/consumer-dynamodb`
  - `examples/consumer-postgres`
  - `examples/consumer-mysql`
- Examples target local endpoints by default:
  - Kinesis: `http://localhost:4567` (typically Kinesalite)
  - DynamoDB: `http://localhost:8000` (for DDB example)

## Testing Notes
- Unit tests cover:
  - consumer scan semantics
  - shard discovery/ordering logic
  - store implementations
  - deaggregator behavior
- CI config (`.travis.yml`) historically used:
  - Go 1.13
  - `go test -v -race ./...`
  - Redis service

## Practical Gotchas
- If no custom `Store` is supplied, checkpoints are effectively non-persistent.
- If no custom `Logger`/`Counter` is supplied, observability is mostly silent.
- `Scan` starts one goroutine per shard; callback should be thread-safe across shards.
- For SQL stores, expected schema must exist before running consumer (see `README.md` examples).
- `ErrSkipCheckpoint` is supported to continue scanning without failing the consumer callback flow.
