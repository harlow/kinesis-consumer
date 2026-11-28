# Change Log

All notable changes to this project will be documented in this file.

## [Unreleased (`master`)][unreleased]

Major changes:

* Remove the concept of `ScanStatus` to simplify the scanning interface
* Add DynamoDB-backed consumer-group coordination with cooperative shard handoff, lineage-aware shard assignment, and local end-to-end example coverage for join, leave, and failover rebalancing

For more context on this change see: https://github.com/harlow/kinesis-consumer/issues/75

Fixes:

* `ScanBatch`: flush and checkpoint the shard buffer before `CloseShard` or `ShardStopped`. Before, a closed shard could be marked complete while its records were still in the buffer, and a lease could be released before its buffer was processed.
* `ScanBatch`: serialize timed, size-based and final flushes for each shard. Before, the flush ticker drained the buffers outside the flush lock, so two batches of a shard could be processed and checkpointed out of order.
* `ScanBatch`: after a retriable `GetRecords` error, start the shard iterator after the last buffered record. Before, it started where the shard scan started, so the callback got records again. `Scan` with `ErrSkipCheckpoint` keeps its behavior.

## v0.3.0 - 2018-12-28

Major changes:

* Remove concept of `Client` it was confusing as it wasn't a direct standin for a Kinesis client.
* Rename `ScanError` to `ScanStatus` as it's not always an error.

Minor changes:

* Update tests to use Kinesis mock

## v0.2.0 - 2018-07-28

This is the last stable release from which there is a separate Client. It has caused confusion and will be removed going forward.

https://github.com/harlow/kinesis-consumer/releases/tag/v0.2.0

## v0.1.0 - 2017-11-20

This is the last stable release of the consumer which aggregated records in `batch` before calling the callback func.

https://github.com/harlow/kinesis-consumer/releases/tag/v0.1.0

[unreleased]: https://github.com/harlow/kinesis-consumer/compare/v0.2.0...HEAD
[options]: https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis
