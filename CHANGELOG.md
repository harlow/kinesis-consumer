# Change Log

All notable changes to this project will be documented in this file.

## [Unreleased (`master`)][unreleased]

** Breaking changes to consumer library **

Major changes:

* Use [functional options][options] for config
* Remove intermediate batching of kinesis records
* Call the callback func with each record
* Use dep for vendoring dependencies
* Add DDB as storage layer for checkpoints

Minor changes:

* remove unused buffer and emitter code

[unreleased]: https://github.com/harlow/kinesis-consumer/compare/v0.1.0...HEAD
[options]: https://dave.cheney.net/2014/10/17/functional-options-for-friendly-apis

## v0.1.0 - 2017-11-20

This is the last stable release of the consumer which aggregated records in `batch` before calling the callback func. 

https://github.com/harlow/kinesis-consumer/releases/tag/v0.1.0
