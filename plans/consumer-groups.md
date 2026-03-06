# Consumer Groups Plan (Issue #42)

## Status
- Draft plan for implementing multi-process shard coordination without breaking existing API behavior.
- Scope target: first implementation pass with DynamoDB-backed lease coordination.

## Problem Statement
Today, multiple processes running `Scan` each consume all shards, so records are duplicated across instances. Issue #42 asks for multi-process coordination so different workers consume different shards.

## Goals
- Non-breaking change for existing users.
- Keep existing `New(...)`, `Scan(...)`, `ScanShard(...)`, `WithStore(...)`, and default `AllGroup` behavior unchanged.
- Add a new parallel interface for coordinated multi-worker consumption.
- Support at-least-once semantics (same as current model).
- Handle worker failure via lease expiration/failover.
- Respect shard lineage ordering (parent/child) as currently handled by `AllGroup`.

## Non-Goals (V1)
- Exactly-once guarantees.
- Full KCL feature parity (CPU-aware balancing, advanced migration/state tables).
- Cross-region coordination.
- Replacing current checkpoint backends.
- Breaking or removing existing interfaces.

## Current Constraints in This Repo
- `Consumer.Scan` delegates shard assignment to `Group.Start(ctx, shardC)`.
- `Group` already abstracts shard assignment + checkpoint access:
  - `Start(ctx, shardC)`
  - `GetCheckpoint(streamName, shardID)`
  - `SetCheckpoint(streamName, shardID, seqNum)`
- Default implementation `AllGroup` discovers all shards and emits all to one process.
- Checkpoint store is pluggable and independent today.

This is ideal for a non-breaking path: add a new `Group` implementation that emits only leased shards per worker.

## Prior Art and Key Learnings
- KCL stores shard leases in DynamoDB with fields like `leaseKey`, `leaseOwner`, `leaseCounter`, lineage, and checkpoint metadata; it uses worker failover timers and balancing knobs.
  - Source: [AWS KCL DynamoDB metadata tables](https://docs.aws.amazon.com/streams/latest/dev/kcl-dynamoDB.html)
  - Source: [AWS KCL shared-throughput concepts](https://docs.aws.amazon.com/streams/latest/dev/shared-throughput-kcl-consumers.html)
- `segmentio/kafka-go` consumer groups expose a separate group config surface (`GroupID`, heartbeats, balancers, rebalance timeout, backoff) while keeping the simple reader API.
  - Source: [kafka-go package docs](https://pkg.go.dev/github.com/segmentio/kafka-go)
- Kinsumer shows a practical Go pattern: DynamoDB-backed coordination for multiple workers consuming multiple shards.
  - Source: [twitchscience/kinsumer](https://github.com/twitchscience/kinsumer)

Design takeaway: add a dedicated group-coordination config/interface, keep current consumer API stable, and use DynamoDB conditional writes for lease ownership.

## Recommended API Design (Non-Breaking)

### Keep Existing API Intact
- No change to default behavior:
  - `consumer.New(stream)` + `Scan` still means “single process consumes all shards”.

### Add New Group Implementation + Config
- New package (recommended): `group/consumergroup/ddb` (or `group/ddbgroup`).
- Constructor returns `consumer.Group`:
  - `func New(cfg Config) (*Group, error)`
- Users opt in via existing option:
  - `consumer.New(stream, consumer.WithGroup(ddbgroup.New(...)), consumer.WithStore(...))`

### Proposed Config (V1)
```go
type Config struct {
    AppName            string        // existing concept, required
    StreamName         string        // required
    WorkerID           string        // required, unique per process
    TableName          string        // required (lease metadata table)
    DynamoClient       *dynamodb.Client
    LeaseDuration      time.Duration // default: 20s
    RenewInterval      time.Duration // default: 5s
    AssignInterval     time.Duration // default: 10s
    FailoverJitter     time.Duration // default: small random jitter
    MaxLeasesForWorker int           // default: 0 => auto/unbounded
    MaxLeasesToSteal   int           // default: 1
    EnableStealing     bool          // default: true
}
```

### Why This Is Non-Breaking
- Existing `Group` interface is reused.
- Existing users do nothing and get identical behavior.
- New behavior is opt-in through `WithGroup(...)`.

## DynamoDB-Only First?
Recommendation: **yes for coordination backend (V1)**.

Rationale:
- KCL and Kinsumer both validate DynamoDB as a lease-coordination store.
- Conditional updates + TTL + GSIs fit lease ownership semantics well.
- Lower implementation risk than supporting Redis/Postgres/MySQL coordination from day one.

Important nuance:
- Coordination store can be DynamoDB-only initially.
- Checkpoint store remains pluggable (`WithStore(...)`) and unchanged.

## Data Model (DynamoDB) - Recommended Single-Table Design

Table keys:
- `PK` (partition key): `namespace` (`<appName>#<streamName>`)
- `SK` (sort key): entity key:
  - lease rows: `LEASE#<shardID>`
  - worker rows: `WORKER#<workerID>`

Lease item attributes:
- `entity_type = "LEASE"`
- `shard_id`
- `lease_owner` (worker ID or empty)
- `lease_counter` (monotonic on ownership change)
- `lease_expires_at` (epoch millis)
- `parent_shard_id` (optional)
- `adjacent_parent_shard_id` (optional)
- `owner_switches_since_checkpoint`
- `updated_at`

Worker item attributes:
- `entity_type = "WORKER"`
- `worker_id`
- `heartbeat_at`
- `worker_expires_at` (TTL candidate)
- `updated_at`

GSI (for efficient lease lookup by owner):
- `GSI1PK = owner_key` where `owner_key = <namespace>#<workerID>`
- `GSI1SK = lease_sk` (`LEASE#<shardID>`)

Notes:
- Single-table keeps deployment simple and avoids cross-table transactions.
- Lease cleanup for deleted shards can be periodic best-effort (no hard dependency for V1).

## Lease Lifecycle Algorithm (V1)

### Bootstrap
1. Ensure table exists (optional helper; can also require pre-provisioned table).
2. `ListShards` and upsert lease rows for unseen shards.
3. Upsert worker heartbeat row.

### Acquire/Renew
For each assignment cycle:
1. Renew currently owned leases (conditional update `lease_owner == me`).
2. If under target capacity, try to acquire unowned/expired leases:
   - Condition: lease unowned OR expired OR already owned by me.
3. Emit newly acquired shards to `shardC`.

### Balancing (Steal)
If no unowned/expired leases and worker under target:
1. Compute active workers from worker heartbeat rows.
2. Estimate target leases/worker.
3. Optionally steal up to `MaxLeasesToSteal` from overloaded workers using conditional update.

### Release
- On graceful shutdown or `CloseShard`, release lease ownership early (set owner empty + short expiry) to reduce failover latency.
- On crash, lease naturally expires after `LeaseDuration`.

## Integration with Existing Consumer Flow
- `Scan` remains unchanged.
- New group only changes which shards are emitted to `shardC`.
- `ScanShard` callback/checkpoint semantics unchanged.
- Parent/child ordering:
  - preserve current behavior by not emitting a child shard until parent closed/fully consumed based on lineage fields and existing closure signals.

## Failure and Consistency Semantics
- Delivery guarantee remains at-least-once.
- Duplicate processing is still possible around worker crashes or lease handoff races.
- Lease conditional updates prevent concurrent “active ownership” in normal operation.

## Observability Requirements
- Counters:
  - leases acquired
  - lease renew failures
  - lease steals
  - rebalances
  - shard assignment loops
- Logs:
  - ownership transitions
  - failed conditional updates
  - worker join/leave detection

## TDD Execution Strategy (Required)
- This feature should be delivered with a strict tests-first workflow.
- For each behavior, write failing tests first, then implement the minimum code to pass, then refactor.
- Do not merge feature code without corresponding tests in the same PR.
- Treat consumer-group correctness tests as release gates for this subsystem.

### Tests-First Delivery Sequence
1. Add deterministic unit tests for lease math and state transitions (no Dynamo dependency).
2. Add contract tests for repository/store interfaces used by the group coordinator.
3. Add orchestration tests for assignment loop behavior under joins/leaves/rebalances.
4. Add integration tests (local DynamoDB) for conditional-write race paths and failover timing.
5. Implement production code incrementally behind these tests until all pass.

### Minimum Acceptance Matrix Before Merge
- Single worker with 10 shards acquires all 10.
- Second worker join converges to near-even shard split (for 10 shards, typically 5/5).
- Worker crash causes lease takeover after expiration window.
- Lease-renew race does not allow dual active ownership.
- Parent shard completion gating prevents child shard early start.
- Checkpoint continuity is preserved through ownership handoff.

## Executable Test Checklist

### Proposed Test Files
- `group/consumergroup/ddb/lease_state_test.go`
- `group/consumergroup/ddb/rebalance_test.go`
- `group/consumergroup/ddb/assignment_loop_test.go`
- `group/consumergroup/ddb/repository_contract_test.go`
- `group/consumergroup/ddb/integration_dynamodb_test.go` (build tag/integration gated)

### Table-Driven Cases (Must Exist)
- `TestLeaseAcquire`:
  - unowned lease -> acquired
  - expired lease -> acquired
  - owned-by-self lease -> renewed
  - owned-by-other unexpired lease -> rejected
- `TestLeaseRenew`:
  - owner matches -> expiry extended
  - owner mismatch -> conditional failure
- `TestLeaseSteal`:
  - stealing disabled -> no-op
  - stealing enabled under target -> steals at most `MaxLeasesToSteal`
  - candidate owner not overloaded -> no steal
- `TestRebalanceTarget`:
  - 10 shards / 1 worker => 10 target
  - 10 shards / 2 workers => 5 target each
  - 10 shards / 3 workers => 3/3/4 distribution constraints
- `TestAssignmentLoop`:
  - worker join triggers convergence to balanced ownership
  - worker leave/crash triggers lease takeover after timeout
  - graceful release shortens takeover latency
- `TestLineageGating`:
  - child shard blocked while parent active
  - child shard allowed after parent close signal
- `TestCheckpointHandoff`:
  - new owner resumes from last persisted checkpoint
  - duplicate window remains bounded to at-least-once expectations

### Determinism Requirements
- Use fake clock/time source for all lease-expiration tests.
- Use deterministic RNG seed for any jitter/rebalance randomness.
- Avoid sleeps in unit tests; drive scheduler loops manually where possible.

### CI Gates
- `go test ./...` must include all non-integration consumer-group tests.
- Integration suite runs in a separate job/stage with local DynamoDB.
- PR merge blocked on both unit and integration suites for files under `group/consumergroup/ddb`.

### Suggested PR Slices (Tests First)
1. PR 1: add interfaces, fake clock harness, and failing unit tests for lease state transitions.
2. PR 2: implement acquire/renew/release until PR 1 tests pass.
3. PR 3: add failing rebalance/steal tests + assignment loop tests.
4. PR 4: implement rebalance logic until PR 3 tests pass.
5. PR 5: add integration tests (DynamoDB local) and implement any race/failover fixes.

## Testing Plan

### Unit Tests (required first)
- lease acquire success/failure paths
- renew success/failure paths
- steal logic boundaries
- worker heartbeat expiration behavior
- parent/child ordering constraints in grouped mode
- deterministic rebalance target calculations (including odd shard counts)
- join/leave convergence timing with fake clock and controlled scheduler

### Integration Tests (phase 2)
- optional local DynamoDB integration tests (gated, not required for default `go test ./...`)
- multi-worker simulation:
  - N workers, M shards
  - one worker dies
  - leases rebalance and continue processing
- conditional update contention tests across concurrent workers
- graceful shutdown lease release tests (handoff latency bounded)

## Rollout Plan

### Phase 0: Design + docs
- finalize config/API names and table schema

### Phase 1: Core lease ownership
- implement DDB-backed group
- acquire/renew/release flows
- no stealing initially (or behind config flag)

### Phase 2: Rebalancing
- add worker table + target-based stealing
- add metrics/logging

### Phase 3: Hardening
- lineage/reshard edge-cases
- optional integration tests
- example app for multi-process setup

## Migration and Backward Compatibility
- No migration required for existing users.
- Existing checkpoint tables remain valid.
- New coordination table is additive and only needed by opt-in users.

## Open Questions
- Should V1 require checkpoint store to be DynamoDB for operational simplicity, or allow any store?
  - Recommendation: allow any store, but document/test DDB checkpoint path first.
- Should lease table auto-create be built-in or documented as pre-provisioned infra?
  - Recommendation: support both; default to pre-provisioned in production docs.
- Should stealing be enabled by default in V1?
  - Recommendation: start conservative (`EnableStealing=true`, `MaxLeasesToSteal=1`) with clear tuning docs.

## Recommendation Summary
- Implement issue #42 with a **new opt-in DynamoDB consumer-group `Group` implementation**.
- Keep all existing interfaces and defaults unchanged.
- Start with DDB-only coordination backend, phased balancing features, and strong unit coverage.
