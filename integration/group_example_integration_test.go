package integration_test

import (
	"context"
	"strings"
	"testing"
	"time"

	groupddb "github.com/harlow/kinesis-consumer/group/consumergroup/ddb"
)

const (
	groupTestLeaseDuration  = 6 * time.Second
	groupTestRenewInterval  = 1 * time.Second
	groupTestAssignInterval = 1 * time.Second
	groupTestScanInterval   = 50 * time.Millisecond
)

func TestGroupExample_StartBeforeStreamExists(t *testing.T) {
	requireExampleIntegration(t)
	kinesisClient := mustLocalKinesisClient(t)
	dynamoClient := mustLocalDynamoClient(t)

	groupBin := buildExampleBinary(t, "./examples/consumer-group-ddb")
	stream := uniqueName("group-start")
	groupName := uniqueName("group")
	leaseTable := uniqueName("lease")
	checkpointTable := uniqueName("checkpoint")
	cleanupDynamoTable(t, dynamoClient, leaseTable)
	cleanupDynamoTable(t, dynamoClient, checkpointTable)

	workerA := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-a")
	workerB := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-b")

	waitFor(t, 5*time.Second, func() bool {
		return workerA.Alive() && workerB.Alive()
	}, "workers to stay alive before stream creation")

	createStream(t, kinesisClient, stream, 2)
	waitFor(t, 10*time.Second, func() bool {
		return strings.Contains(workerA.Logs(), "start scan:") && strings.Contains(workerB.Logs(), "start scan:")
	}, "both workers to start scanning after stream creation")

	putRecords(t, kinesisClient, stream, "startup", 20)

	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		recordsA := parseGroupRecords(workerA.Logs())
		recordsB := parseGroupRecords(workerB.Logs())
		if countUniqueSeq(recordsA, recordsB) >= 20 {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	startupA := parseGroupRecords(workerA.Logs())
	startupB := parseGroupRecords(workerB.Logs())
	if got := countUniqueSeq(startupA, startupB); got < 20 {
		t.Fatalf("startup batch consumed %d records, want at least 20\nworker-a:\n%s\nworker-b:\n%s", got, workerA.Logs(), workerB.Logs())
	}

	if err := workerA.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("workerA.StopInterrupt() error = %v\n%s", err, workerA.Logs())
	}
	if err := workerB.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("workerB.StopInterrupt() error = %v\n%s", err, workerB.Logs())
	}

	assertNoScanError(t, workerA)
	assertNoScanError(t, workerB)
}

func TestGroupExample_LateJoinRebalancesWithoutDuplicates(t *testing.T) {
	requireExampleIntegration(t)
	kinesisClient := mustLocalKinesisClient(t)
	dynamoClient := mustLocalDynamoClient(t)

	groupBin := buildExampleBinary(t, "./examples/consumer-group-ddb")
	stream := uniqueName("group-join")
	groupName := uniqueName("group")
	leaseTable := uniqueName("lease")
	checkpointTable := uniqueName("checkpoint")
	cleanupDynamoTable(t, dynamoClient, leaseTable)
	cleanupDynamoTable(t, dynamoClient, checkpointTable)
	createStream(t, kinesisClient, stream, 2)

	workerA := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-a")
	waitFor(t, 10*time.Second, func() bool {
		return strings.Contains(workerA.Logs(), "start scan:")
	}, "worker-a to start scanning")

	putRecords(t, kinesisClient, stream, "before", 20)
	waitFor(t, 15*time.Second, func() bool {
		return len(parseGroupRecords(workerA.Logs())) > 0
	}, "worker-a to consume initial batch")

	workerB := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-b")
	time.Sleep(4 * time.Second)
	putRecords(t, kinesisClient, stream, "after", 40)

	waitFor(t, 20*time.Second, func() bool {
		recordsA := parseGroupRecords(workerA.Logs())
		recordsB := parseGroupRecords(workerB.Logs())
		return countUniqueSeq(recordsA, recordsB) >= 60
	}, "both workers to consume join batches")

	recordsA := parseGroupRecords(workerA.Logs())
	recordsB := parseGroupRecords(workerB.Logs())
	if got := countDuplicateSeq(recordsA, recordsB); got != 0 {
		t.Fatalf("duplicate sequence count = %d\nworker-a:\n%s\nworker-b:\n%s", got, workerA.Logs(), workerB.Logs())
	}
	if len(recordsB) == 0 {
		t.Fatalf("worker-b consumed 0 records\n%s", workerB.Logs())
	}

	if err := workerA.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("workerA.StopInterrupt() error = %v\n%s", err, workerA.Logs())
	}
	if err := workerB.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("workerB.StopInterrupt() error = %v\n%s", err, workerB.Logs())
	}

	assertNoScanError(t, workerA)
	assertNoScanError(t, workerB)
}

func TestGroupExample_LateJoinLogsHandoffTimeline(t *testing.T) {
	requireExampleIntegration(t)
	kinesisClient := mustLocalKinesisClient(t)
	dynamoClient := mustLocalDynamoClient(t)

	groupBin := buildExampleBinary(t, "./examples/consumer-group-ddb")
	stream := uniqueName("group-handoff")
	groupName := uniqueName("group")
	leaseTable := uniqueName("lease")
	checkpointTable := uniqueName("checkpoint")
	cleanupDynamoTable(t, dynamoClient, leaseTable)
	cleanupDynamoTable(t, dynamoClient, checkpointTable)
	createStream(t, kinesisClient, stream, 10)
	repo, err := groupddb.New(groupddb.Config{
		Client:    dynamoClient,
		TableName: leaseTable,
	})
	if err != nil {
		t.Fatalf("ddb.New() error = %v", err)
	}
	namespace := groupName + "#" + stream

	workerA := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-a")
	waitFor(t, 10*time.Second, func() bool {
		return strings.Contains(workerA.Logs(), "start scan:")
	}, "worker-a to start scanning")
	waitFor(t, 10*time.Second, func() bool {
		aCount, bCount, readErr := integrationOwnerCounts(repo, namespace)
		return readErr == nil && aCount == 10 && bCount == 0
	}, "worker-a to own all 10 shards")
	t.Logf("ownership: t=%s worker-a=10 worker-b=0", time.Now().Format(time.RFC3339Nano))

	putRecords(t, kinesisClient, stream, "before", 40)
	waitFor(t, 20*time.Second, func() bool {
		return countTag(parseGroupRecords(workerA.Logs()), "before") >= 20
	}, "worker-a to consume initial batch")

	joinAt := time.Now()
	workerB := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-b")
	waitFor(t, 10*time.Second, func() bool {
		return strings.Contains(workerB.Logs(), "start scan:")
	}, "worker-b to start scanning")
	t.Logf("ownership: t=%s worker-b joined", joinAt.Format(time.RFC3339Nano))

	rebalanceStart := time.Now()
	lastACount, lastBCount := 10, 0
	waitFor(t, 20*time.Second, func() bool {
		aCount, bCount, readErr := integrationOwnerCounts(repo, namespace)
		if readErr != nil {
			return false
		}
		if aCount != lastACount || bCount != lastBCount {
			t.Logf(
				"ownership: t=%s worker-a=%d worker-b=%d elapsed_since_join=%s",
				time.Now().Format(time.RFC3339Nano),
				aCount,
				bCount,
				time.Since(joinAt).Round(100*time.Millisecond),
			)
			lastACount, lastBCount = aCount, bCount
		}
		return aCount == 5 && bCount == 5
	}, "workers to rebalance to 5/5")
	t.Logf("ownership: rebalance reached 5/5 in %s", time.Since(rebalanceStart).Round(100*time.Millisecond))

	putRecords(t, kinesisClient, stream, "after", 80)
	waitFor(t, 20*time.Second, func() bool {
		return countTag(parseGroupRecords(workerB.Logs()), "after") > 0
	}, "worker-b to consume post-join batch")

	recordsA := parseGroupRecords(workerA.Logs())
	recordsB := parseGroupRecords(workerB.Logs())
	if got := countDuplicateSeq(recordsA, recordsB); got != 0 {
		t.Fatalf("duplicate sequence count = %d\nworker-a:\n%s\nworker-b:\n%s", got, workerA.Logs(), workerB.Logs())
	}

	timedA := parseTimedGroupRecords(workerA.Logs())
	timedB := parseTimedGroupRecords(workerB.Logs())
	firstBeforeA, ok := firstTaggedRecord(timedA, "before")
	if !ok {
		t.Fatalf("worker-a did not log a before record\n%s", workerA.Logs())
	}
	firstAfterB, ok := firstTaggedRecord(timedB, "after")
	if !ok {
		t.Fatalf("worker-b did not log an after record\n%s", workerB.Logs())
	}

	t.Logf(
		"handoff timeline: join_at=%s worker_a_first_before=%s shard=%s seq=%s worker_b_first_after=%s shard=%s seq=%s join_to_b_first_after=%s",
		joinAt.Format(time.RFC3339Nano),
		firstBeforeA.Time.Format(time.RFC3339),
		firstBeforeA.Record.Shard,
		firstBeforeA.Record.Seq,
		firstAfterB.Time.Format(time.RFC3339),
		firstAfterB.Record.Shard,
		firstAfterB.Record.Seq,
		firstAfterB.Time.Sub(joinAt).Round(time.Second),
	)
	t.Logf(
		"post-join message counts: worker-a before=%d after=%d worker-b before=%d after=%d total_after=%d",
		countTag(recordsA, "before"),
		countTag(recordsA, "after"),
		countTag(recordsB, "before"),
		countTag(recordsB, "after"),
		countTag(recordsA, "after")+countTag(recordsB, "after"),
	)

	if err := workerA.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("workerA.StopInterrupt() error = %v\n%s", err, workerA.Logs())
	}
	if err := workerB.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("workerB.StopInterrupt() error = %v\n%s", err, workerB.Logs())
	}

	assertNoScanError(t, workerA)
	assertNoScanError(t, workerB)
}

func integrationOwnerCounts(repo *groupddb.Repository, namespace string) (int, int, error) {
	leases, err := repo.ListLeases(context.Background(), namespace)
	if err != nil {
		return 0, 0, err
	}
	var aCount, bCount int
	for _, lease := range leases {
		switch lease.Owner {
		case "worker-a":
			aCount++
		case "worker-b":
			bCount++
		}
	}
	return aCount, bCount, nil
}

func TestGroupExample_FailoverAfterLeaseExpiry(t *testing.T) {
	requireExampleIntegration(t)
	kinesisClient := mustLocalKinesisClient(t)
	dynamoClient := mustLocalDynamoClient(t)

	groupBin := buildExampleBinary(t, "./examples/consumer-group-ddb")
	stream := uniqueName("group-failover")
	groupName := uniqueName("group")
	leaseTable := uniqueName("lease")
	checkpointTable := uniqueName("checkpoint")
	cleanupDynamoTable(t, dynamoClient, leaseTable)
	cleanupDynamoTable(t, dynamoClient, checkpointTable)
	createStream(t, kinesisClient, stream, 2)

	workerA := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-a")
	workerB := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-b")
	waitFor(t, 10*time.Second, func() bool {
		return strings.Contains(workerA.Logs(), "start scan:") && strings.Contains(workerB.Logs(), "start scan:")
	}, "workers to start scanning before failover batch")

	putRecords(t, kinesisClient, stream, "before", 20)
	waitFor(t, 15*time.Second, func() bool {
		recordsA := parseGroupRecords(workerA.Logs())
		recordsB := parseGroupRecords(workerB.Logs())
		return countUniqueSeq(recordsA, recordsB) >= 20
	}, "workers to consume pre-failover batch")

	if err := workerB.Kill(); err != nil {
		t.Fatalf("workerB.Kill() error = %v", err)
	}

	time.Sleep(groupTestLeaseDuration + 3*time.Second)

	putRecords(t, kinesisClient, stream, "after", 20)
	waitFor(t, 20*time.Second, func() bool {
		recordsA := parseGroupRecords(workerA.Logs())
		return countTag(recordsA, "after") >= 20
	}, "worker-a to consume post-failover batch")

	recordsA := parseGroupRecords(workerA.Logs())
	recordsB := parseGroupRecords(workerB.Logs())
	if got := countDuplicateSeq(recordsA, recordsB); got != 0 {
		t.Fatalf("duplicate sequence count = %d\nworker-a:\n%s\nworker-b:\n%s", got, workerA.Logs(), workerB.Logs())
	}
	if got := countTag(recordsA, "after"); got != 20 {
		t.Fatalf("worker-a consumed %d post-failover records, want 20\n%s", got, workerA.Logs())
	}

	if err := workerA.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("workerA.StopInterrupt() error = %v\n%s", err, workerA.Logs())
	}

	assertNoScanError(t, workerA)
	assertNoScanError(t, workerB)
}

func startGroupWorker(t *testing.T, bin, stream, group, leaseTable, checkpointTable, workerID string) *exampleProcess {
	t.Helper()
	args := []string{
		"-group", group,
		"-stream", stream,
		"-lease-table", leaseTable,
		"-checkpoint-table", checkpointTable,
		"-worker-id", workerID,
		"-ksis-endpoint", localKinesisEndpoint(),
		"-ddb-endpoint", localDynamoEndpoint(),
		"-lease-duration", groupTestLeaseDuration.String(),
		"-renew-interval", groupTestRenewInterval.String(),
		"-assign-interval", groupTestAssignInterval.String(),
		"-scan-interval", groupTestScanInterval.String(),
	}
	return startProcess(t, workerID, bin, args, nil)
}
