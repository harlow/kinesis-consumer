package integration_test

import (
	"strings"
	"testing"
	"time"
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
	waitFor(t, 20*time.Second, func() bool {
		return strings.Contains(workerA.Logs(), "start scan:") && strings.Contains(workerB.Logs(), "start scan:")
	}, "both workers to start scanning after stream creation")

	putRecords(t, kinesisClient, stream, "startup", 20)

	deadline := time.Now().Add(30 * time.Second)
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
	waitFor(t, 20*time.Second, func() bool {
		return strings.Contains(workerA.Logs(), "start scan:")
	}, "worker-a to start scanning")

	putRecords(t, kinesisClient, stream, "before", 20)
	waitFor(t, 30*time.Second, func() bool {
		return len(parseGroupRecords(workerA.Logs())) > 0
	}, "worker-a to consume initial batch")

	workerB := startGroupWorker(t, groupBin, stream, groupName, leaseTable, checkpointTable, "worker-b")
	time.Sleep(15 * time.Second)
	putRecords(t, kinesisClient, stream, "after", 40)

	waitFor(t, 40*time.Second, func() bool {
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
	waitFor(t, 20*time.Second, func() bool {
		return strings.Contains(workerA.Logs(), "start scan:") && strings.Contains(workerB.Logs(), "start scan:")
	}, "workers to start scanning before failover batch")

	putRecords(t, kinesisClient, stream, "before", 20)
	waitFor(t, 30*time.Second, func() bool {
		recordsA := parseGroupRecords(workerA.Logs())
		recordsB := parseGroupRecords(workerB.Logs())
		return countUniqueSeq(recordsA, recordsB) >= 20
	}, "workers to consume pre-failover batch")

	if err := workerB.Kill(); err != nil {
		t.Fatalf("workerB.Kill() error = %v", err)
	}

	// Example config uses LeaseDuration=20s and AssignInterval=5s.
	time.Sleep(28 * time.Second)

	putRecords(t, kinesisClient, stream, "after", 20)
	waitFor(t, 40*time.Second, func() bool {
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
	}
	return startProcess(t, workerID, bin, args, nil)
}
