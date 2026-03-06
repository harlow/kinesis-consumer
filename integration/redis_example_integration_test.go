package integration_test

import (
	"strings"
	"testing"
	"time"
)

func TestRedisExample_RestartsFromCheckpoint(t *testing.T) {
	requireExampleIntegration(t)
	requireRedis(t)
	kinesisClient := mustLocalKinesisClient(t)

	redisBin := buildExampleBinary(t, "./examples/consumer-redis")
	stream := uniqueName("redis-example")
	appName := uniqueName("redis-app")
	createStream(t, kinesisClient, stream, 2)

	consumerA := startProcess(t, "consumer-redis", redisBin, []string{
		"-app", appName,
		"-stream", stream,
		"-endpoint", localKinesisEndpoint(),
	}, nil)
	waitFor(t, 20*time.Second, func() bool {
		return strings.Contains(consumerA.Logs(), "start scan:")
	}, "redis consumer to start scanning")

	putRecords(t, kinesisClient, stream, "first", 12)
	waitFor(t, 30*time.Second, func() bool {
		return strings.Count(consumerA.Logs(), `"run":"first"`) >= 12
	}, "redis consumer to consume first batch")

	if err := consumerA.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("consumerA.StopInterrupt() error = %v\n%s", err, consumerA.Logs())
	}
	if !strings.Contains(consumerA.Logs(), "caught exit signal, cancelling context!") {
		t.Fatalf("consumerA missing shutdown signal log\n%s", consumerA.Logs())
	}

	consumerB := startProcess(t, "consumer-redis-restart", redisBin, []string{
		"-app", appName,
		"-stream", stream,
		"-endpoint", localKinesisEndpoint(),
	}, nil)
	waitFor(t, 20*time.Second, func() bool {
		return strings.Contains(consumerB.Logs(), "start scan:")
	}, "redis consumer restart to start scanning")

	putRecords(t, kinesisClient, stream, "second", 5)
	waitFor(t, 30*time.Second, func() bool {
		return strings.Count(consumerB.Logs(), `"run":"second"`) >= 5
	}, "redis consumer restart to consume second batch")

	if got := strings.Count(consumerB.Logs(), `"run":"first"`); got != 0 {
		t.Fatalf("restart replayed %d records from first batch\n%s", got, consumerB.Logs())
	}

	if err := consumerB.StopInterrupt(10 * time.Second); err != nil {
		t.Fatalf("consumerB.StopInterrupt() error = %v\n%s", err, consumerB.Logs())
	}
	if !strings.Contains(consumerB.Logs(), "caught exit signal, cancelling context!") {
		t.Fatalf("consumerB missing shutdown signal log\n%s", consumerB.Logs())
	}
	if strings.Contains(consumerA.Logs(), "scan error:") || strings.Contains(consumerB.Logs(), "scan error:") {
		t.Fatalf("redis example logs contain scan error\nconsumerA:\n%s\nconsumerB:\n%s", consumerA.Logs(), consumerB.Logs())
	}

}
