package integration_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	kinesistypes "github.com/aws/aws-sdk-go-v2/service/kinesis/types"
)

const (
	exampleIntegrationEnv  = "RUN_EXAMPLE_INTEGRATION"
	defaultKinesisEndpoint = "http://localhost:4567"
	defaultDynamoEndpoint  = "http://localhost:8000"
	defaultAWSRegion       = "us-west-2"
	defaultRedisAddr       = "127.0.0.1:6379"
)

var (
	buildOnce sync.Once
	buildErr  error
	binDir    string

	groupRecordPattern = regexp.MustCompile(`worker=(\S+) shard=(\S+) seq=(\S+) data=(.*)$`)
)

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

type exampleProcess struct {
	name    string
	cmd     *exec.Cmd
	output  *lockedBuffer
	done    chan struct{}
	waitErr error
	mu      sync.Mutex
}

type groupRecord struct {
	Worker string
	Shard  string
	Seq    string
	Data   string
}

func requireExampleIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv(exampleIntegrationEnv) != "1" {
		t.Skip("set RUN_EXAMPLE_INTEGRATION=1 to run example integration tests")
	}
}

func localKinesisEndpoint() string {
	if endpoint := os.Getenv("KINESIS_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return defaultKinesisEndpoint
}

func localDynamoEndpoint() string {
	if endpoint := os.Getenv("DDB_ENDPOINT"); endpoint != "" {
		return endpoint
	}
	return defaultDynamoEndpoint
}

func localRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return defaultRedisAddr
}

func mustRepoRoot(t *testing.T) string {
	t.Helper()
	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("os.Getwd() error = %v", err)
	}
	for {
		if _, statErr := os.Stat(filepath.Join(root, "go.mod")); statErr == nil {
			return root
		}
		next := filepath.Dir(root)
		if next == root {
			t.Fatalf("could not find repo root from %s", root)
		}
		root = next
	}
}

func mustLocalKinesisClient(t *testing.T) *kinesis.Client {
	t.Helper()

	resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if service == kinesis.ServiceID {
			return aws.Endpoint{URL: localKinesisEndpoint(), HostnameImmutable: true, SigningRegion: defaultAWSRegion}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(defaultAWSRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithEndpointResolver(resolver),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig(kinesis) error = %v", err)
	}

	client := kinesis.NewFromConfig(cfg)
	var readyErr error
	for i := 0; i < 20; i++ {
		_, readyErr = client.ListStreams(context.Background(), &kinesis.ListStreamsInput{Limit: aws.Int32(1)})
		if readyErr == nil {
			return client
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Skipf("skipping example integration tests (kinesis endpoint %s unavailable): %v", localKinesisEndpoint(), readyErr)
	return nil
}

func mustLocalDynamoClient(t *testing.T) *dynamodb.Client {
	t.Helper()

	resolver := aws.EndpointResolverFunc(func(service, region string) (aws.Endpoint, error) {
		if service == dynamodb.ServiceID {
			return aws.Endpoint{URL: localDynamoEndpoint(), HostnameImmutable: true}, nil
		}
		return aws.Endpoint{}, &aws.EndpointNotFoundError{}
	})

	cfg, err := config.LoadDefaultConfig(
		context.Background(),
		config.WithRegion(defaultAWSRegion),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("dummy", "dummy", "")),
		config.WithEndpointResolver(resolver),
	)
	if err != nil {
		t.Fatalf("LoadDefaultConfig(dynamodb) error = %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	var readyErr error
	for i := 0; i < 20; i++ {
		_, readyErr = client.ListTables(context.Background(), &dynamodb.ListTablesInput{Limit: aws.Int32(1)})
		if readyErr == nil {
			return client
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Skipf("skipping example integration tests (dynamodb endpoint %s unavailable): %v", localDynamoEndpoint(), readyErr)
	return nil
}

func cleanupDynamoTable(t *testing.T, client *dynamodb.Client, tableName string) {
	t.Helper()
	t.Cleanup(func() {
		_, _ = client.DeleteTable(context.Background(), &dynamodb.DeleteTableInput{TableName: aws.String(tableName)})
	})
}

func requireRedis(t *testing.T) {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: 500 * time.Millisecond}).Dial("tcp", localRedisAddr())
	if err == nil {
		_ = conn.Close()
		return
	}
	t.Skipf("skipping redis example integration tests (redis %s unavailable): %v", localRedisAddr(), err)
}

func buildExampleBinary(t *testing.T, relDir string) string {
	t.Helper()
	buildOnce.Do(func() {
		binDir, buildErr = os.MkdirTemp("", "kinesis-consumer-example-bin-")
		if buildErr != nil {
			return
		}
		root := mustRepoRoot(t)
		buildTargets := []struct {
			name string
			dir  string
		}{
			{name: "consumer-group-ddb", dir: "./examples/consumer-group-ddb"},
			{name: "consumer-redis", dir: "./examples/consumer-redis"},
			{name: "producer", dir: "./examples/producer"},
		}
		for _, target := range buildTargets {
			cmd := exec.Command("go", "build", "-o", filepath.Join(binDir, target.name), target.dir)
			cmd.Dir = root
			output, err := cmd.CombinedOutput()
			if err != nil {
				buildErr = fmt.Errorf("go build %s error: %w\n%s", target.dir, err, output)
				return
			}
		}
	})
	if buildErr != nil {
		t.Fatalf("build example binaries error = %v", buildErr)
	}
	return filepath.Join(binDir, filepath.Base(relDir))
}

func uniqueName(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), rand.Intn(1000))
}

func cleanupIntegrationStreams(t *testing.T, client *kinesis.Client) {
	t.Helper()
	out, err := client.ListStreams(context.Background(), &kinesis.ListStreamsInput{})
	if err != nil {
		t.Fatalf("ListStreams() error = %v", err)
	}
	for _, stream := range out.StreamNames {
		if !isIntegrationStream(stream) {
			continue
		}
		_, _ = client.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: aws.String(stream)})
	}
	time.Sleep(2 * time.Second)
}

func isIntegrationStream(stream string) bool {
	prefixes := []string{"group-start-", "group-join-", "group-failover-", "redis-example-"}
	for _, prefix := range prefixes {
		if strings.HasPrefix(stream, prefix) {
			return true
		}
	}
	return false
}

func createStream(t *testing.T, client *kinesis.Client, stream string, shards int32) {
	t.Helper()
	_, err := client.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: aws.String(stream), ShardCount: aws.Int32(shards)})
	if err != nil {
		var exists *kinesistypes.ResourceInUseException
		if errors.As(err, &exists) {
			goto wait
		}
		var limitExceeded *kinesistypes.LimitExceededException
		if errors.As(err, &limitExceeded) {
			cleanupIntegrationStreams(t, client)
			_, err = client.CreateStream(context.Background(), &kinesis.CreateStreamInput{StreamName: aws.String(stream), ShardCount: aws.Int32(shards)})
		}
		if err != nil {
			t.Fatalf("CreateStream(%s) error = %v", stream, err)
		}
	}
wait:
	waiter := kinesis.NewStreamExistsWaiter(client)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := waiter.Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String(stream)}, 30*time.Second); err != nil {
		t.Fatalf("StreamExistsWaiter(%s) error = %v", stream, err)
	}
	t.Cleanup(func() {
		_, _ = client.DeleteStream(context.Background(), &kinesis.DeleteStreamInput{StreamName: aws.String(stream)})
	})
}

func putRecords(t *testing.T, client *kinesis.Client, stream string, tag string, count int) {
	t.Helper()
	entries := make([]kinesistypes.PutRecordsRequestEntry, 0, count)
	for i := 0; i < count; i++ {
		payload := fmt.Sprintf(`{"run":"%s","i":%d}`, tag, i)
		entries = append(entries, kinesistypes.PutRecordsRequestEntry{
			Data:         []byte(payload),
			PartitionKey: aws.String(fmt.Sprintf("%s-%d-%d", tag, i, time.Now().UnixNano())),
		})
	}
	_, err := client.PutRecords(context.Background(), &kinesis.PutRecordsInput{
		StreamName: aws.String(stream),
		Records:    entries,
	})
	if err != nil {
		t.Fatalf("PutRecords(%s, %s) error = %v", stream, tag, err)
	}
}

func startProcess(t *testing.T, name string, bin string, args []string, env map[string]string) *exampleProcess {
	t.Helper()
	cmd := exec.Command(bin, args...)
	cmd.Dir = mustRepoRoot(t)
	cmd.Env = append(os.Environ(), formatEnv(env)...)
	buf := &lockedBuffer{}
	cmd.Stdout = buf
	cmd.Stderr = buf
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s error = %v", name, err)
	}
	proc := &exampleProcess{name: name, cmd: cmd, output: buf, done: make(chan struct{})}
	go func() {
		err := cmd.Wait()
		proc.mu.Lock()
		proc.waitErr = err
		proc.mu.Unlock()
		close(proc.done)
	}()
	t.Cleanup(func() {
		if proc.Alive() {
			_ = proc.Kill()
		}
	})
	return proc
}

func formatEnv(env map[string]string) []string {
	pairs := make([]string, 0, len(env))
	for k, v := range env {
		pairs = append(pairs, k+"="+v)
	}
	return pairs
}

func (p *exampleProcess) Logs() string { return p.output.String() }

func (p *exampleProcess) Alive() bool {
	select {
	case <-p.done:
		return false
	default:
		return true
	}
}

func (p *exampleProcess) StopInterrupt(timeout time.Duration) error {
	if !p.Alive() {
		return nil
	}
	if err := p.cmd.Process.Signal(os.Interrupt); err != nil {
		return err
	}
	select {
	case <-p.done:
		return p.WaitErr()
	case <-time.After(timeout):
		_ = p.cmd.Process.Kill()
		<-p.done
		return fmt.Errorf("timeout waiting for %s to stop", p.name)
	}
}

func (p *exampleProcess) Kill() error {
	if !p.Alive() {
		return nil
	}
	if err := p.cmd.Process.Signal(syscall.SIGKILL); err != nil {
		return err
	}
	<-p.done
	return nil
}

func (p *exampleProcess) WaitErr() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.waitErr
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool, desc string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", desc)
}

func assertNoScanError(t *testing.T, proc *exampleProcess) {
	t.Helper()
	if strings.Contains(proc.Logs(), "scan error:") {
		t.Fatalf("%s logs contain scan error:\n%s", proc.name, proc.Logs())
	}
}

func parseGroupRecords(logs string) []groupRecord {
	lines := strings.Split(logs, "\n")
	records := make([]groupRecord, 0)
	for _, line := range lines {
		match := groupRecordPattern.FindStringSubmatch(line)
		if len(match) != 5 {
			continue
		}
		records = append(records, groupRecord{Worker: match[1], Shard: match[2], Seq: match[3], Data: match[4]})
	}
	return records
}

func countUniqueSeq(records ...[]groupRecord) int {
	seen := make(map[string]struct{})
	for _, slice := range records {
		for _, rec := range slice {
			seen[rec.Seq] = struct{}{}
		}
	}
	return len(seen)
}

func countDuplicateSeq(records ...[]groupRecord) int {
	seen := make(map[string]int)
	var dupes int
	for _, slice := range records {
		for _, rec := range slice {
			seen[rec.Seq]++
		}
	}
	for _, count := range seen {
		if count > 1 {
			dupes += count - 1
		}
	}
	return dupes
}

func countTag(records []groupRecord, tag string) int {
	needle := strconv.Quote(tag)
	count := 0
	for _, rec := range records {
		if strings.Contains(rec.Data, `"run":`+needle) || strings.Contains(rec.Data, `"run":"`+tag+`"`) {
			count++
		}
	}
	return count
}
