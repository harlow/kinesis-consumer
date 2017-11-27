package consumer

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

func TestNew(t *testing.T) {
	_, err := New("myStreamName")
	if err != nil {
		t.Fatalf("new consumer error: %v", err)
	}
}

func TestScanShard(t *testing.T) {
	var (
		ckp    = &fakeCheckpoint{cache: map[string]string{}}
		ctr    = &fakeCounter{}
		client = newFakeClient(
			&Record{
				Data:           []byte("firstData"),
				SequenceNumber: aws.String("firstSeqNum"),
			},
			&Record{
				Data:           []byte("lastData"),
				SequenceNumber: aws.String("lastSeqNum"),
			},
		)
	)

	c := &Consumer{
		streamName: "myStreamName",
		client:     client,
		checkpoint: ckp,
		counter:    ctr,
		logger:     log.New(ioutil.Discard, "", log.LstdFlags),
	}

	// callback fn simply appends the record data to result string
	var (
		resultData string
		fn         = func(r *Record) bool {
			resultData += string(r.Data)
			return true
		}
	)

	// scan shard
	err := c.ScanShard(context.Background(), "myShard", fn)
	if err != nil {
		t.Fatalf("scan shard error: %v", err)
	}

	// increments counter
	if val := ctr.counter; val != 2 {
		t.Fatalf("counter error expected %d, got %d", 2, val)
	}

	// sets checkpoint
	val, err := ckp.Get("myStreamName", "myShard")
	if err != nil && val != "lastSeqNum" {
		t.Fatalf("checkout error expected %s, got %s", "lastSeqNum", val)
	}

	// calls callback func
	if resultData != "firstDatalastData" {
		t.Fatalf("callback error expected %s, got %s", "firstDatalastData", val)
	}
}

func newFakeClient(rs ...*Record) *fakeClient {
	fc := &fakeClient{
		recc: make(chan *Record, len(rs)),
		errc: make(chan error),
	}

	for _, r := range rs {
		fc.recc <- r
	}

	close(fc.errc)
	close(fc.recc)

	return fc
}

type fakeClient struct {
	shardIDs []string
	recc     chan *Record
	errc     chan error
}

func (fc *fakeClient) GetShardIDs(string) ([]string, error) {
	return fc.shardIDs, nil
}

func (fc *fakeClient) GetRecords(ctx context.Context, streamName, shardID, lastSeqNum string) (<-chan *Record, <-chan error, error) {
	return fc.recc, fc.errc, nil
}

type fakeCheckpoint struct {
	cache map[string]string
	mu    sync.Mutex
}

func (fc *fakeCheckpoint) Set(streamName, shardID, sequenceNumber string) error {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	key := fmt.Sprintf("%s-%s", streamName, shardID)
	fc.cache[key] = sequenceNumber
	return nil
}

func (fc *fakeCheckpoint) Get(streamName, shardID string) (string, error) {
	fc.mu.Lock()
	defer fc.mu.Unlock()

	key := fmt.Sprintf("%s-%s", streamName, shardID)
	return fc.cache[key], nil
}

type fakeCounter struct {
	counter int64
}

func (fc *fakeCounter) Add(streamName string, count int64) {
	fc.counter += count
}
