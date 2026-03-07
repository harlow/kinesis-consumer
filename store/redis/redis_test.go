package redis

import (
	"testing"

	"github.com/alicebob/miniredis/v2"
	redis "github.com/redis/go-redis/v9"
)

func newTestCheckpoint(t *testing.T) *Checkpoint {
	t.Helper()

	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	t.Cleanup(func() { s.Close() })

	client := redis.NewClient(&redis.Options{Addr: s.Addr()})
	t.Cleanup(func() { _ = client.Close() })

	c, err := New("app", WithClient(client))
	if err != nil {
		t.Fatalf("new checkpoint error: %v", err)
	}
	return c
}

func Test_CheckpointOptions(t *testing.T) {
	s, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run() error = %v", err)
	}
	defer s.Close()

	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})

	_, err = New("app", WithClient(client))
	if err != nil {
		t.Fatalf("new checkpoint error: %v", err)
	}
}

func Test_CheckpointLifecycle(t *testing.T) {
	c := newTestCheckpoint(t)

	// set
	c.SetCheckpoint("streamName", "shardID", "testSeqNum")

	// get
	val, err := c.GetCheckpoint("streamName", "shardID")
	if err != nil {
		t.Fatalf("get checkpoint error: %v", err)
	}
	if val != "testSeqNum" {
		t.Fatalf("checkpoint exists expected %s, got %s", "testSeqNum", val)
	}
}

func Test_SetEmptySeqNum(t *testing.T) {
	c := newTestCheckpoint(t)

	err := c.SetCheckpoint("streamName", "shardID", "")
	if err == nil {
		t.Fatalf("should not allow empty sequence number")
	}
}

func Test_key(t *testing.T) {
	c := newTestCheckpoint(t)

	want := "app:checkpoint:stream:shard"

	if got := c.key("stream", "shard"); got != want {
		t.Fatalf("checkpoint key, want %s, got %s", want, got)
	}
}
