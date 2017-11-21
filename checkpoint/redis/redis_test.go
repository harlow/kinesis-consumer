package redis

import (
	"testing"

	"gopkg.in/redis.v5"
)

var defaultAddr = "127.0.0.1:6379"

func Test_CheckpointLifecycle(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: defaultAddr})

	c := &Checkpoint{
		appName:    "app",
		streamName: "stream",
		client:     client,
	}

	// set checkpoint
	c.Set("shard_id", "testSeqNum")

	// get checkpoint
	val, err := c.Get("shard_id")
	if err != nil {
		t.Fatalf("get checkpoint error: %v", err)
	}

	if val != "testSeqNum" {
		t.Fatalf("checkpoint exists expected %s, got %s", "testSeqNum", val)
	}

	client.Del(c.key("shard_id"))
}

func Test_key(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: defaultAddr})

	c := &Checkpoint{
		appName:    "app",
		streamName: "stream",
		client:     client,
	}

	expected := "app:checkpoint:stream:shard"

	if val := c.key("shard"); val != expected {
		t.Fatalf("checkpoint exists expected %s, got %s", expected, val)
	}
}
