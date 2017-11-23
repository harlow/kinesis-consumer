package redis

import (
	"testing"

	"gopkg.in/redis.v5"
)

var defaultAddr = "127.0.0.1:6379"

func Test_CheckpointLifecycle(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: defaultAddr})

	c := &Checkpoint{
		appName: "app",
		client:  client,
	}

	// set checkpoint
	c.Set("streamName", "shardID", "testSeqNum")

	// get checkpoint
	val, err := c.Get("streamName", "shardID")
	if err != nil {
		t.Fatalf("get checkpoint error: %v", err)
	}

	if val != "testSeqNum" {
		t.Fatalf("checkpoint exists expected %s, got %s", "testSeqNum", val)
	}

	client.Del(c.key("streamName", "shardID"))
}

func Test_key(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: defaultAddr})

	c := &Checkpoint{
		appName: "app",
		client:  client,
	}

	expected := "app:checkpoint:stream:shard"

	if val := c.key("stream", "shard"); val != expected {
		t.Fatalf("checkpoint exists expected %s, got %s", expected, val)
	}
}
