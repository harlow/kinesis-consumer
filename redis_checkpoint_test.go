package connector

import (
	"testing"

	"gopkg.in/redis.v5"
)

var defaultAddr = "127.0.0.1:6379"

func Test_CheckpointLifecycle(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: defaultAddr})

	c := RedisCheckpoint{
		AppName:    "app",
		StreamName: "stream",
		client:     client,
	}

	// set checkpoint
	c.SetCheckpoint("shard_id", "testSeqNum")

	// checkpoint exists
	if val := c.CheckpointExists("shard_id"); val != true {
		t.Fatalf("checkpoint exists expected true, got %t", val)
	}

	// get checkpoint
	if val := c.SequenceNumber(); val != "testSeqNum" {
		t.Fatalf("checkpoint exists expected %s, got %s", "testSeqNum", val)
	}

	client.Del("app:checkpoint:stream:shard_id")
}

func Test_key(t *testing.T) {
	client := redis.NewClient(&redis.Options{Addr: defaultAddr})

	c := &RedisCheckpoint{
		AppName:    "app",
		StreamName: "stream",
		client:     client,
	}

	expected := "app:checkpoint:stream:shard"

	if val := c.key("shard"); val != expected {
		t.Fatalf("checkpoint exists expected %s, got %s", expected, val)
	}
}
