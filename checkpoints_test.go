package etl

import (
	"github.com/hoisie/redis"
	"testing"
)

func TestKeyGen(t *testing.T) {
	k := "app:checkpoint:stream:shard"
	c := RedisCheckpoint{AppName: "app"}

	r := c.keyGen("stream", "shard")

	if r != k {
		t.Errorf("Key() = %v, want %v", k, r)
	}
}

func TestCheckpointExists(t *testing.T) {
	var rc redis.Client
	k := "app:checkpoint:stream:shard"
	rc.Set(k, []byte("fakeSeqNum"))
	c := RedisCheckpoint{AppName: "app"}

	r := c.CheckpointExists("stream", "shard")

	if r != true {
		t.Errorf("CheckpointExists() = %v, want %v", false, r)
	}

	rc.Del(k)
}

func TestSetCheckpoint(t *testing.T) {
	k := "app:checkpoint:stream:shard"
	var rc redis.Client
	c := RedisCheckpoint{AppName: "app"}
	c.SetCheckpoint("stream", "shard", "fakeSeqNum")

	r, _ := rc.Get(k)

	if string(r) != "fakeSeqNum" {
		t.Errorf("SetCheckpoint() = %v, want %v", "fakeSeqNum", r)
	}

	rc.Del(k)
}
