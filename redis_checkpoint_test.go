package connector

import (
	"testing"

	"github.com/hoisie/redis"
)

func TestKey(t *testing.T) {
	k := "app:checkpoint:stream:shard"
	c := RedisCheckpoint{AppName: "app", StreamName: "stream"}

	r := c.key("shard")

	if r != k {
		t.Errorf("key() = %v, want %v", k, r)
	}
}

func TestCheckpointExists(t *testing.T) {
	var rc redis.Client
	k := "app:checkpoint:stream:shard"
	rc.Set(k, []byte("fakeSeqNum"))
	c := RedisCheckpoint{AppName: "app", StreamName: "stream"}

	r := c.CheckpointExists("shard")

	if r != true {
		t.Errorf("CheckpointExists() = %v, want %v", false, r)
	}

	rc.Del(k)
}

func TestSetCheckpoint(t *testing.T) {
	k := "app:checkpoint:stream:shard"
	var rc redis.Client
	c := RedisCheckpoint{AppName: "app", StreamName: "stream"}
	c.SetCheckpoint("shard", "fakeSeqNum")

	r, _ := rc.Get(k)

	if string(r) != "fakeSeqNum" {
		t.Errorf("SetCheckpoint() = %v, want %v", "fakeSeqNum", r)
	}

	rc.Del(k)
}
