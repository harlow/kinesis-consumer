package connector

import (
	"testing"

	"github.com/bmizerany/assert"
	"github.com/hoisie/redis"
)

func Test_key(t *testing.T) {
	c := Checkpoint{
		AppName:    "app",
		StreamName: "stream",
	}

	k := c.key("shard")
	assert.Equal(t, k, "app:checkpoint:stream:shard")
}

func Test_CheckpointExists(t *testing.T) {
	var rc redis.Client
	rc.Set("app:checkpoint:stream:shard", []byte("testSeqNum"))
	c := Checkpoint{
		AppName:    "app",
		StreamName: "stream",
	}

	r := c.CheckpointExists("shard")
	assert.Equal(t, r, true)

	rc.Del("app:checkpoint:stream:shard")
}

func Test_SetCheckpoint(t *testing.T) {
	var rc redis.Client
	c := Checkpoint{
		AppName:    "app",
		StreamName: "stream",
	}

	c.SetCheckpoint("shard", "testSeqNum")
	r, _ := rc.Get("app:checkpoint:stream:shard")
	assert.Equal(t, string(r), "testSeqNum")

	rc.Del("app:checkpoint:stream:shard")
}
