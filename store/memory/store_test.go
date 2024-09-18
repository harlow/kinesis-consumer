package store

import (
	"context"
	"testing"
)

func Test_CheckpointLifecycle(t *testing.T) {
	c := New()
	ctx := context.Background()

	// set
	_ = c.SetCheckpoint(ctx, "streamName", "shardID", "testSeqNum")

	// get
	val, err := c.GetCheckpoint(ctx, "streamName", "shardID")
	if err != nil {
		t.Fatalf("get checkpoint error: %v", err)
	}
	if val != "testSeqNum" {
		t.Fatalf("checkpoint exists expected %s, got %s", "testSeqNum", val)
	}
}

func Test_SetEmptySeqNum(t *testing.T) {
	c := New()
	ctx := context.Background()

	err := c.SetCheckpoint(ctx, "streamName", "shardID", "")
	if err == nil || err.Error() != "sequence number should not be empty" {
		t.Fatalf("should not allow empty sequence number")
	}
}
