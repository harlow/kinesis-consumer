package store

import (
	"testing"
)

func Test_CheckpointLifecycle(t *testing.T) {
	c := New()

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
	c := New()

	err := c.SetCheckpoint("streamName", "shardID", "")
	if err == nil || err.Error() != "sequence number should not be empty" {
		t.Fatalf("should not allow empty sequence number")
	}
}
