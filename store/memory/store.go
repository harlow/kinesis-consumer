// Package store
//
// The memory store provides a store that can be used for testing and single-threaded applications.
// DO NOT USE this in a production application where persistence beyond a single application lifecycle is necessary
// or when there are multiple consumers.
package store

import (
	"fmt"
	"sync"
)

// New returns a new in memory store to persist the last consumed offset.
func New() *Store {
	return &Store{}
}

// Store is the in-memory data structure that holds the offsets per stream
type Store struct {
	sync.Map
}

// SetCheckpoint stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
func (c *Store) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}
	c.Store(streamName+":"+shardID, sequenceNumber)
	return nil
}

// GetCheckpoint determines if a checkpoint for a particular Shard exists.
// Typically, this is used to determine whether processing should start with TRIM_HORIZON or AFTER_SEQUENCE_NUMBER
// (if checkpoint exists).
func (c *Store) GetCheckpoint(streamName, shardID string) (string, error) {
	val, ok := c.Load(streamName + ":" + shardID)
	if !ok {
		return "", nil
	}
	return val.(string), nil
}
