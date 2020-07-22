// The memory store provides a store that can be used for testing and single-threaded applications.
// DO NOT USE this in a production application where persistence beyond a single application lifecycle is necessary
// or when there are multiple consumers.
package store

import (
	"fmt"
	"sync"
)

func New() *Store {
	return &Store{}
}

type Store struct {
	sync.Map
}

func (c *Store) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}
	c.Store(streamName+":"+shardID, sequenceNumber)
	return nil
}

func (c *Store) GetCheckpoint(streamName, shardID string) (string, error) {
	val, ok := c.Load(streamName + ":" + shardID)
	if !ok {
		return "", nil
	}
	return val.(string), nil
}
