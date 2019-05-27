package storage

import (
	"errors"
	"time"
)

// Lease is data for handling a lease/lock on a particular shard
type Lease struct {
	// LeaseKey is the partition/primaryKey in storage and is the shardID
	LeaseKey string `json:"leaseKey"`

	// Checkpoint the most updated sequenceNumber from kinesis
	Checkpoint string `json:"checkpoint"`

	// LeaseCounter will be updated any time a lease changes owners
	LeaseCounter int `json:"leaseCounter"`

	// LeaseOwner is the client id (defaulted to a guid)
	LeaseOwner string `json:"leaseOwner"`

	// HeartbeatID is a guid that gets updated on every heartbeat.  It is used to help determine if a lease is expired.
	// If a lease's heartbeatID hasn't been updated within the lease duration, then we assume the lease is expired
	HeartbeatID string `json:"heartbeatID"`

	// LastUpdateTime is the last time the lease has changed.  Purposely not stored in storage.  It is used with
	LastUpdateTime time.Time `json:"-"` // purposely left out of json so it doesn't get stored in dynamo
}

// IsExpired is a function to check if the lease is expired, but is only expected to be used in the heartbeat loop
func (lease Lease) IsExpired(maxLeaseDuration time.Duration) bool {
	if !lease.LastUpdateTime.IsZero() {
		durationPast := time.Since(lease.LastUpdateTime)
		if durationPast > maxLeaseDuration {
			return true
		}
	}
	return false
}

// StorageCouldNotUpdateOrCreateLease is a simple error for handling races that are lost in storage
var StorageCouldNotUpdateOrCreateLease = errors.New("storage could not update or create lease")
