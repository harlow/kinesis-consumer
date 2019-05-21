package consumer

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/twinj/uuid"
)

// TODO change logging to actual logger

// StorageCouldNotUpdateOrCreateLease is a simple error for handling races that are lost in storage
var StorageCouldNotUpdateOrCreateLease = errors.New("storage could not update or create lease")

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

// CheckpointStorage is a simple interface for abstracting away the storage functions
type CheckpointStorage interface {
	CreateLease(lease Lease) error
	UpdateLease(originalLease, updatedLease Lease) error
	GetLease(leaseKey string) (*Lease, error)
	GetAllLeases() (map[string]Lease, error)
}

// ConsumerGroupCheckpoint is a simple struct for managing the
type ConsumerGroupCheckpoint struct {
	Storage           CheckpointStorage
	kinesis           Kinesis
	LeaseDuration     time.Duration
	HeartBeatDuration time.Duration
	OwnerID           string
	done              chan struct{}
	currentLeases     map[string]*Lease //Initially, this will only be one
	Mutex             *sync.Mutex
}

func (cgc ConsumerGroupCheckpoint) Get(shardID string) (string, error) {
	return cgc.currentLeases[shardID].Checkpoint, nil
}

func (cgc ConsumerGroupCheckpoint) Set(shardID, sequenceNumber string) error {
	cgc.Mutex.Lock()
	defer cgc.Mutex.Unlock()

	cgc.currentLeases[shardID].Checkpoint = sequenceNumber
	return nil
}

func NewConsumerGroupCheckpoint(
	storage CheckpointStorage,
	kinesis Kinesis,
	leaseDuration time.Duration,
	heartBeatDuration time.Duration) *ConsumerGroupCheckpoint {
	return &ConsumerGroupCheckpoint{
		Storage:           storage,
		kinesis:           kinesis,
		LeaseDuration:     leaseDuration,
		HeartBeatDuration: heartBeatDuration,
		OwnerID:           uuid.NewV4().String(), // generated owner id
		done:              make(chan struct{}),
		currentLeases:     make(map[string]*Lease, 1),
		Mutex:             &sync.Mutex{},
	}

}

// Start is a blocking call that will attempt to acquire a lease on every tick of leaseDuration
// If a lease is successfully acquired it will be added to the channel otherwise it will continue to retry
func (cgc ConsumerGroupCheckpoint) Start(ctx context.Context, shardc chan string) {
	fmt.Printf("Starting ConsumerGroupCheckpoint for Consumer %s \n", cgc.OwnerID)

	tick := time.NewTicker(cgc.LeaseDuration)
	defer tick.Stop()
	var currentLeases map[string]Lease
	var previousLeases map[string]Lease

	for {
		select {
		case <-tick.C:
			if len(cgc.currentLeases) == 0 { // only do anything if there are no current leases
				fmt.Printf("Attempting to acquire lease for OwnerID=%s\n", cgc.OwnerID)
				var err error
				currentLeases, err = cgc.Storage.GetAllLeases()
				if err != nil {
					// TODO log this error
				}

				lease := cgc.CreateOrGetExpiredLease(currentLeases, previousLeases)
				if lease != nil && lease.LeaseKey != "" {
					cgc.currentLeases[lease.LeaseKey] = lease
					go cgc.heartbeatLoop(lease)
					shardc <- lease.LeaseKey
				}
				previousLeases = currentLeases
			}
		}
	}

}

// CreateOrGetExpiredLease is a helper function that tries checks to see if there are any leases available if not it tries to grab an "expired" lease where the heartbeat isn't updated.
func (cgc ConsumerGroupCheckpoint) CreateOrGetExpiredLease(currentLeases map[string]Lease, previousLeases map[string]Lease) *Lease {
	cgc.Mutex.Lock()
	defer cgc.Mutex.Unlock()

	listOfShards, err := cgc.kinesis.ListAllShards()
	if err != nil {
		//TODO log error
		// TODO return error
	}

	shardIDsNotYetTaken := getShardIDsNotLeased(listOfShards, currentLeases)
	var currentLease *Lease
	if len(shardIDsNotYetTaken) > 0 {
		fmt.Println("Grabbing lease from shardIDs not taken")
		shardId := shardIDsNotYetTaken[0] //grab the first one //TODO randomize
		tempLease := Lease{
			LeaseKey:       shardId,
			Checkpoint:     "0", // we don't have this yet
			LeaseCounter:   1,
			LeaseOwner:     cgc.OwnerID,
			HeartbeatID:    uuid.NewV4().String(),
			LastUpdateTime: time.Now(),
		}

		if err := cgc.Storage.CreateLease(tempLease); err != nil {
			fmt.Printf("Error is happening create the lease")
		} else {
			//success
			if isLeaseInvalidOrChanged(cgc, tempLease) {
				//Lease must have been acquired by another worker
				return nil
			}
			fmt.Printf("Successfully Acquired lease %v", tempLease)
			currentLease = &tempLease //successfully acquired the lease
		}

	}
	if currentLease == nil || currentLease.LeaseKey == "" && len(previousLeases) > 0 {
		for _, lease := range currentLeases {
			// TODO add some nil checking
			if currentLeases[lease.LeaseKey].HeartbeatID == previousLeases[lease.LeaseKey].HeartbeatID { //we assume the lease was not updated during the amount of time
				updatedLease := Lease{
					LeaseKey:       lease.LeaseKey,
					Checkpoint:     lease.Checkpoint,
					LeaseCounter:   lease.LeaseCounter + 1,
					LeaseOwner:     cgc.OwnerID,
					HeartbeatID:    uuid.NewV4().String(),
					LastUpdateTime: time.Now(),
				}
				if err := cgc.Storage.UpdateLease(lease, updatedLease); err != nil {
					fmt.Printf("Error is happening updating the lease")
				} else {
					if isLeaseInvalidOrChanged(cgc, lease) {
						return nil //should not be a valid lease at this point
					}
					fmt.Printf("Successfully Acquired Expired lease %v\n", lease)
					currentLease = &updatedLease //successfully acquired the lease
					break
				}
			}
		}
	}
	return currentLease
}

// heartbeatLoop should constantly update the lease that is provided
func (cgc ConsumerGroupCheckpoint) heartbeatLoop(lease *Lease) {
	cgc.Mutex.Lock()
	defer cgc.Mutex.Unlock()
	fmt.Println("Starting heartbeat loop")
	ticker := time.NewTicker(cgc.HeartBeatDuration)
	defer ticker.Stop()
	defer close(cgc.done)
	for {
		select {
		case <-ticker.C:

			if isLeaseInvalidOrChanged(cgc, *lease) || lease.IsExpired(cgc.LeaseDuration) {
				delete(cgc.currentLeases, lease.LeaseKey)
			}
			updatedLease := Lease{
				LeaseKey:       lease.LeaseKey,
				Checkpoint:     lease.Checkpoint,
				LeaseCounter:   lease.LeaseCounter,
				LeaseOwner:     lease.LeaseOwner,
				HeartbeatID:    uuid.NewV4().String(),
				LastUpdateTime: time.Now(),
			}
			// TODO handle error
			cgc.Storage.UpdateLease(*lease, updatedLease)
			lease = &updatedLease
			fmt.Printf("Sucessfully updated lease %v\n", lease)
		case <-cgc.done:
			return
		}
	}
}

// isLeaseInvalidOrChanged checks to see if the lease changed
func isLeaseInvalidOrChanged(cgc ConsumerGroupCheckpoint, lease Lease) bool {
	leaseCurrent, _ := cgc.Storage.GetLease(lease.LeaseKey)
	if lease.LeaseKey != leaseCurrent.LeaseKey || cgc.OwnerID != leaseCurrent.LeaseOwner || leaseCurrent.LeaseCounter != lease.LeaseCounter {
		fmt.Printf("The lease changed\n")
		return true
	}
	return false
}

// getShardIDsNotLeased finds any open shards where there are no leases yet created
func getShardIDsNotLeased(shardIDs []string, leases map[string]Lease) []string {
	var shardIDsNotUsed []string
	for _, shardID := range shardIDs {
		if _, ok := leases[shardID]; !ok {
			shardIDsNotUsed = append(shardIDsNotUsed, shardID)
		}
	}
	return shardIDsNotUsed
}
