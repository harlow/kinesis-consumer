package consumer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/twinj/uuid"

	"github.com/harlow/kinesis-consumer/storage"
)

// TODO change logging to actual logger

// ConsumerGroupCheckpoint is a simple struct for managing the consumergroup and heartbeat of updating leases
type ConsumerGroupCheckpoint struct {
	HeartBeatDuration time.Duration
	LeaseDuration     time.Duration
	OwnerID           string
	Storage           Storage
	done              chan struct{}
	kinesis           Kinesis

	leasesMutex *sync.Mutex
	leases      map[string]*storage.Lease // Initially, this will only be one
}

func (cgc ConsumerGroupCheckpoint) Get(shardID string) (string, error) {
	return cgc.leases[shardID].Checkpoint, nil
}

func (cgc ConsumerGroupCheckpoint) Set(shardID, sequenceNumber string) error {
	cgc.leasesMutex.Lock()
	defer cgc.leasesMutex.Unlock()

	cgc.leases[shardID].Checkpoint = sequenceNumber
	return nil
}

func NewConsumerGroupCheckpoint(
	Storage Storage,
	kinesis Kinesis,
	leaseDuration time.Duration,
	heartBeatDuration time.Duration) *ConsumerGroupCheckpoint {
	return &ConsumerGroupCheckpoint{
		HeartBeatDuration: heartBeatDuration,
		LeaseDuration:     leaseDuration,
		OwnerID:           uuid.NewV4().String(), // generated owner id
		Storage:           Storage,
		done:              make(chan struct{}),
		kinesis:           kinesis,

		leasesMutex: &sync.Mutex{},
		leases:      make(map[string]*storage.Lease, 1),
	}

}

// Start is a blocking call that will attempt to acquire a lease on every tick of leaseDuration
// If a lease is successfully acquired it will be added to the channel otherwise it will continue to retry
func (cgc ConsumerGroupCheckpoint) Start(ctx context.Context, shardc chan string) {
	fmt.Printf("Starting ConsumerGroupCheckpoint for Consumer %s \n", cgc.OwnerID)

	tick := time.NewTicker(cgc.LeaseDuration)
	defer tick.Stop()
	var currentLeases map[string]storage.Lease
	var previousLeases map[string]storage.Lease

	for {
		select {
		case <-tick.C:
			if len(cgc.leases) > 0 { // only do anything if there are no current leases
				continue
			}
			fmt.Printf("Attempting to acquire lease for OwnerID=%s\n", cgc.OwnerID)
			var err error
			currentLeases, err = cgc.Storage.GetAllLeases()
			if err != nil {
				// TODO log this error
			}

			lease := cgc.CreateOrGetExpiredLease(currentLeases, previousLeases)
			previousLeases = currentLeases

			if lease == nil || lease.LeaseKey == "" {
				continue // lease wasn't acquired continue
			}
			// lease sucessfully acquired
			// start the heartbeat and send back the shardID on the channel
			cgc.leases[lease.LeaseKey] = lease
			go cgc.heartbeatLoop(lease)
			shardc <- lease.LeaseKey
		}
	}

}

// CreateOrGetExpiredLease is a helper function that tries checks to see if there are any leases available if not it tries to grab an "expired" lease where the heartbeat isn't updated.
func (cgc ConsumerGroupCheckpoint) CreateOrGetExpiredLease(currentLeases map[string]storage.Lease, previousLeases map[string]storage.Lease) *storage.Lease {
	cgc.leasesMutex.Lock()
	defer cgc.leasesMutex.Unlock()

	listOfShards, err := cgc.kinesis.ListAllShards()
	if err != nil {
		//TODO log error
		// TODO return error
	}

	shardIDsNotYetTaken := getShardIDsNotLeased(listOfShards, currentLeases)
	var currentLease *storage.Lease
	if len(shardIDsNotYetTaken) > 0 {
		fmt.Println("Grabbing lease from shardIDs not taken")
		shardId := shardIDsNotYetTaken[0] //grab the first one //TODO randomize
		tempLease := storage.Lease{
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
				updatedLease := storage.Lease{
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
func (cgc ConsumerGroupCheckpoint) heartbeatLoop(lease *storage.Lease) {
	cgc.leasesMutex.Lock()
	defer cgc.leasesMutex.Unlock()
	fmt.Println("Starting heartbeat loop")
	ticker := time.NewTicker(cgc.HeartBeatDuration)
	defer ticker.Stop()
	defer close(cgc.done)
	for {
		select {
		case <-ticker.C:

			if isLeaseInvalidOrChanged(cgc, *lease) || lease.IsExpired(cgc.LeaseDuration) {
				delete(cgc.leases, lease.LeaseKey)
			}
			updatedLease := storage.Lease{
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
func isLeaseInvalidOrChanged(cgc ConsumerGroupCheckpoint, lease storage.Lease) bool {
	leaseCurrent, _ := cgc.Storage.GetLease(lease.LeaseKey)
	if lease.LeaseKey != leaseCurrent.LeaseKey || cgc.OwnerID != leaseCurrent.LeaseOwner || leaseCurrent.LeaseCounter != lease.LeaseCounter {
		fmt.Printf("The lease changed\n")
		return true
	}
	return false
}

// getShardIDsNotLeased finds any open shards where there are no leases yet created
func getShardIDsNotLeased(shardIDs []string, leases map[string]storage.Lease) []string {
	var shardIDsNotUsed []string
	for _, shardID := range shardIDs {
		if _, ok := leases[shardID]; !ok {
			shardIDsNotUsed = append(shardIDsNotUsed, shardID)
		}
	}
	return shardIDsNotUsed
}
