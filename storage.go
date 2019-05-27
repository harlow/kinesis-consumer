package consumer

import "github.com/harlow/kinesis-consumer/storage"

// Storage is a simple interface for abstracting away the storage functions
type Storage interface {
	CreateLease(lease storage.Lease) error
	UpdateLease(originalLease, updatedLease storage.Lease) error
	GetLease(leaseKey string) (*storage.Lease, error)
	GetAllLeases() (map[string]storage.Lease, error)
}
