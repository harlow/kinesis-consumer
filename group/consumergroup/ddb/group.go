package ddb

import (
	"errors"
	"time"

	consumergroup "github.com/harlow/kinesis-consumer/group/consumergroup"
)

// GroupConfig configures an opt-in DynamoDB-backed consumer group.
type GroupConfig struct {
	GroupName       string // preferred
	AppName         string // deprecated alias for GroupName
	StreamName      string
	WorkerID        string
	KinesisClient   consumergroup.KinesisClient
	Repository      Config // dynamodb client + lease table
	CheckpointStore consumergroup.CheckpointStore

	LeaseDuration      time.Duration
	RenewInterval      time.Duration
	AssignInterval     time.Duration
	MaxLeasesForWorker int
	Clock              consumergroup.Clock
}

// NewGroup builds a consumergroup.Group backed by a DynamoDB lease repository.
func NewGroup(cfg GroupConfig) (*consumergroup.Group, error) {
	if cfg.KinesisClient == nil {
		return nil, errors.New("kinesis client is required")
	}

	repo, err := New(cfg.Repository)
	if err != nil {
		return nil, err
	}

	return consumergroup.New(consumergroup.Config{
		GroupName:          cfg.GroupName,
		AppName:            cfg.AppName,
		StreamName:         cfg.StreamName,
		WorkerID:           cfg.WorkerID,
		KinesisClient:      cfg.KinesisClient,
		Repository:         repo,
		CheckpointStore:    cfg.CheckpointStore,
		LeaseDuration:      cfg.LeaseDuration,
		RenewInterval:      cfg.RenewInterval,
		AssignInterval:     cfg.AssignInterval,
		MaxLeasesForWorker: cfg.MaxLeasesForWorker,
		Clock:              cfg.Clock,
	})
}
