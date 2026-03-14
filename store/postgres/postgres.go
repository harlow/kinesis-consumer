package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	// this is the postgres package so it makes sense to be here
	_ "github.com/lib/pq"
)

type key struct {
	streamName string
	shardID    string
}

// Option is used to override defaults when creating a new Checkpoint
type Option func(*Checkpoint)

// WithMaxInterval sets the flush interval
func WithMaxInterval(maxInterval time.Duration) Option {
	return func(c *Checkpoint) {
		c.maxInterval = maxInterval
	}
}

// Checkpoint stores and retrieves the last evaluated key from a DDB scan
type Checkpoint struct {
	appName     string
	tableName   string
	conn        *sql.DB
	mu          *sync.Mutex // protects the checkpoints
	done        chan struct{}
	checkpoints map[key]string
	maxInterval time.Duration
}

// New returns a checkpoint that uses PostgresDB for underlying storage
// Using connectionStr turn it more flexible to use specific db configs
func New(appName, tableName, connectionStr string, opts ...Option) (*Checkpoint, error) {
	if appName == "" {
		return nil, errors.New("application name not defined")
	}

	if tableName == "" {
		return nil, errors.New("table name not defined")
	}

	conn, err := sql.Open("postgres", connectionStr)

	if err != nil {
		return nil, err
	}

	ck := &Checkpoint{
		conn:        conn,
		appName:     appName,
		tableName:   tableName,
		done:        make(chan struct{}),
		maxInterval: 1 * time.Minute,
		mu:          new(sync.Mutex),
		checkpoints: map[key]string{},
	}

	for _, opt := range opts {
		opt(ck)
	}

	go ck.loop()

	return ck, nil
}

// GetMaxInterval returns the maximum interval before the checkpoint
func (c *Checkpoint) GetMaxInterval() time.Duration {
	return c.maxInterval
}

// GetCheckpoint determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *Checkpoint) GetCheckpoint(streamName, shardID string) (string, error) {
	namespace := fmt.Sprintf("%s-%s", c.appName, streamName)

	var sequenceNumber string
	getCheckpointQuery := fmt.Sprintf(`SELECT sequence_number FROM %s WHERE namespace=$1 AND shard_id=$2;`, c.tableName) //nolint: gas, it replaces only the table name
	err := c.conn.QueryRow(getCheckpointQuery, namespace, shardID).Scan(&sequenceNumber)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}
		return "", err
	}

	return sequenceNumber, nil
}

// SetCheckpoint stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) SetCheckpoint(streamName, shardID, sequenceNumber string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if sequenceNumber == "" {
		return fmt.Errorf("sequence number should not be empty")
	}

	key := key{
		streamName: streamName,
		shardID:    shardID,
	}

	c.checkpoints[key] = sequenceNumber

	return nil
}

// Flush saves any buffered checkpoints without closing the store.
func (c *Checkpoint) Flush() error {
	return c.save()
}

// Shutdown the checkpoint. Save any in-flight data.
func (c *Checkpoint) Shutdown() error {
	defer c.conn.Close()

	c.done <- struct{}{}

	return c.Flush()
}

func (c *Checkpoint) loop() {
	tick := time.NewTicker(c.maxInterval)
	defer tick.Stop()
	defer close(c.done)

	for {
		select {
		case <-tick.C:
			c.save()
		case <-c.done:
			return
		}
	}
}

func (c *Checkpoint) save() error {
	//nolint: gas, it replaces only the table name
	upsertCheckpoint := fmt.Sprintf(`INSERT INTO %s (namespace, shard_id, sequence_number)
					    VALUES($1, $2, $3)
						ON CONFLICT (namespace, shard_id)
						DO
						UPDATE
						SET sequence_number= $3;`, c.tableName)

	pending := c.drainCheckpoints()
	for key, sequenceNumber := range pending {
		if _, err := c.conn.Exec(upsertCheckpoint, fmt.Sprintf("%s-%s", c.appName, key.streamName), key.shardID, sequenceNumber); err != nil {
			c.restoreCheckpoints(pending)
			return err
		}
	}

	return nil
}

func (c *Checkpoint) drainCheckpoints() map[key]string {
	c.mu.Lock()
	defer c.mu.Unlock()

	pending := make(map[key]string, len(c.checkpoints))
	for checkpointKey, sequenceNumber := range c.checkpoints {
		pending[checkpointKey] = sequenceNumber
	}
	c.checkpoints = map[key]string{}
	return pending
}

func (c *Checkpoint) restoreCheckpoints(pending map[key]string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for checkpointKey, sequenceNumber := range pending {
		if _, exists := c.checkpoints[checkpointKey]; exists {
			continue
		}
		c.checkpoints[checkpointKey] = sequenceNumber
	}
}
