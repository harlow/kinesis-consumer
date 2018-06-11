package postgres

import (
	"database/sql"
	"fmt"
	"sync"
	"time"
	// this is the postgres package so it makes sense to be here
	_ "github.com/lib/pq"
)

// consumer represents the data that will be stored about the checkpoint
type consumer struct {
	Checkpoint JSONMap `db:"checkpoint"`
}

const getCheckpointQuery = `
							SELECT checkpoint 
							FROM %s
							WHERE checkpoint @> '{"namespace":"%s", "shard_id":"%s"}'`

const upsertCheckpoint = `INSERT INTO %s 
						  VALUES('{"namespace": "%s", "shard_id": "%s", "sequence_number":"%s"}')
						  ON CONFLICT ((checkpoint->>'namespace'),(checkpoint->>'shard_id'))
						  DO 
						  UPDATE 
						  SET checkpoint = %[1]s.checkpoint::jsonb || '{"sequence_number":"%[4]s"}'`

type key struct {
	streamName string
	shardID    string
}

// Option is used to override defaults when creating a new Checkpoint
type Option func(*Checkpoint)

// Checkpoint stores and retreives the last evaluated key from a DDB scan
type Checkpoint struct {
	appName     string
	tableName   string
	conn        *sql.DB
	mu          *sync.Mutex // protects the checkpoints
	done        chan struct{}
	checkpoints map[key]string
	maxInterval time.Duration
}

// New returns a checkpoint that uses DynamoDB for underlying storage
// Using connectionStr turn it more flexible to use specific db configs
func New(appName, tableName, connectionStr string, opts ...Option) (*Checkpoint, error) {

	conn, err := sql.Open("postgres", connectionStr)

	if err != nil {
		return nil, err
	}

	ck := &Checkpoint{
		conn:        conn,
		appName:     appName,
		tableName:   tableName,
		done:        make(chan struct{}),
		maxInterval: time.Duration(1 * time.Minute),
		mu:          new(sync.Mutex),
		checkpoints: map[key]string{},
	}

	for _, opt := range opts {
		opt(ck)
	}

	go ck.loop()

	return ck, nil
}

// Get determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *Checkpoint) Get(streamName, shardID string) (string, error) {
	namespace := fmt.Sprintf("%s-%s", c.appName, streamName)
	consumer := consumer{}
	// using fmt instead of using args in order to be able to use quotes surrounding the param
	query := fmt.Sprintf(getCheckpointQuery, c.tableName, namespace, shardID)

	err := c.conn.QueryRow(query).Scan(&consumer.Checkpoint)

	if err != nil {
		if err == sql.ErrNoRows {
			return "", nil
		}

		return "", err
	}

	return consumer.Checkpoint["sequence_number"].(string), nil
}

// Set stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *Checkpoint) Set(streamName, shardID, sequenceNumber string) error {
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

// Shutdown the checkpoint. Save any in-flight data.
func (c *Checkpoint) Shutdown() error {
	defer c.conn.Close()

	c.done <- struct{}{}

	return c.save()
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
	c.mu.Lock()
	defer c.mu.Unlock()

	for key, sequenceNumber := range c.checkpoints {
		// using fmt instead of using args in order to be able to use quotes surrounding the param
		upsertStatement := fmt.Sprintf(upsertCheckpoint, c.tableName, fmt.Sprintf("%s-%s", c.appName, key.streamName), key.shardID, sequenceNumber)

		_, err := c.conn.Exec(upsertStatement)

		return err
	}

	return nil
}
