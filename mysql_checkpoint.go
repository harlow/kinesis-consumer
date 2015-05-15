package connector

import (
	"database/sql"
	"fmt"

	_ "github.com/go-sql-driver/mysql"
)

// MysqlCheckpoint implements the Checkpont interface.
// This class is used to enable the Pipeline.ProcessShard to checkpoint their progress.
type MysqlCheckpoint struct {
	AppName    string
	StreamName string
	TableName  string
	Db         *sql.DB

	sequenceNumber string
}

// CheckpointExists determines if a checkpoint for a particular Shard exists.
// Typically used to determine whether we should start processing the shard with
// TRIM_HORIZON or AFTER_SEQUENCE_NUMBER (if checkpoint exists).
func (c *MysqlCheckpoint) CheckpointExists(shardID string) bool {

	logger.Printf("SELECT sequence_number FROM " + c.TableName + " WHERE checkpoint_key = ?")

	row := c.Db.QueryRow("SELECT sequence_number FROM "+c.TableName+" WHERE checkpoint_key = ?", c.key(shardID))
	var val string
	err := row.Scan(&val)
	if err == nil {
		logger.Printf("sequence:%s", val)
		c.sequenceNumber = val
		return true
	}

	if err == sql.ErrNoRows {
		return false
	}

	// something bad happened, better blow up the process
	panic(err)
}

// SequenceNumber returns the current checkpoint stored for the specified shard.
func (c *MysqlCheckpoint) SequenceNumber() string {
	return c.sequenceNumber
}

// SetCheckpoint stores a checkpoint for a shard (e.g. sequence number of last record processed by application).
// Upon failover, record processing is resumed from this point.
func (c *MysqlCheckpoint) SetCheckpoint(shardID string, sequenceNumber string) {

	_, err := c.Db.Exec("INSERT INTO "+c.TableName+" (sequence_number, checkpoint_key) VALUES (?, ?) ON DUPLICATE KEY UPDATE sequence_number = ?", sequenceNumber, c.key(shardID), sequenceNumber)
	if err != nil {
		panic(err)
	}
	c.sequenceNumber = sequenceNumber

}

// key generates a unique mysql key for storage of Checkpoint.
func (c *MysqlCheckpoint) key(shardID string) string {
	return fmt.Sprintf("%v:checkpoint:%v:%v", c.AppName, c.StreamName, shardID)
}
