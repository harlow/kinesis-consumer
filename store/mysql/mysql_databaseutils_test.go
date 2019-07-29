package mysql

import "database/sql"

func (c *Checkpoint) SetConn(conn *sql.DB) {
	c.conn = conn
}
