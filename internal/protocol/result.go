// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import "database/sql/driver"

// ServerMessage represents a message from the server
type ServerMessage interface {
	GetSequence() uint8
}

// ClientMessage represents a message to the server
type ClientMessage interface {
	Encode() ([]byte, error)
}

// Completion represents the result of a query execution.
// Returned by both OK and EOF packets, and by result sets
// (which are terminated by an OK or EOF packet).
type Completion struct {
	// From OK/EOF packets
	AffectedRows int64
	InsertID     int64
	WarningCount uint16
	ServerStatus uint16
	Message      string

	// Populated for result sets
	Columns   []ColumnDefinition
	Binary    bool
	ParsedRow []driver.Value // pre-parsed row 0 (always set when result set has ≥1 rows)
	Loaded    bool           // true = terminator received; false = rows still on the wire
}

// HasResultSet returns true if this completion carries a result set
func (c *Completion) HasResultSet() bool {
	return len(c.Columns) > 0
}

// LastInsertId implements driver.Result
func (c *Completion) LastInsertId() (int64, error) {
	return c.InsertID, nil
}

// RowsAffected implements driver.Result
func (c *Completion) RowsAffected() (int64, error) {
	return c.AffectedRows, nil
}
