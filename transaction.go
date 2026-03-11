// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"strings"
)

// escapeIdentifier escapes a SQL identifier by wrapping it in backticks
// and escaping any backticks within the identifier by doubling them
func escapeIdentifier(name string) string {
	// Replace ` with ``
	escaped := strings.ReplaceAll(name, "`", "``")
	return "`" + escaped + "`"
}

// Tx implements driver.Tx interface
type Tx struct {
	conn *Conn
}

// Commit commits the transaction
func (tx *Tx) Commit() error {
	if tx.conn.client.IsClosed() {
		return driver.ErrBadConn
	}

	return tx.conn.execInternal(context.Background(), "COMMIT")
}

// Rollback aborts the transaction
func (tx *Tx) Rollback() error {
	if tx.conn.client.IsClosed() {
		return driver.ErrBadConn
	}

	return tx.conn.execInternal(context.Background(), "ROLLBACK")
}

// Savepoint creates a savepoint with the given name
func (tx *Tx) Savepoint(name string) error {
	if tx.conn.client.IsClosed() {
		return driver.ErrBadConn
	}

	query := fmt.Sprintf("SAVEPOINT %s", escapeIdentifier(name))
	return tx.conn.execInternal(context.Background(), query)
}

// RollbackToSavepoint rolls back to the specified savepoint
func (tx *Tx) RollbackToSavepoint(name string) error {
	if tx.conn.client.IsClosed() {
		return driver.ErrBadConn
	}

	query := fmt.Sprintf("ROLLBACK TO SAVEPOINT %s", escapeIdentifier(name))
	return tx.conn.execInternal(context.Background(), query)
}

// ReleaseSavepoint releases the specified savepoint
func (tx *Tx) ReleaseSavepoint(name string) error {
	if tx.conn.client.IsClosed() {
		return driver.ErrBadConn
	}

	query := fmt.Sprintf("RELEASE SAVEPOINT %s", escapeIdentifier(name))
	return tx.conn.execInternal(context.Background(), query)
}
