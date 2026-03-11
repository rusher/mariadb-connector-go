// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
	clientpkt "github.com/mariadb-connector-go/mariadb/internal/protocol/client"
)

// Stmt implements driver.Stmt interface
type Stmt struct {
	conn        *Conn
	query       string
	stmtID      uint32
	paramCount  uint16
	columnCount uint16
	prepared    bool
}

// Close closes the statement
func (s *Stmt) Close() error {
	if !s.prepared {
		return nil
	}

	s.conn.client.Lock()
	defer s.conn.client.Unlock()

	if s.conn.client.IsClosed() {
		return driver.ErrBadConn
	}

	// COM_STMT_CLOSE doesn't send a response
	return s.conn.client.Send(clientpkt.NewStmtClose(s.stmtID))
}

// NumInput returns the number of placeholder parameters
func (s *Stmt) NumInput() int {
	if s.prepared {
		return int(s.paramCount)
	}
	// For unprepared statements, return -1 to indicate unknown
	return -1
}

// Exec executes a query that doesn't return rows
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

// ExecContext executes a query that doesn't return rows
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	s.conn.client.Lock()
	defer s.conn.client.Unlock()

	if s.conn.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	execPkt, err := clientpkt.NewExecute(s.stmtID, args)
	if err != nil {
		return nil, err
	}

	if !s.prepared {
		if err := s.prepareAndExecute(execPkt); err != nil {
			return nil, err
		}
	} else {
		if err := s.conn.client.Send(execPkt); err != nil {
			return nil, err
		}
	}

	completions, err := s.conn.client.ReadCompletions(true, s.conn.client.FetchSize())
	if err != nil {
		return nil, err
	}

	var result *protocol.Completion
	for _, comp := range completions {
		if !comp.HasResultSet() {
			result = comp
		}
	}
	if result == nil {
		result = &protocol.Completion{}
	}
	return result, nil
}

// Query executes a query that may return rows
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that may return rows
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	s.conn.client.Lock()
	defer s.conn.client.Unlock()

	if s.conn.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	execPkt, err := clientpkt.NewExecute(s.stmtID, args)
	if err != nil {
		return nil, err
	}

	if !s.prepared {
		if err := s.prepareAndExecute(execPkt); err != nil {
			return nil, err
		}
	} else {
		if err := s.conn.client.Send(execPkt); err != nil {
			return nil, err
		}
	}

	completions, err := s.conn.client.ReadCompletions(true, s.conn.client.FetchSize())
	if err != nil {
		return nil, err
	}

	// Error if no completion returned a result set
	hasResultSet := false
	for _, comp := range completions {
		if comp.HasResultSet() {
			hasResultSet = true
			break
		}
	}
	if !hasResultSet {
		return nil, fmt.Errorf("query did not return a result set")
	}

	rows := &Rows{
		conn:        s.conn,
		completions: completions,
	}
	if !completions[0].Loaded {
		s.conn.client.SetActiveRows(rows)
	}
	return rows, nil
}

// prepareInternal sends COM_STMT_PREPARE and reads the server response.
// Must be called with conn.mu locked.
func (s *Stmt) prepareInternal() error {
	if err := s.conn.client.Send(clientpkt.NewPrepare(s.query)); err != nil {
		return err
	}
	stmtID, paramCount, columnCount, err := s.conn.client.ReadPrepareResponse()
	if err != nil {
		return err
	}
	s.stmtID = stmtID
	s.paramCount = paramCount
	s.columnCount = columnCount
	s.prepared = true
	return nil
}

// prepareAndExecute pipelines COM_STMT_PREPARE + COM_STMT_EXECUTE using
// stmtID=0xFFFFFFFF as the sentinel value (MariaDB STMT_BULK_OPERATIONS).
// Only called when CanPipelinePrepare() is true.
// Must be called with conn.mu locked.
func (s *Stmt) prepareAndExecute(execPkt []byte) error {
	clientpkt.SetStmtID(execPkt, 0xFFFFFFFF)
	if err := s.conn.client.Send(clientpkt.NewPrepare(s.query)); err != nil {
		return err
	}
	if err := s.conn.client.SendNext(execPkt); err != nil {
		return err
	}
	stmtID, paramCount, columnCount, err := s.conn.client.ReadPrepareResponse()
	if err != nil {
		return err
	}
	s.stmtID = stmtID
	s.paramCount = paramCount
	s.columnCount = columnCount
	s.prepared = true
	return nil
}

// valuesToNamedValues converts []driver.Value to []driver.NamedValue
func valuesToNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for i, v := range values {
		namedValues[i] = driver.NamedValue{
			Ordinal: i + 1,
			Value:   v,
		}
	}
	return namedValues
}
