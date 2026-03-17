// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// Stmt implements driver.Stmt interface
type Stmt struct {
	conn          *Conn
	query         string
	stmtID        uint32 // 0xFFFFFFFF = not yet prepared (pipeline sentinel)
	paramCount    uint16
	columnCount   uint16
	cachedColumns []protocol.ColumnDefinition // cached for CACHE_METADATA
}

// Close closes the statement
func (s *Stmt) Close() error {
	if s.stmtID == 0xFFFFFFFF {
		return nil
	}

	if s.conn.client.IsClosed() {
		return driver.ErrBadConn
	}

	// COM_STMT_CLOSE doesn't send a response
	return s.conn.client.Send(protocol.NewStmtClose(s.conn.client.WriterBuf(), s.stmtID))
}

// NumInput returns the number of placeholder parameters.
// Returns -1 when the statement is not yet prepared (pipelined mode).
func (s *Stmt) NumInput() int {
	if s.stmtID != 0xFFFFFFFF {
		return int(s.paramCount)
	}
	return -1
}

// Exec executes a query that doesn't return rows
func (s *Stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), valuesToNamedValues(args))
}

// ExecContext executes a query that doesn't return rows
func (s *Stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	if s.conn.client.IsClosed() {
		return nil, driver.ErrBadConn
	}
	comp, err := s.execute(ctx, args)
	if err != nil {
		return nil, err
	}
	if comp.HasResultSet() {
		s.cachedColumns = comp.Columns
	}
	return comp, nil
}

// Query executes a query that may return rows
func (s *Stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), valuesToNamedValues(args))
}

// QueryContext executes a query that may return rows
func (s *Stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	if s.conn.client.IsClosed() {
		return nil, driver.ErrBadConn
	}
	comp, err := s.execute(ctx, args)
	if err != nil {
		return nil, err
	}
	s.cachedColumns = comp.Columns
	if !comp.HasResultSet() {
		return nil, fmt.Errorf("query did not return a result set")
	}
	rows := &Rows{
		conn:    s.conn,
		current: comp,
	}
	if !comp.Loaded {
		s.conn.client.SetActiveRows(rows)
	}
	return rows, nil
}

// execute builds and sends COM_STMT_EXECUTE (pipelining COM_STMT_PREPARE first
// when stmtID == 0xFFFFFFFF) and reads the server response.
// Each command is sent with its own sequence reset to 0; after reading the
// prepare response StartNextRead resets the sequence to 1 so ReadCompletion
// correctly reads the execute response.
func (s *Stmt) execute(ctx context.Context, args []driver.NamedValue) (*protocol.Completion, error) {
	stop, err := s.conn.client.WithContext(ctx)
	if err != nil {
		return nil, driver.ErrBadConn
	}
	defer stop()

	if s.stmtID == 0xFFFFFFFF {
		// Pipeline: send prepare (seq=0), then after it's written to socket,
		// reuse the buffer to build and send execute (seq=0).
		if err := s.conn.client.Send(protocol.NewPrepare(s.conn.client.WriterBuf(), s.query)); err != nil {
			return nil, err
		}
		execPkt, err := protocol.NewExecute(s.conn.client.WriterBuf(), 0xFFFFFFFF, args)
		if err != nil {
			return nil, err
		}
		if err := s.conn.client.Send(execPkt); err != nil {
			return nil, err
		}
		// Read the prepare response first.
		stmtID, paramCount, columnCount, columns, err := s.conn.client.ReadPrepareResponse()
		if err != nil {
			return nil, err
		}
		s.stmtID = stmtID
		s.paramCount = paramCount
		s.columnCount = columnCount
		s.cachedColumns = columns
		// Reset sequence to 1 so the reader expects the execute response's first packet.
		s.conn.client.StartNextRead()
	} else {
		execPkt, err := protocol.NewExecute(s.conn.client.WriterBuf(), s.stmtID, args)
		if err != nil {
			return nil, err
		}
		if err := s.conn.client.Send(execPkt); err != nil {
			return nil, err
		}
	}

	comp, err := s.conn.client.ReadCompletion(true, s.cachedColumns)
	if err != nil {
		return nil, err
	}
	s.cachedColumns = comp.Columns
	return comp, nil
}

// prepareInternal sends COM_STMT_PREPARE and reads the server response.
func (s *Stmt) prepareInternal() error {
	if err := s.conn.client.Send(protocol.NewPrepare(s.conn.client.WriterBuf(), s.query)); err != nil {
		return err
	}
	stmtID, paramCount, columnCount, columns, err := s.conn.client.ReadPrepareResponse()
	if err != nil {
		return err
	}
	s.stmtID = stmtID
	s.paramCount = paramCount
	s.columnCount = columnCount
	s.cachedColumns = columns
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
