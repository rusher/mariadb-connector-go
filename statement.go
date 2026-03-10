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

	// Send COM_STMT_CLOSE
	packet := make([]byte, 5)
	packet[0] = protocol.COM_STMT_CLOSE
	protocol.PutUint32(packet[1:], s.stmtID)

	// COM_STMT_CLOSE doesn't send a response
	return s.conn.client.SendCommand(packet)
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

	// Prepare statement if not already prepared
	if !s.prepared {
		if err := s.prepareInternal(); err != nil {
			return nil, err
		}
	}

	// Build COM_STMT_EXECUTE packet
	packet, err := protocol.BuildStmtExecutePacket(s.stmtID, args)
	if err != nil {
		return nil, err
	}

	// Send command (resets sequence)
	if err := s.conn.client.SendCommand(packet); err != nil {
		return nil, err
	}

	// Read response
	response, err := s.conn.client.Reader().ReadPacket()
	if err != nil {
		return nil, err
	}

	// Parse result
	result, err := protocol.ParseResultPacket(response)
	if err != nil {
		return nil, err
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

	// Prepare statement if not already prepared
	if !s.prepared {
		if err := s.prepareInternal(); err != nil {
			return nil, err
		}
	}

	// Build COM_STMT_EXECUTE packet
	packet, err := protocol.BuildStmtExecutePacket(s.stmtID, args)
	if err != nil {
		return nil, err
	}

	// Send command (resets sequence)
	if err := s.conn.client.SendCommand(packet); err != nil {
		return nil, err
	}

	// Read column count or result
	columnCountData, err := s.conn.client.Reader().ReadPacket()
	if err != nil {
		return nil, err
	}

	// Check for error
	if len(columnCountData) > 0 && columnCountData[0] == 0xff {
		return nil, protocol.ParseErrorPacket(columnCountData)
	}

	// Check for OK packet (no result set)
	if len(columnCountData) > 0 && columnCountData[0] == 0x00 {
		return &Rows{columns: []*protocol.ColumnDefinition{}, binary: true}, nil
	}

	// Parse column count
	columnCount, _, err := protocol.ReadLengthEncodedInteger(columnCountData, 0)
	if err != nil {
		return nil, err
	}

	// Read column definitions
	columns := make([]*protocol.ColumnDefinition, columnCount)
	for i := 0; i < int(columnCount); i++ {
		colData, err := s.conn.client.Reader().ReadPacket()
		if err != nil {
			return nil, err
		}

		col, err := protocol.ParseColumnDefinition(colData)
		if err != nil {
			return nil, err
		}
		columns[i] = col
	}

	// Read EOF packet (if not using CLIENT_DEPRECATE_EOF)
	if !s.conn.context.IsEOFDeprecated() {
		_, err := s.conn.client.Reader().ReadPacket()
		if err != nil {
			return nil, err
		}
	}

	return &Rows{
		conn:    s.conn,
		columns: columns,
		binary:  true,
	}, nil
}

// prepareInternal prepares the statement (must be called with conn.mu locked)
func (s *Stmt) prepareInternal() error {
	// Build COM_STMT_PREPARE packet
	packet := make([]byte, 1+len(s.query))
	packet[0] = protocol.COM_STMT_PREPARE
	copy(packet[1:], s.query)

	// Send command (resets sequence)
	if err := s.conn.client.SendCommand(packet); err != nil {
		return err
	}

	// Read response
	response, err := s.conn.client.Reader().ReadPacket()
	if err != nil {
		return err
	}

	// Check for error
	if len(response) > 0 && response[0] == 0xff {
		return protocol.ParseErrorPacket(response)
	}

	// Parse prepare OK packet
	if len(response) < 12 {
		return fmt.Errorf("invalid prepare response")
	}

	if response[0] != 0x00 {
		return fmt.Errorf("unexpected prepare response")
	}

	s.stmtID = protocol.GetUint32(response[1:])
	s.columnCount = protocol.GetUint16(response[5:])
	s.paramCount = protocol.GetUint16(response[7:])

	// Read parameter definitions if any
	if s.paramCount > 0 {
		for i := 0; i < int(s.paramCount); i++ {
			_, err := s.conn.client.Reader().ReadPacket()
			if err != nil {
				return err
			}
		}

		// Read EOF packet (if not using CLIENT_DEPRECATE_EOF)
		if !s.conn.context.IsEOFDeprecated() {
			_, err := s.conn.client.Reader().ReadPacket()
			if err != nil {
				return err
			}
		}
	}

	// Read column definitions if any
	if s.columnCount > 0 {
		for i := 0; i < int(s.columnCount); i++ {
			_, err := s.conn.client.Reader().ReadPacket()
			if err != nil {
				return err
			}
		}

		// Read EOF packet (if not using CLIENT_DEPRECATE_EOF)
		if !s.conn.context.IsEOFDeprecated() {
			_, err := s.conn.client.Reader().ReadPacket()
			if err != nil {
				return err
			}
		}
	}

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
