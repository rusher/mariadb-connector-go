// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
	"github.com/mariadb-connector-go/mariadb/internal/protocol/server"
)

// Rows implements driver.Rows interface
// Uses streaming mode by default - fetches rows on demand
// If a new command is issued, remaining rows are buffered into memory
type Rows struct {
	conn    *Conn
	columns []*protocol.ColumnDefinition
	binary  bool // true for binary protocol (prepared statements)

	// Buffering for when connection is needed for another command
	buffered  bool     // Whether rows have been buffered into memory
	buffer    [][]byte // Buffered row data packets
	bufferPos int      // Current position in buffer

	currentRow []interface{}
	closed     bool
}

// Columns returns the names of the columns
func (r *Rows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	// If already buffered, just clear buffer
	if r.buffered {
		r.buffer = nil
		return nil
	}

	// Read remaining packets until EOF
	// This is critical to prevent connection reuse with unread data
	if r.conn != nil && !r.conn.client.IsClosed() {
		r.conn.client.Lock()
		defer r.conn.client.Unlock()

		// Clear active rows tracking
		if r.conn.activeRows == r {
			r.conn.activeRows = nil
		}

		for {
			data, err := r.conn.client.Reader().ReadPacket()
			if err != nil {
				return err
			}

			// Check for error packet
			if len(data) > 0 && data[0] == 0xff {
				// Error packet - we're done (but there was an error)
				break
			}

			// Check for EOF packet (0xFE)
			if len(data) > 0 && data[0] == 0xfe && len(data) < 0xffffff {
				if r.conn.context.IsEOFDeprecated() {
					// With CLIENT_DEPRECATE_EOF, 0xFE is an OK packet
					_, _ = server.ParseOkPacket(data, r.conn.context)
				} else {
					// Traditional EOF packet
					_, _ = server.ParseEOFPacket(data, r.conn.context)
				}
				break
			}

			// Otherwise it's a data row packet - keep consuming
		}
	}

	return nil
}

// Next is called to populate the next row of data into
// the provided slice
func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	// If buffered, read from buffer
	if r.buffered {
		return r.nextFromBuffer(dest)
	}

	if r.conn == nil {
		return io.EOF
	}

	// Lock connection while reading
	r.conn.client.Lock()
	defer r.conn.client.Unlock()

	// Read next row packet
	data, err := r.conn.client.Reader().ReadPacket()
	if err != nil {
		r.closed = true
		return err
	}

	// Check for error packet
	if len(data) > 0 && data[0] == 0xff {
		r.closed = true
		return protocol.ParseErrorPacket(data)
	}

	// Check for EOF packet (0xFE)
	if len(data) > 0 && data[0] == 0xfe && len(data) < 0xffffff {
		if r.conn.context.IsEOFDeprecated() {
			// With CLIENT_DEPRECATE_EOF, 0xFE is an OK packet
			_, _ = server.ParseOkPacket(data, r.conn.context)
		} else {
			// Traditional EOF packet
			_, _ = server.ParseEOFPacket(data, r.conn.context)
		}
		r.closed = true
		return io.EOF
	}

	// Parse row data
	var values []interface{}
	if r.binary {
		values, err = protocol.ParseBinaryRow(data, r.columns)
	} else {
		values, err = protocol.ParseTextRow(data, r.columns)
	}

	if err != nil {
		r.closed = true
		return err
	}

	// Copy values to destination
	for i, v := range values {
		if i < len(dest) {
			dest[i] = v
		}
	}

	return nil
}

// nextFromBuffer reads the next row from the internal buffer
func (r *Rows) nextFromBuffer(dest []driver.Value) error {
	if r.bufferPos >= len(r.buffer) {
		r.closed = true
		return io.EOF
	}

	// Parse row data from buffer
	var values []interface{}
	var err error
	if r.binary {
		values, err = protocol.ParseBinaryRow(r.buffer[r.bufferPos], r.columns)
	} else {
		values, err = protocol.ParseTextRow(r.buffer[r.bufferPos], r.columns)
	}

	r.bufferPos++

	if err != nil {
		r.closed = true
		return err
	}

	// Copy values to destination
	for i, v := range values {
		if i < len(dest) {
			dest[i] = v
		}
	}

	return nil
}

// bufferRemaining buffers all remaining rows into memory
// This is called when the connection is needed for another command
// Must be called with connection mutex locked
func (r *Rows) bufferRemaining() error {
	if r.closed || r.buffered {
		return nil
	}

	r.buffer = make([][]byte, 0, 10)

	// Read all remaining packets into buffer
	for {
		data, err := r.conn.client.Reader().ReadPacket()
		if err != nil {
			r.closed = true
			return err
		}

		// Check for EOF packet
		if len(data) > 0 && data[0] == 0xfe && len(data) < 9 {
			break
		}

		// Check for error packet
		if len(data) > 0 && data[0] == 0xff {
			r.closed = true
			return protocol.ParseErrorPacket(data)
		}

		// Store row data
		r.buffer = append(r.buffer, data)
	}

	r.buffered = true
	r.bufferPos = 0
	return nil
}

// ColumnTypeDatabaseTypeName returns the database system type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return protocol.TypeToString(r.columns[index].Type)
}

// ColumnTypeLength returns the length of the column type
func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, false
	}
	return int64(r.columns[index].Length), true
}

// ColumnTypeNullable returns whether the column may be null
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return false, false
	}
	return r.columns[index].Flags&protocol.NOT_NULL_FLAG == 0, true
}

// ColumnTypePrecisionScale returns the precision and scale for decimal types
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, 0, false
	}

	col := r.columns[index]
	if col.Type == protocol.MYSQL_TYPE_DECIMAL || col.Type == protocol.MYSQL_TYPE_NEWDECIMAL {
		return int64(col.Length), int64(col.Decimals), true
	}

	return 0, 0, false
}

// ColumnTypeScanType returns the Go type that can be used to scan
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	if index < 0 || index >= len(r.columns) {
		return nil
	}
	return protocol.TypeToScanType(r.columns[index].Type)
}
