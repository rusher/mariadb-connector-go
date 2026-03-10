// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"database/sql/driver"
	"io"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
	"github.com/mariadb-connector-go/mariadb/internal/protocol/server"
)

// ResultFetchMode determines how results are fetched
type ResultFetchMode int

const (
	// FetchModeComplete fetches all rows immediately into memory
	FetchModeComplete ResultFetchMode = iota
	// FetchModeStreaming fetches rows on-demand, keeping them in TCP buffer
	FetchModeStreaming
)

// CompleteRows implements driver.Rows for complete result sets
// All rows are fetched immediately into memory
type CompleteRows struct {
	columns []*protocol.ColumnDefinition
	data    [][]byte // All row data packets
	pos     int      // Current position (-1 = before first)
	binary  bool
}

// NewCompleteRows creates a complete result set by fetching all rows immediately
func NewCompleteRows(conn *Conn, columns []*protocol.ColumnDefinition, binary bool) (*CompleteRows, error) {
	rows := &CompleteRows{
		columns: columns,
		data:    make([][]byte, 0, 10), // Start with capacity for 10 rows
		pos:     -1,
		binary:  binary,
	}

	// Fetch all rows immediately
	for {
		data, err := conn.client.Reader().ReadPacket()
		if err != nil {
			return nil, err
		}

		// Check for error packet
		if len(data) > 0 && data[0] == 0xff {
			return nil, protocol.ParseErrorPacket(data)
		}

		// Check for EOF packet (0xFE)
		if len(data) > 0 && data[0] == 0xfe && len(data) < 0xffffff {
			if conn.context.IsEOFDeprecated() {
				// With CLIENT_DEPRECATE_EOF, 0xFE is an OK packet
				_, _ = server.ParseOkPacket(data, conn.context)
			} else {
				// Traditional EOF packet
				_, _ = server.ParseEOFPacket(data, conn.context)
			}
			break
		}

		// Store row data
		rows.data = append(rows.data, data)
	}

	return rows, nil
}

// Columns returns the column names
func (r *CompleteRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator
func (r *CompleteRows) Close() error {
	// All data is already in memory, nothing to clean up
	r.data = nil
	return nil
}

// Next advances to the next row
func (r *CompleteRows) Next(dest []driver.Value) error {
	r.pos++

	if r.pos >= len(r.data) {
		return io.EOF
	}

	// Parse row data
	var values []interface{}
	var err error

	if r.binary {
		values, err = protocol.ParseBinaryRow(r.data[r.pos], r.columns)
	} else {
		values, err = protocol.ParseTextRow(r.data[r.pos], r.columns)
	}

	if err != nil {
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

// ColumnTypeDatabaseTypeName returns the database system type name
func (r *CompleteRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return protocol.TypeToString(r.columns[index].Type)
}

// ColumnTypeLength returns the length of the column type
func (r *CompleteRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, false
	}
	return int64(r.columns[index].Length), true
}

// ColumnTypeNullable returns whether the column can be null
func (r *CompleteRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return false, false
	}
	return (r.columns[index].Flags & protocol.NOT_NULL_FLAG) == 0, true
}

// ColumnTypePrecisionScale returns the precision and scale for decimal types
func (r *CompleteRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, 0, false
	}
	return int64(r.columns[index].Decimals), int64(r.columns[index].Decimals), true
}

// ColumnTypeScanType returns the Go type that can be used to scan
func (r *CompleteRows) ColumnTypeScanType(index int) interface{} {
	if index < 0 || index >= len(r.columns) {
		return nil
	}
	return protocol.TypeToScanType(r.columns[index].Type)
}

// StreamingRows implements driver.Rows for streaming result sets
// Rows are fetched on-demand from the TCP buffer
type StreamingRows struct {
	conn    *Conn
	columns []*protocol.ColumnDefinition
	binary  bool
	closed  bool

	// Buffering for fetch size
	buffer    [][]byte // Buffered row data
	bufferPos int      // Position in buffer
	fetchSize int      // Number of rows to fetch at once
	eof       bool     // Reached end of result set
}

// NewStreamingRows creates a streaming result set
func NewStreamingRows(conn *Conn, columns []*protocol.ColumnDefinition, binary bool, fetchSize int) *StreamingRows {
	if fetchSize <= 0 {
		fetchSize = 10 // Default fetch size
	}

	return &StreamingRows{
		conn:      conn,
		columns:   columns,
		binary:    binary,
		buffer:    make([][]byte, 0, fetchSize),
		fetchSize: fetchSize,
	}
}

// Columns returns the column names
func (r *StreamingRows) Columns() []string {
	names := make([]string, len(r.columns))
	for i, col := range r.columns {
		names[i] = col.Name
	}
	return names
}

// Close closes the rows iterator and consumes any remaining rows
func (r *StreamingRows) Close() error {
	if r.closed {
		return nil
	}

	r.closed = true

	// If we haven't reached EOF, consume remaining packets
	if !r.eof && r.conn != nil {
		r.conn.client.Lock()
		defer r.conn.client.Unlock()

		for {
			data, err := r.conn.client.Reader().ReadPacket()
			if err != nil {
				return err
			}

			// Check for EOF packet
			if len(data) > 0 && data[0] == 0xfe && len(data) < 9 {
				break
			}

			// Check for error packet
			if len(data) > 0 && data[0] == 0xff {
				break
			}

			// Otherwise it's a data row packet - keep consuming
		}
	}

	r.buffer = nil
	return nil
}

// Next advances to the next row
func (r *StreamingRows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	if r.eof && r.bufferPos >= len(r.buffer) {
		return io.EOF
	}

	// If buffer is empty or exhausted, fetch more rows
	if r.bufferPos >= len(r.buffer) && !r.eof {
		if err := r.fetchRows(); err != nil {
			return err
		}

		// Check again after fetching
		if r.bufferPos >= len(r.buffer) {
			return io.EOF
		}
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

// fetchRows fetches the next batch of rows into the buffer
func (r *StreamingRows) fetchRows() error {
	r.conn.client.Lock()
	defer r.conn.client.Unlock()

	// Reset buffer
	r.buffer = r.buffer[:0]
	r.bufferPos = 0

	// Fetch up to fetchSize rows
	for i := 0; i < r.fetchSize; i++ {
		data, err := r.conn.client.Reader().ReadPacket()
		if err != nil {
			r.closed = true
			return err
		}

		// Check for EOF packet
		if len(data) > 0 && data[0] == 0xfe && len(data) < 9 {
			r.eof = true
			break
		}

		// Check for error packet
		if len(data) > 0 && data[0] == 0xff {
			r.closed = true
			return protocol.ParseErrorPacket(data)
		}

		// Add row to buffer
		r.buffer = append(r.buffer, data)
	}

	return nil
}

// ColumnTypeDatabaseTypeName returns the database system type name
func (r *StreamingRows) ColumnTypeDatabaseTypeName(index int) string {
	if index < 0 || index >= len(r.columns) {
		return ""
	}
	return protocol.TypeToString(r.columns[index].Type)
}

// ColumnTypeLength returns the length of the column type
func (r *StreamingRows) ColumnTypeLength(index int) (length int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, false
	}
	return int64(r.columns[index].Length), true
}

// ColumnTypeNullable returns whether the column can be null
func (r *StreamingRows) ColumnTypeNullable(index int) (nullable, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return false, false
	}
	return (r.columns[index].Flags & protocol.NOT_NULL_FLAG) == 0, true
}

// ColumnTypePrecisionScale returns the precision and scale for decimal types
func (r *StreamingRows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	if index < 0 || index >= len(r.columns) {
		return 0, 0, false
	}
	return int64(r.columns[index].Decimals), int64(r.columns[index].Decimals), true
}

// ColumnTypeScanType returns the Go type that can be used to scan
func (r *StreamingRows) ColumnTypeScanType(index int) interface{} {
	if index < 0 || index >= len(r.columns) {
		return nil
	}
	return protocol.TypeToScanType(r.columns[index].Type)
}
