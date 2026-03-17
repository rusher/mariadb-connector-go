// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"database/sql/driver"
	"io"
	"reflect"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// Rows implements driver.Rows interface.
// Holds a single completion; multi-result sets are advanced via NextResultSet.
type Rows struct {
	conn     *Conn
	current  *protocol.Completion
	rowPos   int      // position within current result set
	colNames []string // cached column name slice for the current result set
	closed   bool
}

// cols returns the columns of the current completion
func (r *Rows) cols() []protocol.ColumnDefinition {
	if r.current != nil {
		return r.current.Columns
	}
	return nil
}

// Columns returns the names of the columns, caching the result for the
// current result set so repeated calls do not allocate a new slice.
func (r *Rows) Columns() []string {
	if r.colNames != nil {
		return r.colNames
	}
	cols := r.cols()
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	r.colNames = names
	return names
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	if r.current != nil && (!r.current.Loaded || r.current.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0) && r.conn != nil && !r.conn.client.IsClosed() {
		_ = r.conn.client.DrainRemainingRows(r.current)
	}

	if r.conn != nil {
		r.conn.client.ClearActiveRows()
	}
	return nil
}

// parseRow parses a raw row packet into dest using the current completion's binary flag
func (r *Rows) parseRow(data []byte, c *protocol.Completion, dest []driver.Value) error {
	var err error
	if c.Binary {
		err = protocol.ParseBinaryRowDirect(data, c.Columns, dest)
	} else {
		err = protocol.ParseTextRowDirect(data, c.Columns, dest)
	}
	if err != nil {
		r.closed = true
		return err
	}
	return nil
}

// Next is called to populate the next row of data into the provided slice
func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	if r.current == nil || !r.current.HasResultSet() {
		return io.EOF
	}
	c := r.current

	// Row 0 is always pre-parsed into ParsedRow (avoids raw-byte allocation).
	if r.rowPos == 0 {
		if c.ParsedRow == nil {
			return io.EOF // empty result set
		}
		copy(dest, c.ParsedRow)
		r.rowPos++
		return nil
	}

	if c.Loaded {
		return io.EOF
	}

	// Rows 1+ stream directly from the wire into dest.
	loaded, err := r.fetchCurrentRow(c, dest)
	if err != nil {
		return err
	}
	if loaded {
		// Terminator received — no row was written into dest.
		r.conn.client.ClearActiveRows()
		return io.EOF
	}
	r.rowPos++
	return nil
}

// fetchCurrentRow reads exactly one packet from the wire.
// If it is a row packet it is parsed directly into dest and loaded=false is returned.
// If it is the terminator (EOF/OK) the completion is updated and loaded=true is returned
// (dest is left untouched).
// ReadScratch is used so no allocation is made for the raw packet bytes.
// Must be called with the client mutex locked.
func (r *Rows) fetchCurrentRow(c *protocol.Completion, dest []driver.Value) (loaded bool, err error) {
	data, err := r.conn.client.Reader().ReadScratch()
	if err != nil {
		r.closed = true
		return false, err
	}

	if data[0] == 0xff {
		r.closed = true
		return false, protocol.ParseErrorPacket(data)
	}

	if data[0] == 0xfe && len(data) < 0xffffff {
		var term *protocol.Completion
		if r.conn.context.IsEOFDeprecated() {
			term, err = protocol.ParseOkPacket(data, r.conn.context)
		} else {
			term, err = protocol.ParseEOFPacket(data, r.conn.context)
		}
		if err != nil {
			r.closed = true
			return false, err
		}
		c.AffectedRows = term.AffectedRows
		c.InsertID = term.InsertID
		c.WarningCount = term.WarningCount
		c.ServerStatus = term.ServerStatus
		c.Message = term.Message
		c.Loaded = true
		return true, nil
	}

	return false, r.parseRow(data, c, dest)
}

// drainRemainingRows discards all remaining row packets from the wire without
// storing them. The terminator (OK/EOF) is still parsed to update server status.
// Must be called with the client mutex locked.
func (r *Rows) drainRemainingRows(c *protocol.Completion) (loaded bool, err error) {
	for {
		data, err := r.conn.client.Reader().ReadScratch()
		if err != nil {
			r.closed = true
			return false, err
		}

		if len(data) > 0 && data[0] == 0xff {
			r.closed = true
			return false, protocol.ParseErrorPacket(data)
		}

		if len(data) > 0 && data[0] == 0xfe && len(data) < 0xffffff {
			var term *protocol.Completion
			if r.conn.context.IsEOFDeprecated() {
				term, err = protocol.ParseOkPacket(data, r.conn.context)
			} else {
				term, err = protocol.ParseEOFPacket(data, r.conn.context)
			}
			if err != nil {
				r.closed = true
				return false, err
			}
			c.AffectedRows = term.AffectedRows
			c.InsertID = term.InsertID
			c.WarningCount = term.WarningCount
			c.ServerStatus = term.ServerStatus
			c.Message = term.Message
			c.Loaded = true
			return true, nil
		}
		// Row data — discarded.
	}
}

// DrainRemaining discards all remaining rows still on the wire.
// Called when the connection is needed for another command.
// Must be called with client mutex locked.
func (r *Rows) DrainRemaining() error {
	if r.closed || r.current == nil || r.current.Loaded {
		return nil
	}
	_, err := r.drainRemainingRows(r.current)
	return err
}

// HasNextResultSet implements driver.RowsNextResultSet
func (r *Rows) HasNextResultSet() bool {
	return r.current != nil && r.current.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0
}

// NextResultSet advances to the next result set by reading the next completion.
// It skips result-less completions (e.g. DO 1) as long as
// SERVER_MORE_RESULTS_EXISTS is set, advancing to the next actual result set.
func (r *Rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return io.EOF
	}
	for {
		comp, err := r.conn.client.ReadCompletion(r.current.Binary, r.current.Columns)
		if err != nil {
			r.closed = true
			return err
		}
		if comp.HasResultSet() {
			r.current = comp
			r.rowPos = 0
			r.colNames = nil
			if !comp.Loaded {
				r.conn.client.SetActiveRows(r)
			}
			return nil
		}
		// No result set on this completion (e.g. DO 1).
		// If more results follow, keep reading; otherwise we're done.
		if comp.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS == 0 {
			return io.EOF
		}
		r.current = comp
	}
}

// ColumnTypeDatabaseTypeName returns the database system type name
func (r *Rows) ColumnTypeDatabaseTypeName(index int) string {
	cols := r.cols()
	if index < 0 || index >= len(cols) {
		return ""
	}
	return protocol.TypeToString(cols[index].Type)
}

// ColumnTypeLength returns the length of the column type
func (r *Rows) ColumnTypeLength(index int) (length int64, ok bool) {
	cols := r.cols()
	if index < 0 || index >= len(cols) {
		return 0, false
	}
	return int64(cols[index].Length), true
}

// ColumnTypeNullable returns whether the column may be null
func (r *Rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	cols := r.cols()
	if index < 0 || index >= len(cols) {
		return false, false
	}
	return cols[index].Flags&protocol.NOT_NULL_FLAG == 0, true
}

// ColumnTypePrecisionScale returns the precision and scale for decimal types
func (r *Rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	cols := r.cols()
	if index < 0 || index >= len(cols) {
		return 0, 0, false
	}
	col := cols[index]
	if col.Type == protocol.MYSQL_TYPE_DECIMAL || col.Type == protocol.MYSQL_TYPE_NEWDECIMAL {
		return int64(col.Length), int64(col.Decimals), true
	}
	return 0, 0, false
}

// ColumnTypeScanType returns the Go type that can be used to scan
func (r *Rows) ColumnTypeScanType(index int) reflect.Type {
	cols := r.cols()
	if index < 0 || index >= len(cols) {
		return nil
	}
	return protocol.TypeToScanTypeWithColumn(&cols[index])
}

// IsClosed returns whether the rows are closed
func (r *Rows) IsClosed() bool {
	return r.closed
}
