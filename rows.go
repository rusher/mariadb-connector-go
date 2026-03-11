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

// Rows implements driver.Rows interface.
// Holds a list of completions (supports multi-resultset).
// Pre-fetched rows are served first; remaining rows are streamed if MoreRows is set.
type Rows struct {
	conn        *Conn
	completions []*protocol.Completion
	idx         int // current completion index
	rowPos      int // position within current completion's pre-fetched Rows slice
	closed      bool
}

// current returns the active completion, or nil if exhausted
func (r *Rows) current() *protocol.Completion {
	if r.idx < len(r.completions) {
		return r.completions[r.idx]
	}
	return nil
}

// cols returns the columns of the current completion
func (r *Rows) cols() []protocol.ColumnDefinition {
	if c := r.current(); c != nil {
		return c.Columns
	}
	return nil
}

// Columns returns the names of the columns
func (r *Rows) Columns() []string {
	cols := r.cols()
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	return names
}

// lastCompletion returns the last completion, which is the only one that can have MoreRows=true
func (r *Rows) lastCompletion() *protocol.Completion {
	if len(r.completions) == 0 {
		return nil
	}
	return r.completions[len(r.completions)-1]
}

// Close closes the rows iterator
func (r *Rows) Close() error {
	if r.closed {
		return nil
	}
	r.closed = true

	// Drain the last completion if it still has rows on the wire
	last := r.lastCompletion()
	if last != nil && (!last.Loaded || (last.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0)) && r.conn != nil && !r.conn.client.IsClosed() {
		_, _ = r.conn.client.ReadRemainingRows(last)
	}

	if r.conn != nil {
		r.conn.client.ClearActiveRows()
	}
	return nil
}

// parseRow parses a raw row packet into dest using the current completion's binary flag
func (r *Rows) parseRow(data []byte, c *protocol.Completion, dest []driver.Value) error {
	var values []interface{}
	var err error
	if c.Binary {
		values, err = protocol.ParseBinaryRow(data, c.Columns)
	} else {
		values, err = protocol.ParseTextRow(data, c.Columns)
	}
	if err != nil {
		r.closed = true
		return err
	}
	for i, v := range values {
		if i < len(dest) {
			dest[i] = v
		}
	}
	return nil
}

// Next is called to populate the next row of data into the provided slice
func (r *Rows) Next(dest []driver.Value) error {
	if r.closed {
		return io.EOF
	}

	c := r.current()
	if c == nil || !c.HasResultSet() {
		return io.EOF
	}

	// Serve from pre-fetched rows first
	if r.rowPos < len(c.Rows) {
		data := c.Rows[r.rowPos]
		r.rowPos++
		return r.parseRow(data, c, dest)
	}

	// All pre-fetched rows consumed
	if c.Loaded {
		return io.EOF
	}

	// Fetch the next batch from the wire
	loaded, err := r.fetchCurrentRows(c)
	if err != nil {
		return err
	}
	r.rowPos = 0

	if loaded {
		if c.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0 {
			more, err := r.conn.client.ReadCompletions(c.Binary, r.conn.client.FetchSize())
			if err != nil {
				r.closed = true
				return err
			}
			r.completions = append(r.completions, more...)
			if len(more) == 0 || more[len(more)-1].Loaded {
				r.conn.client.ClearActiveRows()
			}
		} else {
			r.conn.client.ClearActiveRows()
		}
	}

	if len(c.Rows) == 0 {
		return io.EOF
	}

	data := c.Rows[r.rowPos]
	r.rowPos++
	return r.parseRow(data, c, dest)
}

// fetchCurrentRows reads row packets from the wire into c.Rows.
// fetchSize=0 reads all rows. Must be called with the client mutex locked.
// Returns loaded=true when the row terminator packet was received.
func (r *Rows) fetchCurrentRows(c *protocol.Completion) (loaded bool, err error) {
	fetchSize := r.conn.client.FetchSize()
	for i := 0; i < fetchSize; i++ {
		data, err := r.conn.client.Reader().ReadPacket()
		if err != nil {
			r.closed = true
			return false, err
		}

		if len(data) > 0 && data[0] == 0xff {
			r.closed = true
			return false, server.ParseErrorPacket(data)
		}

		if len(data) > 0 && data[0] == 0xfe && len(data) < 0xffffff {
			var term *protocol.Completion
			if r.conn.context.IsEOFDeprecated() {
				term, err = server.ParseOkPacket(data, r.conn.context)
			} else {
				term, err = server.ParseEOFPacket(data, r.conn.context)
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
			c.Rows = c.Rows[:i]
			return true, nil
		}

		c.Rows[i] = data
	}
	return false, nil
}

// fetchRemainingRows reads all remaining row packets from the wire into c.Rows.
// Must be called with the client mutex locked.
// Returns hitEOF=true when the row terminator packet was received.
func (r *Rows) fetchRemainingRows(c *protocol.Completion) (loaded bool, err error) {
	c.Rows = c.Rows[:0]
	for {
		data, err := r.conn.client.Reader().ReadPacket()
		if err != nil {
			r.closed = true
			return false, err
		}

		if len(data) > 0 && data[0] == 0xff {
			r.closed = true
			return false, server.ParseErrorPacket(data)
		}

		if len(data) > 0 && data[0] == 0xfe && len(data) < 0xffffff {
			var term *protocol.Completion
			if r.conn.context.IsEOFDeprecated() {
				term, err = server.ParseOkPacket(data, r.conn.context)
			} else {
				term, err = server.ParseEOFPacket(data, r.conn.context)
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

		c.Rows = append(c.Rows, data)
	}
}

// BufferRemaining reads all remaining rows from the wire into memory.
// Called when the connection is needed for another command.
// Must be called with client mutex locked.
func (r *Rows) BufferRemaining() error {
	if r.closed {
		return nil
	}
	last := r.lastCompletion()
	if last == nil || last.Loaded {
		return nil
	}
	loaded, err := r.fetchRemainingRows(last)
	if err != nil {
		return err
	}
	if loaded && last.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0 {
		more, err := r.conn.client.ReadCompletions(last.Binary, 0)
		if err != nil {
			r.closed = true
			return err
		}
		r.completions = append(r.completions, more...)
	}
	return nil
}

// HasNextResultSet implements driver.RowsNextResultSet
func (r *Rows) HasNextResultSet() bool {
	for i := r.idx + 1; i < len(r.completions); i++ {
		if r.completions[i].HasResultSet() {
			return true
		}
	}
	return false
}

// NextResultSet advances to the next result set, skipping non-result-set completions
func (r *Rows) NextResultSet() error {
	for {

		r.idx++
		if r.idx >= len(r.completions) {
			return io.EOF
		}
		c := r.completions[r.idx]
		if c.HasResultSet() {
			r.rowPos = 0
			if !c.Loaded {
				r.conn.client.SetActiveRows(r)
			}
			return nil
		}
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
