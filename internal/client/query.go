// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// RowsInterface defines the interface for result sets that can be drained
type RowsInterface interface {
	DrainRemaining() error
	IsClosed() bool
}

// ReadCompletion reads one completion from the server response and stores it
// in c.completion. Returns a pointer to c.completion (valid until the next
// call to ReadCompletion). Must be called with client mutex locked.
//
// cachedCols is the previously cached column definitions for a prepared
// statement (used with CACHE_METADATA). Pass nil for text-protocol queries.
func (c *Client) ReadCompletion(binary bool, cachedCols []protocol.ColumnDefinition) (*protocol.Completion, error) {
	// Use scratch buffer: this packet is always consumed immediately.
	response, err := c.reader.ReadScratch()
	if err != nil {
		return nil, err
	}

	// Error packet
	if response[0] == 0xff {
		return nil, protocol.ParseErrorPacket(response)
	}

	// OK packet (no result set — DML or empty response)
	if response[0] == 0x00 {
		return protocol.ParseOkPacket(response, c.context)
	}

	// Result set — parse column count and optional CACHE_METADATA byte.
	columnCount, pos := protocol.ReadLengthEncodedInteger(response, 0)

	var columns []protocol.ColumnDefinition
	if binary && c.context.CanSkipMeta() {
		metadataFollows := response[pos]
		if metadataFollows == 0 {
			// Server says: no metadata, use cached columns.
			if cachedCols == nil {
				return nil, fmt.Errorf("server skipped metadata but no cached columns available")
			}
			columns = cachedCols
		} else {
			// Metadata follows — read column definitions and update cache.
			columns = make([]protocol.ColumnDefinition, columnCount)
			for i := range columns {
				colData, err := c.reader.ReadScratch()
				if err != nil {
					return nil, fmt.Errorf("failed to read column %d: %w", i, err)
				}
				if err := protocol.FillColumnDefinition(colData, &columns[i], c.context.IsExtendedMetadata()); err != nil {
					return nil, fmt.Errorf("failed to parse column %d: %w", i, err)
				}
			}
			if !c.context.IsEOFDeprecated() {
				if _, err := c.reader.ReadScratch(); err != nil {
					return nil, fmt.Errorf("failed to read column EOF: %w", err)
				}
			}
		}
	} else {
		// Text protocol or CACHE_METADATA not negotiated: always read column defs.
		columns = make([]protocol.ColumnDefinition, columnCount)
		for i := range columns {
			colData, err := c.reader.ReadScratch()
			if err != nil {
				return nil, fmt.Errorf("failed to read column %d: %w", i, err)
			}
			if err := protocol.FillColumnDefinition(colData, &columns[i], c.context.IsExtendedMetadata()); err != nil {
				return nil, fmt.Errorf("failed to parse column %d: %w", i, err)
			}
		}
		if !c.context.IsEOFDeprecated() {
			if _, err := c.reader.ReadScratch(); err != nil {
				return nil, fmt.Errorf("failed to read column EOF: %w", err)
			}
		}
	}

	// Read the first row using the reusable scratch buffer (no allocation).
	// Peek ahead: single-row → consume terminator and mark Loaded; multi-row → stream via Next().
	firstRow, err := c.reader.ReadScratch()
	if err != nil {
		return nil, err
	}
	if firstRow[0] == 0xff {
		return nil, protocol.ParseErrorPacket(firstRow)
	}

	if firstRow[0] == 0xfe && len(firstRow) < 0xffffff {
		// Empty result set: first packet is already the terminator.
		term, err := c.parseTerminator(firstRow)
		if err != nil {
			return nil, err
		}
		c.completion = protocol.Completion{
			Columns:      columns,
			Binary:       binary,
			AffectedRows: term.AffectedRows,
			InsertID:     term.InsertID,
			WarningCount: term.WarningCount,
			ServerStatus: term.ServerStatus,
			Message:      term.Message,
			Loaded:       true,
		}
		return &c.completion, nil
	}

	// Parse row 0 from scratch — no allocation for raw bytes.
	parsedRow := make([]driver.Value, len(columns))
	if binary {
		err = protocol.ParseBinaryRowDirect(firstRow, columns, parsedRow)
	} else {
		err = protocol.ParseTextRowDirect(firstRow, columns, parsedRow)
	}
	if err != nil {
		return nil, err
	}

	if c.reader.PeekIsTerminator() {
		// Single-row result: consume the terminator now.
		termData, err := c.reader.ReadScratch()
		if err != nil {
			return nil, err
		}
		if termData[0] == 0xff {
			return nil, protocol.ParseErrorPacket(termData)
		}
		term, err := c.parseTerminator(termData)
		if err != nil {
			return nil, err
		}
		c.completion = protocol.Completion{
			Columns:      columns,
			Binary:       binary,
			ParsedRow:    parsedRow,
			AffectedRows: term.AffectedRows,
			InsertID:     term.InsertID,
			WarningCount: term.WarningCount,
			ServerStatus: term.ServerStatus,
			Message:      term.Message,
			Loaded:       true,
		}
	} else {
		// Multi-row: rows 1+ stream via Next()
		c.completion = protocol.Completion{
			Columns:   columns,
			Binary:    binary,
			ParsedRow: parsedRow,
		}
	}
	return &c.completion, nil
}

// parseTerminator parses an EOF or OK-as-EOF terminator packet.
func (c *Client) parseTerminator(data []byte) (*protocol.Completion, error) {
	if c.context.IsEOFDeprecated() {
		return protocol.ParseOkPacket(data, c.context)
	}
	return protocol.ParseEOFPacket(data, c.context)
}

// drainResultSetMeta reads and discards a result-set header (column count,
// optional CACHE_METADATA byte, column definitions, and inter-column EOF).
// Called when SERVER_MORE_RESULTS_EXISTS is set during drain.
func (c *Client) drainResultSetMeta(binary bool) error {
	response, err := c.reader.ReadScratch()
	if err != nil {
		return err
	}
	if response[0] == 0xff {
		return protocol.ParseErrorPacket(response)
	}
	if response[0] == 0x00 {
		return nil // OK packet — no column defs follow
	}

	columnCount, pos := protocol.ReadLengthEncodedInteger(response, 0)

	if binary && c.context.CanSkipMeta() {
		if response[pos] == 0 {
			return nil // metadata skipped by server — nothing more to read
		}
	}

	for i := 0; i < int(columnCount); i++ {
		if _, err := c.reader.ReadScratch(); err != nil {
			return err
		}
	}
	if !c.context.IsEOFDeprecated() {
		if _, err := c.reader.ReadScratch(); err != nil {
			return err
		}
	}
	return nil
}

// DrainRemainingRows discards all remaining row packets for a streaming result set
// without allocating. The terminator (OK/EOF) is parsed to update c.completion.
// If SERVER_MORE_RESULTS_EXISTS, subsequent result sets are also drained.
// Must be called with client mutex locked.
func (c *Client) DrainRemainingRows(completion *protocol.Completion) error {
	for {
		rowData, err := c.reader.ReadScratch()
		if err != nil {
			return err
		}

		if len(rowData) > 0 && rowData[0] == 0xff {
			return protocol.ParseErrorPacket(rowData)
		}

		if len(rowData) > 0 && rowData[0] == 0xfe && len(rowData) < 0xffffff {
			term, err := c.parseTerminator(rowData)
			if err != nil {
				return err
			}
			completion.AffectedRows = term.AffectedRows
			completion.InsertID = term.InsertID
			completion.WarningCount = term.WarningCount
			completion.ServerStatus = term.ServerStatus
			completion.Message = term.Message
			completion.Loaded = true
			if term.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0 {
				// More result sets follow — skip their metadata, then drain rows.
				if err := c.drainResultSetMeta(completion.Binary); err != nil {
					return err
				}
				continue
			}
			return nil
		}
		// Row data — discarded.
	}
}

// Query executes a SELECT and returns the single completion.
// When args is non-empty, placeholders are interpolated directly into the
// packet buffer via AppendQueryInterpolated, avoiding an intermediate string alloc.
func (c *Client) Query(ctx context.Context, query string, args []driver.NamedValue, noBackslashEscapes bool) (*protocol.Completion, error) {
	stop, err := c.WithContext(ctx)
	if err != nil {
		return nil, err
	}
	defer stop()
	if err := c.drainActiveRows(); err != nil {
		return nil, err
	}
	c.sequence = 0
	var pkt []byte
	if len(args) > 0 {
		pkt, err = protocol.NewQueryParam(c.writer.Buf(), query, args, noBackslashEscapes)
		if err != nil {
			return nil, err
		}
	} else {
		pkt = protocol.NewQuery(c.writer.Buf(), query)
	}
	if err := c.writer.Write(pkt); err != nil {
		return nil, err
	}
	return c.ReadCompletion(false, nil)
}

// Exec executes a DML statement and returns the single OK completion.
// When args is non-empty, placeholders are interpolated directly into the
// packet buffer via AppendQueryInterpolated, avoiding an intermediate string alloc.
func (c *Client) Exec(ctx context.Context, query string, args []driver.NamedValue, noBackslashEscapes bool) (*protocol.Completion, error) {
	stop, err := c.WithContext(ctx)
	if err != nil {
		return nil, err
	}
	defer stop()
	if err := c.drainActiveRows(); err != nil {
		return nil, err
	}
	c.sequence = 0
	var pkt []byte
	if len(args) > 0 {
		pkt, err = protocol.NewQueryParam(c.writer.Buf(), query, args, noBackslashEscapes)
		if err != nil {
			return nil, err
		}
	} else {
		pkt = protocol.NewQuery(c.writer.Buf(), query)
	}
	if err := c.writer.Write(pkt); err != nil {
		return nil, err
	}
	data, err := c.reader.ReadScratch()
	if err != nil {
		return nil, err
	}
	if data[0] == 0xff {
		return nil, protocol.ParseErrorPacket(data)
	}
	term, err := protocol.ParseOkPacket(data, c.context)
	if err != nil {
		return nil, err
	}
	c.completion = *term
	return &c.completion, nil
}

// SetActiveRows sets the currently active result set
// Must be called with client mutex locked
func (c *Client) SetActiveRows(rows RowsInterface) {
	c.activeRows = rows
}

// ClearActiveRows clears the active result set
// Must be called with client mutex locked
func (c *Client) ClearActiveRows() {
	c.activeRows = nil
}

// drainActiveRows discards any rows still on the wire for the active result set.
// This allows the connection to be used for another command.
// Must be called with client mutex locked.
func (c *Client) drainActiveRows() error {
	if c.activeRows == nil {
		return nil
	}

	rows, ok := c.activeRows.(RowsInterface)
	if !ok {
		return nil
	}

	if !rows.IsClosed() {
		if err := rows.DrainRemaining(); err != nil {
			return err
		}
	}

	c.activeRows = nil
	return nil
}
