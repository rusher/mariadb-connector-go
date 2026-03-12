// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"context"
	"fmt"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// Completion is an alias for protocol.Completion
type Completion = protocol.Completion

// RowsInterface defines the interface for result sets that can be buffered
type RowsInterface interface {
	BufferRemaining() error
	IsClosed() bool
}

// FetchSize returns the configured fetch size (minimum 1).
func (c *Client) FetchSize() int {
	if c.config.FetchSize <= 0 {
		return 10
	}
	return c.config.FetchSize
}

// ReadCompletions reads one or more completions from the server response.
// fetchSize controls row pre-fetching for result sets:
//   - 0: read all rows into memory (Completion.MoreRows is always false)
//   - >0: pre-fetch up to fetchSize rows; if more remain, set MoreRows=true and stop
//
// Must be called with client mutex locked.
func (c *Client) ReadCompletions(binary bool, fetchSize int) ([]*protocol.Completion, error) {
	completions := make([]*protocol.Completion, 0, 1)

	for {
		// Use scratch buffer: this packet is always consumed immediately.
		response, err := c.reader.ReadScratch()
		if err != nil {
			return nil, err
		}

		// Error packet
		if len(response) > 0 && response[0] == 0xff {
			return nil, protocol.ParseErrorPacket(response)
		}

		// OK packet (no result set)
		if len(response) > 0 && response[0] == 0x00 {
			completion, err := protocol.ParseOkPacket(response, c.context)
			if err != nil {
				return nil, err
			}
			completion.Loaded = true
			completions = append(completions, completion)
			if completion.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0 {
				continue
			}
			break
		}

		// Result set - parse column count
		columnCount, _, err := protocol.ReadLengthEncodedInteger(response, 0)
		if err != nil {
			return nil, fmt.Errorf("failed to read column count: %w", err)
		}

		// Read column definitions — value slice: one allocation for all N columns.
		// Use ReadScratch: FillColumnDefinition copies all strings out of the buffer.
		columns := make([]protocol.ColumnDefinition, columnCount)
		for i := 0; i < int(columnCount); i++ {
			colData, err := c.reader.ReadPacket()
			if err != nil {
				return nil, fmt.Errorf("failed to read column %d: %w", i, err)
			}
			if err := protocol.FillColumnDefinition(colData, &columns[i], c.context.IsExtendedMetadata()); err != nil {
				return nil, fmt.Errorf("failed to parse column %d: %w", i, err)
			}
		}

		// Read EOF after column definitions (if not CLIENT_DEPRECATE_EOF)
		if !c.context.IsEOFDeprecated() {
			_, err := c.reader.ReadScratch()
			if err != nil {
				return nil, fmt.Errorf("failed to read column EOF: %w", err)
			}
		}

		rows := make([][]byte, 0, 1)

		// Pre-allocate the completion for the terminating EOF/OK packet.
		comp := protocol.Completion{}
		var completion *protocol.Completion
		for i := 0; fetchSize == 0 || i < fetchSize; i++ {
			rowData, err := c.reader.ReadPacket()
			if err != nil {
				return nil, err
			}

			if len(rowData) > 0 && rowData[0] == 0xff {
				return nil, protocol.ParseErrorPacket(rowData)
			}

			// End of rows: fill the pre-allocated completion.
			if len(rowData) > 0 && rowData[0] == 0xfe && len(rowData) < 0xffffff {
				if c.context.IsEOFDeprecated() {
					err = protocol.FillOkPacket(rowData, c.context, &comp)
				} else {
					err = protocol.FillEOFPacket(rowData, c.context, &comp)
				}
				if err != nil {
					return nil, err
				}
				completion = &comp
				break
			}

			rows = append(rows, rowData)
		}

		if completion == nil {
			// Hit fetchSize limit without reaching EOF — still streaming
			completion = &comp
		} else {
			completion.Loaded = true
		}
		completion.Columns = columns
		completion.Binary = binary
		completion.Rows = rows

		completions = append(completions, completion)

		if !completion.Loaded || completion.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS == 0 {
			break
		}
	}

	return completions, nil
}

// ReadRemainingRows reads all remaining row packets for a streaming result set
// into the completion's Rows slice, clears MoreRows, then reads any subsequent
// completions fully (fetchSize=0). Must be called with client mutex locked.
func (c *Client) ReadRemainingRows(completion *protocol.Completion) ([]*protocol.Completion, error) {
	for {
		rowData, err := c.reader.ReadPacket()
		if err != nil {
			return nil, err
		}

		if len(rowData) > 0 && rowData[0] == 0xff {
			return nil, protocol.ParseErrorPacket(rowData)
		}

		if len(rowData) > 0 && rowData[0] == 0xfe && len(rowData) < 0xffffff {
			var term *protocol.Completion
			if c.context.IsEOFDeprecated() {
				term, err = protocol.ParseOkPacket(rowData, c.context)
			} else {
				term, err = protocol.ParseEOFPacket(rowData, c.context)
			}
			if err != nil {
				return nil, err
			}
			completion.AffectedRows = term.AffectedRows
			completion.InsertID = term.InsertID
			completion.WarningCount = term.WarningCount
			completion.ServerStatus = term.ServerStatus
			completion.Message = term.Message
			completion.Loaded = true
			if term.ServerStatus&protocol.SERVER_MORE_RESULTS_EXISTS != 0 {
				return c.ReadCompletions(completion.Binary, 0)
			}
			return nil, nil
		}

		completion.Rows = append(completion.Rows, rowData)
	}
}

// Query executes a query and returns a list of completions (supports multi-resultset).
func (c *Client) Query(ctx context.Context, query string) ([]*protocol.Completion, error) {
	stop, err := c.WithContext(ctx)
	if err != nil {
		return nil, err
	}
	defer stop()
	if err := c.bufferActiveRows(); err != nil {
		return nil, err
	}
	c.sequence = 0
	if err := c.writer.Write(protocol.NewQuery(c.writer.Buf(), query)); err != nil {
		return nil, err
	}
	return c.ReadCompletions(false, c.FetchSize())
}

// Exec executes a command and returns a list of completions (supports multi-resultset).
func (c *Client) Exec(ctx context.Context, query string) ([]*protocol.Completion, error) {
	stop, err := c.WithContext(ctx)
	if err != nil {
		return nil, err
	}
	defer stop()
	if err := c.bufferActiveRows(); err != nil {
		return nil, err
	}
	c.sequence = 0
	if err := c.writer.Write(protocol.NewQuery(c.writer.Buf(), query)); err != nil {
		return nil, err
	}
	return c.ReadCompletions(false, c.FetchSize())
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

// bufferActiveRows buffers any active streaming result set into memory
// This allows the connection to be used for another command
// Must be called with client mutex locked
func (c *Client) bufferActiveRows() error {
	if c.activeRows == nil {
		return nil
	}

	rows, ok := c.activeRows.(RowsInterface)
	if !ok {
		return nil
	}

	if !rows.IsClosed() {
		if err := rows.BufferRemaining(); err != nil {
			return err
		}
	}

	c.activeRows = nil
	return nil
}
