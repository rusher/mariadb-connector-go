// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql/driver"

	"github.com/mariadb-connector-go/mariadb/internal/client"
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// Conn implements driver.Conn interface
// This is a thin wrapper around the internal client
type Conn struct {
	client  *client.Client
	config  *client.Config
	context *Context

	// Active result set tracking
	activeRows *Rows // Currently active streaming result set
}

// newConn creates a new connection wrapper
func newConn(cfg *client.Config) (*Conn, error) {
	// Create internal client
	c := &Conn{
		client: client.NewClient(cfg),
		config: cfg,
	}

	return c, nil
}

// Connect establishes the connection
func (c *Conn) connect(ctx context.Context) error {
	// Connect using internal client
	if err := c.client.Connect(ctx); err != nil {
		return err
	}

	// Wrap the internal context
	c.context = &Context{
		internal: c.client.Context(),
	}

	return nil
}

// Prepare returns a prepared statement, bound to this connection
func (c *Conn) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

// PrepareContext returns a prepared statement, bound to this connection
func (c *Conn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	stmt := &Stmt{
		conn:  c,
		query: query,
	}

	return stmt, nil
}

// Close invalidates and potentially stops any current
// prepared statements and transactions, marking this
// connection as no longer in use
func (c *Conn) Close() error {
	return c.client.Close()
}

// Begin starts and returns a new transaction
func (c *Conn) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

// BeginTx starts and returns a new transaction
func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	// Build BEGIN statement based on isolation level
	query := "START TRANSACTION"

	if opts.Isolation != 0 {
		var isolationLevel string
		switch opts.Isolation {
		case driver.IsolationLevel(1): // READ UNCOMMITTED
			isolationLevel = "READ UNCOMMITTED"
		case driver.IsolationLevel(2): // READ COMMITTED
			isolationLevel = "READ COMMITTED"
		case driver.IsolationLevel(3): // REPEATABLE READ
			isolationLevel = "REPEATABLE READ"
		case driver.IsolationLevel(4): // SERIALIZABLE
			isolationLevel = "SERIALIZABLE"
		default:
			return nil, driver.ErrBadConn
		}

		// Set isolation level before starting transaction
		setIsolation := "SET TRANSACTION ISOLATION LEVEL " + isolationLevel
		if err := c.client.ExecInternal(ctx, setIsolation); err != nil {
			return nil, err
		}
	}

	if opts.ReadOnly {
		query += " READ ONLY"
	}

	if err := c.client.ExecInternal(ctx, query); err != nil {
		return nil, err
	}

	return &Tx{conn: c}, nil
}

// ExecContext executes a query that doesn't return rows
func (c *Conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	if len(args) > 0 {
		return nil, driver.ErrSkip // Use prepared statement for queries with args
	}

	// Buffer any active result set before issuing new command
	if err := c.bufferActiveRows(); err != nil {
		return nil, err
	}

	// Build COM_QUERY packet
	packet := make([]byte, 1+len(query))
	packet[0] = protocol.COM_QUERY
	copy(packet[1:], query)

	// Send command (resets sequence)
	if err := c.client.SendCommand(packet); err != nil {
		return nil, err
	}

	// Read response
	response, err := c.client.ReadPacket()
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

// QueryContext executes a query that may return rows
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	// Lock the client for the entire operation
	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	if len(args) > 0 {
		return nil, driver.ErrSkip // Use prepared statement for queries with args
	}

	// Buffer any active result set before issuing new command
	if err := c.bufferActiveRows(); err != nil {
		return nil, err
	}

	// Build COM_QUERY packet
	packet := make([]byte, 1+len(query))
	packet[0] = protocol.COM_QUERY
	copy(packet[1:], query)

	// Send command (resets sequence)
	if err := c.client.SendCommand(packet); err != nil {
		return nil, err
	}

	// Read column count
	columnCountData, err := c.client.ReadPacket()
	if err != nil {
		return nil, err
	}

	// Check for error
	if len(columnCountData) > 0 && columnCountData[0] == 0xff {
		return nil, protocol.ParseErrorPacket(columnCountData)
	}

	// Check for OK packet (no result set)
	if len(columnCountData) > 0 && columnCountData[0] == 0x00 {
		return &Rows{conn: c, columns: []*protocol.ColumnDefinition{}, closed: true}, nil
	}

	// Parse column count
	columnCount, _, err := protocol.ReadLengthEncodedInteger(columnCountData, 0)
	if err != nil {
		return nil, err
	}

	// Read column definitions
	columns := make([]*protocol.ColumnDefinition, columnCount)
	for i := 0; i < int(columnCount); i++ {
		colData, err := c.client.ReadPacket()
		if err != nil {
			return nil, err
		}

		col, err := protocol.ParseColumnDefinition(colData)
		if err != nil {
			return nil, err
		}
		columns[i] = col
	}

	// Read EOF packet after column definitions (if not using CLIENT_DEPRECATE_EOF)
	if !c.context.IsEOFDeprecated() {
		eofData, err := c.client.ReadPacket()
		if err != nil {
			return nil, err
		}
		// Verify it's actually an EOF packet (0xfe with length < 9)
		if len(eofData) == 0 || (eofData[0] != 0xfe || len(eofData) >= 9) {
			return nil, protocol.ParseErrorPacket(eofData)
		}
	}

	// Create streaming rows
	rows := &Rows{
		conn:    c,
		columns: columns,
		binary:  false,
	}

	// Track as active result set
	c.activeRows = rows

	return rows, nil
}

// bufferActiveRows buffers any active streaming result set into memory
// This allows the connection to be used for another command
// Must be called with connection mutex locked
func (c *Conn) bufferActiveRows() error {
	if c.activeRows != nil && !c.activeRows.closed && !c.activeRows.buffered {
		if err := c.activeRows.bufferRemaining(); err != nil {
			return err
		}
		c.activeRows = nil
	}
	return nil
}

// Ping verifies the connection is still alive
func (c *Conn) Ping(ctx context.Context) error {
	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return driver.ErrBadConn
	}

	// Send COM_PING (resets sequence)
	packet := []byte{protocol.COM_PING}
	if err := c.client.SendCommand(packet); err != nil {
		return err
	}

	// Read response
	response, err := c.client.ReadPacket()
	if err != nil {
		return driver.ErrBadConn
	}

	// Check for error
	if len(response) > 0 && response[0] == 0xff {
		return protocol.ParseErrorPacket(response)
	}

	// Verify it's an OK packet
	if len(response) == 0 || response[0] != 0x00 {
		return driver.ErrBadConn
	}

	return nil
}

// ResetSession is called prior to executing a query on the connection
// if the connection has been used before
func (c *Conn) ResetSession(ctx context.Context) error {
	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return driver.ErrBadConn
	}

	// Send COM_RESET_CONNECTION if supported
	if c.context.HasClientCapability(protocol.CLIENT_SESSION_TRACK) {
		packet := []byte{protocol.COM_RESET_CONNECTION}
		if err := c.client.WritePacket(packet); err != nil {
			return err
		}

		// Read response
		response, err := c.client.ReadPacket()
		if err != nil {
			return err
		}

		// Check for error
		if len(response) > 0 && response[0] == 0xff {
			return protocol.ParseErrorPacket(response)
		}
	}

	return nil
}

// execInternal executes a query without returning results (internal use)
// Must be called with client mutex locked
func (c *Conn) execInternal(ctx context.Context, query string) error {
	return c.client.ExecInternal(ctx, query)
}
