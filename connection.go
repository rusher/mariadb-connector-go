// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql/driver"
	"fmt"

	"github.com/mariadb-connector-go/mariadb/internal/client"
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
	clientpkt "github.com/mariadb-connector-go/mariadb/internal/protocol/client"
	"github.com/mariadb-connector-go/mariadb/internal/protocol/server"
)

// Conn implements driver.Conn interface
// This is a thin wrapper around the internal client
type Conn struct {
	client  *client.Client
	config  *client.Config
	context *Context
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

	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	stmt := &Stmt{
		conn:  c,
		query: query,
	}

	if !c.context.CanPipelinePrepare() {
		if err := stmt.prepareInternal(); err != nil {
			return nil, err
		}
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

	// Use client's Exec method which handles buffering and packet creation
	completions, err := c.client.Exec(ctx, query)
	if err != nil {
		return nil, err
	}

	// Return the last non-result-set completion directly (implements driver.Result)
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

	// Use client's Query method which handles buffering and packet creation
	completions, err := c.client.Query(ctx, query)
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
		conn:        c,
		completions: completions,
	}

	// If the first completion has streaming rows, track as active
	if !completions[0].Loaded {
		c.client.SetActiveRows(rows)
	}

	return rows, nil
}

// Ping verifies the connection is still alive
func (c *Conn) Ping(ctx context.Context) error {
	c.client.Lock()
	defer c.client.Unlock()

	if c.client.IsClosed() {
		return driver.ErrBadConn
	}

	if err := c.client.Send(clientpkt.NewPing()); err != nil {
		return err
	}

	// Read response
	response, err := c.client.ReadPacket()
	if err != nil {
		return driver.ErrBadConn
	}

	// Check for error
	if len(response) > 0 && response[0] == 0xff {
		return server.ParseErrorPacket(response)
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
		if err := c.client.Send(clientpkt.NewResetConnection()); err != nil {
			return err
		}

		// Read response
		response, err := c.client.ReadPacket()
		if err != nil {
			return err
		}

		// Check for error
		if len(response) > 0 && response[0] == 0xff {
			return server.ParseErrorPacket(response)
		}
	}

	return nil
}

// execInternal executes a query without returning results (internal use)
// Must be called with client mutex locked
func (c *Conn) execInternal(ctx context.Context, query string) error {
	return c.client.ExecInternal(ctx, query)
}
