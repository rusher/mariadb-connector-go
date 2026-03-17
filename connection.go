// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/mariadb-connector-go/mariadb/internal/client"
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
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
	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	stmt := &Stmt{
		conn:   c,
		query:  query,
		stmtID: 0xFFFFFFFF,
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

		if err := c.client.ExecInternal(ctx, "SET TRANSACTION ISOLATION LEVEL "+isolationLevel); err != nil {
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
	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	result, err := c.client.Exec(ctx, query, args, c.context.NoBackslashEscapes())
	if err != nil {
		return nil, err
	}
	return result, nil
}

// QueryContext executes a query that may return rows
func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.client.IsClosed() {
		return nil, driver.ErrBadConn
	}

	comp, err := c.client.Query(ctx, query, args, c.context.NoBackslashEscapes())
	if err != nil {
		return nil, err
	}
	if !comp.HasResultSet() {
		return nil, fmt.Errorf("query did not return a result set")
	}
	rows := &Rows{
		conn:    c,
		current: comp,
	}
	if !comp.Loaded {
		c.client.SetActiveRows(rows)
	}
	return rows, nil
}

// Ping verifies the connection is still alive
func (c *Conn) Ping(ctx context.Context) error {
	if c.client.IsClosed() {
		return driver.ErrBadConn
	}
	stop, err := c.client.WithContext(ctx)
	if err != nil {
		return driver.ErrBadConn
	}
	defer stop()

	if err := c.client.Send(protocol.NewPing(c.client.WriterBuf())); err != nil {
		return err
	}

	response, err := c.client.ReadScratch()
	if err != nil {
		return driver.ErrBadConn
	}
	if len(response) > 0 && response[0] == 0xff {
		return protocol.ParseErrorPacket(response)
	}
	if len(response) == 0 || response[0] != 0x00 {
		return driver.ErrBadConn
	}
	return nil
}

// ResetSession is called prior to executing a query on the connection
// if the connection has been used before.
//
// Validation strategy:
//  1. Hard checks: closed client or dead socket → ErrBadConn immediately.
//  2. Skip check: if the last completed exchange was less than MinDelayValidation
//     ago, the connection is assumed healthy and no packet is sent.
//  3. Active check: send COM_PING (or COM_RESET_CONNECTION if
//     ResetConnectionOnBorrow=true and the server supports it) and expect OK.
func (c *Conn) ResetSession(ctx context.Context) error {
	if c.client.IsClosed() {
		return driver.ErrBadConn
	}

	cfg := c.client.Config()
	minDelay := cfg.MinDelayValidation
	if minDelay > 0 && time.Since(c.client.Context().LastExchange()) < minDelay {
		return nil
	}

	stop, err := c.client.WithContext(ctx)
	if err != nil {
		return driver.ErrBadConn
	}
	defer stop()

	if cfg.ResetConnectionOnBorrow && c.context.HasClientCapability(protocol.CLIENT_SESSION_TRACK) {
		if err := c.client.Send(protocol.NewResetConnection(c.client.WriterBuf())); err != nil {
			return driver.ErrBadConn
		}
	} else {
		if err := c.client.Send(protocol.NewPing(c.client.WriterBuf())); err != nil {
			return driver.ErrBadConn
		}
	}

	response, err := c.client.ReadScratch()
	if err != nil {
		return driver.ErrBadConn
	}
	if len(response) > 0 && response[0] == 0xff {
		return protocol.ParseErrorPacket(response)
	}
	return nil
}

// execInternal executes a query without returning results (internal use)
// Must be called with client mutex locked
func (c *Conn) execInternal(ctx context.Context, query string) error {
	return c.client.ExecInternal(ctx, query)
}
