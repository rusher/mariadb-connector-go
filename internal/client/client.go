// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"bufio"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
	clientpkt "github.com/mariadb-connector-go/mariadb/internal/protocol/client"
	"github.com/mariadb-connector-go/mariadb/internal/protocol/server"
)

// Client represents the internal MariaDB client implementation
// This handles all low-level protocol communication and connection management
type Client struct {
	config  *Config
	context *Context

	netConn  net.Conn
	reader   *protocol.PacketReader
	writer   *protocol.PacketWriter
	sequence uint8 // Shared packet sequence number

	// Active result set tracking
	activeRows interface{} // Currently active streaming result set (type will be *Rows from parent package)

	closed bool
}

// pastDeadline is used to immediately expire any pending network I/O.
var pastDeadline = time.Unix(1, 0)

// WithContext starts a watchdog goroutine that cancels pending network I/O
// by setting a past deadline when ctx is done. The returned stop function
// must always be called (e.g. via defer). It is a no-op when ctx can never
// be cancelled (ctx.Done() == nil, e.g. context.Background).
func (c *Client) WithContext(ctx context.Context) (func(), error) {
	if ctx.Done() == nil {
		return func() {}, nil
	}
	select {
	case <-ctx.Done():
		return func() {}, ctx.Err()
	default:
	}
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			c.netConn.SetDeadline(pastDeadline)
		case <-done:
		}
	}()
	return func() {
		close(done)
		if ctx.Err() != nil {
			// Connection state is indeterminate after a mid-I/O cancellation.
			c.closed = true
			c.netConn.Close()
		} else {
			c.netConn.SetDeadline(time.Time{})
		}
	}, nil
}

// NewClient creates a new client instance
func NewClient(config *Config) *Client {
	return &Client{
		config: config,
	}
}

// Connect establishes the network connection and performs handshake
func (c *Client) Connect(ctx context.Context) error {
	var err error

	// Establish network connection
	if c.config.Net == "unix" {
		c.netConn, err = net.Dial("unix", c.config.Socket)
	} else {
		addr := fmt.Sprintf("%s:%d", c.config.Host, c.config.Port)
		dialer := &net.Dialer{
			Timeout: c.config.Timeout,
		}
		c.netConn, err = dialer.DialContext(ctx, "tcp", addr)
	}

	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	// Set timeouts
	if c.config.ReadTimeout > 0 {
		// Will be set per-operation
	}
	if c.config.WriteTimeout > 0 {
		// Will be set per-operation
	}

	// Enable debug logging if configured (MUST be set before creating reader/writer)
	if c.config.Debug {
		protocol.SetLogger(protocol.NewDebugLogger(true))
	}

	// Initialize packet reader/writer with shared sequence.
	// The reader is wrapped in a 64 KB bufio.Reader to batch kernel reads:
	// without buffering, every ReadPacket call makes two syscalls (header + data).
	c.sequence = 0
	c.reader = protocol.NewPacketReader(bufio.NewReaderSize(c.netConn, 65536), &c.sequence)
	c.writer = protocol.NewPacketWriter(c.netConn, &c.sequence)

	// Perform handshake
	if err := c.handshake(ctx); err != nil {
		c.netConn.Close()
		return fmt.Errorf("handshake failed: %w", err)
	}

	return nil
}

// handshake performs the MySQL/MariaDB handshake
func (c *Client) handshake(ctx context.Context) error {
	// Read initial handshake packet from server (sequence 0)
	data, err := c.reader.ReadPacket()
	if err != nil {
		return fmt.Errorf("failed to read handshake packet: %w", err)
	}

	// Parse handshake packet
	handshake, err := server.ParseHandshakePacket(data)
	if err != nil {
		return fmt.Errorf("failed to parse handshake packet: %w", err)
	}

	// Initialize client capabilities based on configuration and server capabilities
	clientCaps := protocol.InitializeClientCapabilities(c.config, handshake.ServerCapabilities, c.config.DBName)

	// Create connection context
	c.context = NewContext(c.config, handshake, clientCaps)

	// Build handshake response
	response, err := server.BuildHandshakeResponse(
		c.config,
		handshake,
		clientCaps,
	)
	if err != nil {
		return fmt.Errorf("failed to build handshake response: %w", err)
	}

	// Send handshake response (sequence 1)
	if err := c.writer.WritePacket(response); err != nil {
		return fmt.Errorf("failed to send handshake response: %w", err)
	}

	// Read authentication result (sequence 2)
	authResult, err := c.reader.ReadPacket()
	if err != nil {
		return fmt.Errorf("failed to read auth result: %w", err)
	}

	// Handle authentication result with plugin support
	if err := c.handleAuthResult(authResult, handshake.Salt, handshake.AuthPluginName, 10); err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	// Reset sequence for next command
	c.sequence = 0

	// Validate and set charset
	if err := c.config.ValidateCharset(); err != nil {
		return fmt.Errorf("invalid charset: %w", err)
	}

	// Execute SET NAMES to ensure UTF-8 encoding
	if err := c.setCharset(); err != nil {
		return fmt.Errorf("failed to set charset: %w", err)
	}

	return nil
}

// setCharset executes SET NAMES to configure the connection charset
func (c *Client) setCharset() error {
	// Build SET NAMES query
	var query string
	if c.config.Collation != "" {
		query = fmt.Sprintf("SET NAMES %s COLLATE %s", c.config.Charset, c.config.Collation)
	} else {
		query = fmt.Sprintf("SET NAMES %s", c.config.Charset)
	}

	// Send command (resets sequence)
	c.sequence = 0
	if err := c.writer.Write(clientpkt.NewQuery(query)); err != nil {
		return fmt.Errorf("failed to send SET NAMES query: %w", err)
	}

	// Read response
	data, err := c.reader.ReadPacket()
	if err != nil {
		return fmt.Errorf("failed to read SET NAMES response: %w", err)
	}

	// Check for error packet
	if len(data) > 0 && data[0] == 0xff {
		return server.ParseErrorPacket(data)
	}

	// Should be OK packet - parse it to update context
	if len(data) > 0 && data[0] == 0x00 {
		_, _ = server.ParseOkPacket(data, c.context)
	}

	return nil
}

// Send resets the sequence and sends a packet whose first 4 bytes are reserved
// for the header (as returned by clientpkt constructors).
// Must be called with client mutex locked.
func (c *Client) Send(buf []byte) error {
	c.sequence = 0
	return c.writer.Write(buf)
}

// SendNext sends a continuation packet without resetting the sequence number.
// Use after Send to pipeline multiple packets before reading any response.
// Must be called with client mutex locked.
func (c *Client) SendNext(buf []byte) error {
	return c.writer.Write(buf)
}

// ReadPrepareResponse reads and parses a COM_STMT_PREPARE response.
// It returns the statement ID and consumes the param/column definition packets.
// Must be called with client mutex locked.
func (c *Client) ReadPrepareResponse() (stmtID uint32, paramCount uint16, columnCount uint16, err error) {
	response, err := c.reader.ReadPacket()
	if err != nil {
		return 0, 0, 0, err
	}

	if len(response) > 0 && response[0] == 0xff {
		return 0, 0, 0, server.ParseErrorPacket(response)
	}

	if len(response) < 12 || response[0] != 0x00 {
		return 0, 0, 0, fmt.Errorf("unexpected COM_STMT_PREPARE response (first byte: 0x%02x)", response[0])
	}

	stmtID = binary.LittleEndian.Uint32(response[1:])
	columnCount = binary.LittleEndian.Uint16(response[5:])
	paramCount = binary.LittleEndian.Uint16(response[7:])

	for i := 0; i < int(paramCount); i++ {
		if _, err = c.reader.ReadPacket(); err != nil {
			return 0, 0, 0, err
		}
	}
	if paramCount > 0 && !c.context.IsEOFDeprecated() {
		if _, err = c.reader.ReadPacket(); err != nil {
			return 0, 0, 0, err
		}
	}

	for i := 0; i < int(columnCount); i++ {
		if _, err = c.reader.ReadPacket(); err != nil {
			return 0, 0, 0, err
		}
	}
	if columnCount > 0 && !c.context.IsEOFDeprecated() {
		if _, err = c.reader.ReadPacket(); err != nil {
			return 0, 0, 0, err
		}
	}

	return stmtID, paramCount, columnCount, nil
}

// ReadPacket reads a packet from the server.
func (c *Client) ReadPacket() ([]byte, error) {
	return c.reader.ReadPacket()
}

// WritePacket writes a raw (no header reservation) packet to the server.
func (c *Client) WritePacket(data []byte) error {
	return c.writer.WritePacket(data)
}

// Close closes the client connection
func (c *Client) Close() error {
	if c.closed {
		return nil
	}

	c.closed = true

	// Send COM_QUIT
	if c.writer != nil {
		c.writer.Write(clientpkt.NewQuit()) //nolint:errcheck
	}

	if c.netConn != nil {
		return c.netConn.Close()
	}

	return nil
}

// IsClosed returns whether the client is closed
// Note: This does NOT lock - caller must hold the lock if needed
func (c *Client) IsClosed() bool {
	return c.closed
}

// Context returns the connection context
func (c *Client) Context() *Context {
	return c.context
}

// Reader returns the packet reader
func (c *Client) Reader() *protocol.PacketReader {
	return c.reader
}

// Writer returns the packet writer
func (c *Client) Writer() *protocol.PacketWriter {
	return c.writer
}

// Sequence returns a pointer to the sequence number
func (c *Client) Sequence() *uint8 {
	return &c.sequence
}

// ExecInternal executes a query without returning results.
func (c *Client) ExecInternal(ctx context.Context, query string) error {
	stop, err := c.WithContext(ctx)
	if err != nil {
		return err
	}
	defer stop()
	c.sequence = 0
	if err := c.writer.Write(clientpkt.NewQuery(query)); err != nil {
		return err
	}

	response, err := c.reader.ReadPacket()
	if err != nil {
		return err
	}

	if len(response) > 0 && response[0] == 0xff {
		return server.ParseErrorPacket(response)
	}

	return nil
}
