// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"context"
	"fmt"
	"net"
	"sync"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
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

	closed bool
	mu     sync.Mutex
}

// NewClient creates a new client instance
func NewClient(config *Config) *Client {
	return &Client{
		config: config,
	}
}

// Connect establishes the network connection and performs handshake
func (c *Client) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	var err error

	// Establish network connection
	if c.config.Protocol == "unix" {
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

	// Initialize packet reader/writer with shared sequence
	c.sequence = 0
	c.reader = protocol.NewPacketReader(c.netConn, &c.sequence)
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
	handshake, err := protocol.ParseHandshakePacket(data)
	if err != nil {
		return fmt.Errorf("failed to parse handshake packet: %w", err)
	}

	// Initialize client capabilities based on configuration and server capabilities
	clientCaps := protocol.InitializeClientCapabilities(c.config, handshake.ServerCapabilities, c.config.Database)

	// Create connection context
	c.context = NewContext(c.config, handshake, clientCaps)

	// Build handshake response
	response, err := protocol.BuildHandshakeResponse(
		c.config,
		handshake,
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

	// Build COM_QUERY packet
	packet := make([]byte, 1+len(query))
	packet[0] = protocol.COM_QUERY
	copy(packet[1:], query)

	// Send command (resets sequence)
	c.sequence = 0
	if err := c.writer.WritePacket(packet); err != nil {
		return fmt.Errorf("failed to send SET NAMES query: %w", err)
	}

	// Read response
	data, err := c.reader.ReadPacket()
	if err != nil {
		return fmt.Errorf("failed to read SET NAMES response: %w", err)
	}

	// Check for error packet
	if len(data) > 0 && data[0] == 0xff {
		return protocol.ParseErrorPacket(data)
	}

	// Should be OK packet - parse it to update context
	if len(data) > 0 && data[0] == 0x00 {
		_, _ = server.ParseOkPacket(data, c.context)
	}

	return nil
}

// SendCommand resets sequence and sends a command packet
func (c *Client) SendCommand(commandPacket []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Reset sequence for new command
	c.sequence = 0
	return c.writer.WritePacket(commandPacket)
}

// ReadPacket reads a packet from the server
func (c *Client) ReadPacket() ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.reader.ReadPacket()
}

// WritePacket writes a packet to the server
func (c *Client) WritePacket(data []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.writer.WritePacket(data)
}

// Close closes the client connection
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil
	}

	c.closed = true

	// Send COM_QUIT
	if c.writer != nil {
		quitPacket := []byte{protocol.COM_QUIT}
		c.writer.WritePacket(quitPacket)
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

// Lock locks the client mutex
func (c *Client) Lock() {
	c.mu.Lock()
}

// Unlock unlocks the client mutex
func (c *Client) Unlock() {
	c.mu.Unlock()
}

// Sequence returns a pointer to the sequence number
func (c *Client) Sequence() *uint8 {
	return &c.sequence
}

// ExecInternal executes a query without returning results (internal use)
func (c *Client) ExecInternal(ctx context.Context, query string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Build COM_QUERY packet
	packet := make([]byte, 1+len(query))
	packet[0] = protocol.COM_QUERY
	copy(packet[1:], query)

	// Send command (resets sequence)
	c.sequence = 0
	if err := c.writer.WritePacket(packet); err != nil {
		return err
	}

	// Read response
	response, err := c.reader.ReadPacket()
	if err != nil {
		return err
	}

	// Check for error
	if len(response) > 0 && response[0] == 0xff {
		return protocol.ParseErrorPacket(response)
	}

	return nil
}
