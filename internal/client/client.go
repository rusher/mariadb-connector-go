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

	"github.com/mariadb-connector-go/mariadb/internal/client/handshake"
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
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

	completion protocol.Completion // current result — reused per query, no allocation

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

	// Initialize packet reader/writer with shared sequence.
	// The reader is wrapped in a 256 KB bufio.Reader to batch kernel reads:
	c.sequence = 0
	c.reader = protocol.NewPacketReader(bufio.NewReaderSize(c.netConn, 256*1024), &c.sequence)
	c.writer = protocol.NewPacketWriter(c.netConn, &c.sequence)

	// Perform handshake
	err = handshake.Perform(
		c.reader,
		c.writer,
		&c.sequence,
		c.config,
		func(hs *handshake.HandshakePacket, clientCaps uint64) protocol.ContextUpdater {
			c.context = NewContext(c.config, hs.ServerCapabilities, hs.ServerVersion, hs.ConnectionID, hs.ServerStatus, clientCaps)
			return c.context
		},
	)
	if err != nil {
		c.netConn.Close()
		return fmt.Errorf("handshake failed: %w", err)
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

// StartNextRead resets the sequence to 1, ready to read the first response
// packet of the next pipelined command (server always starts responses at seq=1).
// Call this between ReadPrepareResponse and ReadCompletion when pipelining
// COM_STMT_PREPARE + COM_STMT_EXECUTE.
func (c *Client) StartNextRead() {
	c.sequence = 1
}

// WriterBuf returns the writer's scratch buffer for use by packet constructors.
// The returned slice must not be retained across Write calls.
func (c *Client) WriterBuf() []byte {
	return c.writer.Buf()
}

// ReadPrepareResponse reads and parses a COM_STMT_PREPARE response.
// It returns the statement ID, counts, and parsed column definitions.
// Must be called with client mutex locked.
func (c *Client) ReadPrepareResponse() (stmtID uint32, paramCount uint16, columnCount uint16, columns []protocol.ColumnDefinition, err error) {
	response, err := c.reader.ReadScratch()
	if err != nil {
		return 0, 0, 0, nil, err
	}

	if len(response) > 0 && response[0] == 0xff {
		return 0, 0, 0, nil, protocol.ParseErrorPacket(response)
	}

	if len(response) < 12 || response[0] != 0x00 {
		return 0, 0, 0, nil, fmt.Errorf("unexpected COM_STMT_PREPARE response (first byte: 0x%02x)", response[0])
	}

	stmtID = binary.LittleEndian.Uint32(response[1:])
	columnCount = binary.LittleEndian.Uint16(response[5:])
	paramCount = binary.LittleEndian.Uint16(response[7:])

	// Consume param definitions (not used; we rely on the caller's arg list).
	for i := 0; i < int(paramCount); i++ {
		if _, err = c.reader.ReadScratch(); err != nil {
			return 0, 0, 0, nil, err
		}
	}
	if paramCount > 0 && !c.context.IsEOFDeprecated() {
		if _, err = c.reader.ReadScratch(); err != nil {
			return 0, 0, 0, nil, err
		}
	}

	// Parse column definitions — seed the CACHE_METADATA column cache.
	if columnCount > 0 {
		columns = make([]protocol.ColumnDefinition, columnCount)
		for i := range columns {
			colData, rerr := c.reader.ReadScratch()
			if rerr != nil {
				return 0, 0, 0, nil, rerr
			}
			if rerr = protocol.FillColumnDefinition(colData, &columns[i], c.context.IsExtendedMetadata()); rerr != nil {
				return 0, 0, 0, nil, rerr
			}
		}
		if !c.context.IsEOFDeprecated() {
			if _, err = c.reader.ReadScratch(); err != nil {
				return 0, 0, 0, nil, err
			}
		}
	}

	return stmtID, paramCount, columnCount, columns, nil
}

// ReadScratch reads a packet using the reader's scratch buffer.
// The returned slice is only valid until the next read call.
func (c *Client) ReadScratch() ([]byte, error) {
	return c.reader.ReadScratch()
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
		c.writer.Write(protocol.NewQuit(c.writer.Buf())) //nolint:errcheck
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

// Config returns the client configuration.
func (c *Client) Config() *Config {
	return c.config
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
	if err := c.writer.Write(protocol.NewQuery(c.writer.Buf(), query)); err != nil {
		return err
	}

	response, err := c.reader.ReadScratch()
	if err != nil {
		return err
	}

	if len(response) > 0 && response[0] == 0xff {
		return protocol.ParseErrorPacket(response)
	}

	return nil
}
