// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// Context represents the current connection state
type Context struct {
	// Server information
	serverCapabilities uint64
	clientCapabilities uint64
	serverVersion      string
	threadID           uint32
	serverStatus       uint16
	charset            string

	// Connection state
	transactionIsolationLevel int
	warningCount              int
	autoIncrement             *int64

	// Capability-derived flags
	eofDeprecated bool
	skipMeta      bool

	// Configuration
	config *Config
}

// NewContext creates a new connection context from handshake
func NewContext(config *Config, handshake *protocol.HandshakePacket, clientCaps uint64) *Context {
	// Negotiated capabilities = client AND server
	negotiatedCaps := clientCaps & handshake.ServerCapabilities

	ctx := &Context{
		serverCapabilities: handshake.ServerCapabilities,
		clientCapabilities: negotiatedCaps,
		serverVersion:      handshake.ServerVersion,
		threadID:           handshake.ConnectionID,
		serverStatus:       handshake.ServerStatus,
		config:             config,
	}

	// Set capability-derived flags
	ctx.eofDeprecated = ctx.HasClientCapability(protocol.CLIENT_DEPRECATE_EOF)
	ctx.skipMeta = ctx.HasClientCapability(protocol.CACHE_METADATA)

	return ctx
}

// HasServerCapability checks if server has a specific capability
func (c *Context) HasServerCapability(flag uint64) bool {
	return (c.serverCapabilities & flag) != 0
}

// HasClientCapability checks if client has a specific capability (negotiated)
func (c *Context) HasClientCapability(flag uint64) bool {
	return (c.clientCapabilities & flag) != 0
}

// GetServerCapabilities returns the server capabilities
func (c *Context) GetServerCapabilities() uint64 {
	return c.serverCapabilities
}

// GetClientCapabilities returns the negotiated client capabilities
func (c *Context) GetClientCapabilities() uint64 {
	return c.clientCapabilities
}

// GetThreadID returns the server thread ID
func (c *Context) GetThreadID() uint32 {
	return c.threadID
}

// SetThreadID sets the server thread ID
func (c *Context) SetThreadID(id uint32) {
	c.threadID = id
}

// GetServerStatus returns the server status flags
func (c *Context) GetServerStatus() uint16 {
	return c.serverStatus
}

// SetServerStatus sets the server status flags
func (c *Context) SetServerStatus(status uint16) {
	c.serverStatus = status
}

// GetServerVersion returns the server version string
func (c *Context) GetServerVersion() string {
	return c.serverVersion
}

// IsEOFDeprecated returns whether EOF packets are deprecated
func (c *Context) IsEOFDeprecated() bool {
	return c.eofDeprecated
}

// CanSkipMeta returns whether metadata can be skipped
func (c *Context) CanSkipMeta() bool {
	return c.skipMeta
}

// GetWarningCount returns the warning count
func (c *Context) GetWarningCount() int {
	return c.warningCount
}

// SetWarningCount sets the warning count
func (c *Context) SetWarningCount(count int) {
	c.warningCount = count
}

// GetTransactionIsolationLevel returns the transaction isolation level
func (c *Context) GetTransactionIsolationLevel() int {
	return c.transactionIsolationLevel
}

// SetTransactionIsolationLevel sets the transaction isolation level
func (c *Context) SetTransactionIsolationLevel(level int) {
	c.transactionIsolationLevel = level
}

// GetAutoIncrement returns the last auto increment value
func (c *Context) GetAutoIncrement() *int64 {
	return c.autoIncrement
}

// SetAutoIncrement sets the last auto increment value
func (c *Context) SetAutoIncrement(value int64) {
	c.autoIncrement = &value
}

// GetCharset returns the connection charset
func (c *Context) GetCharset() string {
	return c.charset
}

// SetCharset sets the connection charset
func (c *Context) SetCharset(charset string) {
	c.charset = charset
}
