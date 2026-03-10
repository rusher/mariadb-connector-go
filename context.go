// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"github.com/mariadb-connector-go/mariadb/internal/client"
)

// Context represents the current connection state
// This is a wrapper around the internal client context
type Context struct {
	internal *client.Context
}

// HasServerCapability checks if server has a specific capability
func (c *Context) HasServerCapability(flag uint64) bool {
	return c.internal.HasServerCapability(flag)
}

// HasClientCapability checks if client has a specific capability (negotiated)
func (c *Context) HasClientCapability(flag uint64) bool {
	return c.internal.HasClientCapability(flag)
}

// GetServerCapabilities returns the server capabilities
func (c *Context) GetServerCapabilities() uint64 {
	return c.internal.GetServerCapabilities()
}

// GetClientCapabilities returns the negotiated client capabilities
func (c *Context) GetClientCapabilities() uint64 {
	return c.internal.GetClientCapabilities()
}

// GetThreadID returns the server thread ID
func (c *Context) GetThreadID() uint32 {
	return c.internal.GetThreadID()
}

// SetThreadID sets the server thread ID
func (c *Context) SetThreadID(id uint32) {
	c.internal.SetThreadID(id)
}

// GetServerStatus returns the server status flags
func (c *Context) GetServerStatus() uint16 {
	return c.internal.GetServerStatus()
}

// SetServerStatus sets the server status flags
func (c *Context) SetServerStatus(status uint16) {
	c.internal.SetServerStatus(status)
}

// GetServerVersion returns the server version string
func (c *Context) GetServerVersion() string {
	return c.internal.GetServerVersion()
}

// IsEOFDeprecated returns whether EOF packets are deprecated
func (c *Context) IsEOFDeprecated() bool {
	return c.internal.IsEOFDeprecated()
}

// CanSkipMeta returns whether metadata can be skipped
func (c *Context) CanSkipMeta() bool {
	return c.internal.CanSkipMeta()
}

// GetWarningCount returns the warning count
func (c *Context) GetWarningCount() int {
	return c.internal.GetWarningCount()
}

// SetWarningCount sets the warning count
func (c *Context) SetWarningCount(count int) {
	c.internal.SetWarningCount(count)
}

// GetTransactionIsolationLevel returns the transaction isolation level
func (c *Context) GetTransactionIsolationLevel() int {
	return c.internal.GetTransactionIsolationLevel()
}

// SetTransactionIsolationLevel sets the transaction isolation level
func (c *Context) SetTransactionIsolationLevel(level int) {
	c.internal.SetTransactionIsolationLevel(level)
}

// GetAutoIncrement returns the last auto increment value
func (c *Context) GetAutoIncrement() *int64 {
	return c.internal.GetAutoIncrement()
}

// SetAutoIncrement sets the last auto increment value
func (c *Context) SetAutoIncrement(value int64) {
	c.internal.SetAutoIncrement(value)
}

// GetCharset returns the connection charset
func (c *Context) GetCharset() string {
	return c.internal.GetCharset()
}

// SetCharset sets the connection charset
func (c *Context) SetCharset(charset string) {
	c.internal.SetCharset(charset)
}
