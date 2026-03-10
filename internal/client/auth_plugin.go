// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"errors"
)

// AuthPlugin defines the interface for authentication plugins
type AuthPlugin interface {
	// PluginName returns the name of the authentication plugin
	PluginName() string

	// InitAuth performs the initial authentication step
	// Returns the authentication data to send to the server
	InitAuth(authData []byte, config *Config) ([]byte, error)

	// ContinuationAuth handles continuation of authentication
	// Returns:
	// - nextPacket: data to send to server (nil if no data to send)
	// - done: true if authentication is complete
	// - error: any error that occurred
	ContinuationAuth(authData []byte, seed []byte, config *Config) (nextPacket []byte, done bool, err error)
}

// SimpleAuth provides a base implementation for simple authentication plugins
// that don't require continuation
type SimpleAuth struct{}

// ContinuationAuth provides a no-op implementation for plugins that don't need continuation
func (s *SimpleAuth) ContinuationAuth(authData []byte, seed []byte, config *Config) ([]byte, bool, error) {
	return nil, true, nil
}

// Common authentication errors
var (
	ErrNativePassword    = errors.New("mysql_native_password authentication is disabled")
	ErrCleartextPassword = errors.New("mysql_clear_password authentication is disabled")
)
