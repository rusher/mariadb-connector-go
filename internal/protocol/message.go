// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

// ServerMessage represents a message from the server
// Based on MariaDB Java connector ServerMessage interface
type ServerMessage interface {
	// GetSequence returns the packet sequence number
	GetSequence() uint8
}

// ClientMessage represents a message to the server
// Based on MariaDB Java connector ClientMessage interface
type ClientMessage interface {
	// Encode encodes the message to bytes
	Encode() ([]byte, error)
}

// Completion represents a command completion (OK or Error)
type Completion interface {
	ServerMessage
	GetAffectedRows() int64
	GetLastInsertId() int64
}
