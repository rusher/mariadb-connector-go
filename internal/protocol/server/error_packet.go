// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package server

import (
	"fmt"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// ErrorPacket represents an error packet from the server
// See https://mariadb.com/kb/en/err_packet/
type ErrorPacket struct {
	sequence  uint8
	errorCode uint16
	sqlState  string
	message   string
}

// ParseErrorPacket parses an error packet from raw bytes
func ParseErrorPacket(data []byte) (*ErrorPacket, error) {
	if len(data) < 9 {
		return nil, protocol.ErrMalformedPacket
	}

	if data[0] != 0xff {
		return nil, fmt.Errorf("not an error packet: header is 0x%02x", data[0])
	}

	pos := 1

	// Error code (2 bytes)
	if pos+2 > len(data) {
		return nil, protocol.ErrMalformedPacket
	}
	errorCode := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	var sqlState string

	// Check for SQL state marker '#'
	if pos < len(data) && data[pos] == '#' {
		pos++ // Skip '#'
		if pos+5 <= len(data) {
			sqlState = string(data[pos : pos+5])
			pos += 5
		}
	}

	// Rest is the error message
	message := ""
	if pos < len(data) {
		message = string(data[pos:])
	}

	return &ErrorPacket{
		errorCode: errorCode,
		sqlState:  sqlState,
		message:   message,
	}, nil
}

// GetSequence returns the packet sequence number
func (p *ErrorPacket) GetSequence() uint8 {
	return p.sequence
}

// GetErrorCode returns the MariaDB error code
func (p *ErrorPacket) GetErrorCode() uint16 {
	return p.errorCode
}

// GetSQLState returns the SQL state
func (p *ErrorPacket) GetSQLState() string {
	return p.sqlState
}

// GetMessage returns the error message
func (p *ErrorPacket) GetMessage() string {
	return p.message
}

// Error implements the error interface
func (p *ErrorPacket) Error() string {
	if p.sqlState != "" {
		return fmt.Sprintf("Error %d (%s): %s", p.errorCode, p.sqlState, p.message)
	}
	return fmt.Sprintf("Error %d: %s", p.errorCode, p.message)
}

// IsErrorPacket checks if a packet is an error packet
func IsErrorPacket(data []byte) bool {
	return len(data) > 0 && data[0] == 0xff
}
