// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package server

import (
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// EOFPacket represents an EOF packet from the server
// See https://mariadb.com/kb/en/eof_packet/
//
// Note: When CLIENT_DEPRECATE_EOF is set, EOF is sent as an OK packet with 0xFE header
type EOFPacket struct {
	sequence     uint8
	warningCount uint16
	serverStatus uint16
}

// ParseEOFPacket parses a traditional EOF packet from raw bytes and updates context
// Based on MariaDB Java connector Result.readNext()
// Note: This is only called for traditional EOF packets (not when CLIENT_DEPRECATE_EOF is enabled)
func ParseEOFPacket(data []byte, ctx ContextUpdater) (*EOFPacket, error) {
	if len(data) < 5 {
		return nil, protocol.ErrMalformedPacket
	}

	pos := 1 // Skip 0xFE header

	// Traditional EOF_Packet format:
	// 1 byte: 0xFE header
	// 2 bytes: warning count
	// 2 bytes: server status flags
	if pos+4 > len(data) {
		return nil, protocol.ErrMalformedPacket
	}

	warningCount := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2
	serverStatus := uint16(data[pos]) | uint16(data[pos+1])<<8

	// Update context with server status and warning count
	if ctx != nil {
		ctx.SetServerStatus(serverStatus)
		ctx.SetWarningCount(int(warningCount))
	}

	return &EOFPacket{
		warningCount: warningCount,
		serverStatus: serverStatus,
	}, nil
}

// GetSequence returns the packet sequence number
func (p *EOFPacket) GetSequence() uint8 {
	return p.sequence
}

// GetWarningCount returns the warning count
func (p *EOFPacket) GetWarningCount() uint16 {
	return p.warningCount
}

// GetServerStatus returns the server status flags
func (p *EOFPacket) GetServerStatus() uint16 {
	return p.serverStatus
}
