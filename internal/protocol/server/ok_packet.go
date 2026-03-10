// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package server

import (
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// ContextUpdater interface for updating connection context
type ContextUpdater interface {
	SetServerStatus(status uint16)
	SetWarningCount(count int)
}

// OkPacket represents an OK packet from the server
// See https://mariadb.com/kb/en/ok_packet/
type OkPacket struct {
	sequence     uint8
	affectedRows int64
	lastInsertId int64
	serverStatus uint16
	warningCount uint16
	info         string
	sessionState []byte
}

// Parse parses an OK packet from raw bytes and updates context
func ParseOkPacket(data []byte, ctx ContextUpdater) (*OkPacket, error) {
	if len(data) < 7 {
		return nil, protocol.ErrMalformedPacket
	}

	pos := 1 // Skip 0x00 or 0xFE header

	// Read affected rows (length-encoded integer)
	affectedRows, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return nil, err
	}
	pos = newPos

	// Read last insert id (length-encoded integer)
	lastInsertId, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return nil, err
	}
	pos = newPos

	// Read server status (2 bytes)
	if pos+2 > len(data) {
		return nil, protocol.ErrMalformedPacket
	}
	serverStatus := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	// Read warning count (2 bytes)
	if pos+2 > len(data) {
		return nil, protocol.ErrMalformedPacket
	}
	warningCount := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	packet := &OkPacket{
		affectedRows: int64(affectedRows),
		lastInsertId: int64(lastInsertId),
		serverStatus: serverStatus,
		warningCount: warningCount,
	}

	// Update context with server status and warning count
	if ctx != nil {
		ctx.SetServerStatus(serverStatus)
		ctx.SetWarningCount(int(warningCount))
	}

	// Read info string if present (length-encoded)
	if pos < len(data) {
		info, newPos, err := protocol.ReadLengthEncodedString(data, pos)
		if err == nil {
			packet.info = info
			pos = newPos
		}
	}

	// Read and parse session state if present (CLIENT_SESSION_TRACK capability)
	if pos < len(data) {
		stateLen, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
		if err == nil {
			pos = newPos
			if pos+int(stateLen) <= len(data) {
				sessionStateData := data[pos : pos+int(stateLen)]
				packet.sessionState = sessionStateData

				// Parse session state changes and update context
				if ctx != nil {
					parseSessionState(sessionStateData, ctx)
				}
			}
		}
	}

	return packet, nil
}

// parseSessionState parses session state changes and updates context
func parseSessionState(data []byte, ctx ContextUpdater) {
	pos := 0
	for pos < len(data) {
		if pos >= len(data) {
			break
		}

		// Read state change type
		stateType := data[pos]
		pos++

		// Read state data length
		stateDataLen, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
		if err != nil || newPos+int(stateDataLen) > len(data) {
			break
		}
		pos = newPos
		pos += int(stateDataLen) // Skip state data

		switch stateType {
		case protocol.SESSION_TRACK_SCHEMA:
			// Database changes are not tracked in Go driver
			// Skip this session state change

		case protocol.SESSION_TRACK_SYSTEM_VARIABLES:
			// System variables - could parse these if needed in the future
			// For now, just skip

		default:
			// Unknown state change type - skip
		}
	}
}

// GetSequence returns the packet sequence number
func (p *OkPacket) GetSequence() uint8 {
	return p.sequence
}

// GetAffectedRows returns the number of affected rows
func (p *OkPacket) GetAffectedRows() int64 {
	return p.affectedRows
}

// GetLastInsertId returns the last insert ID
func (p *OkPacket) GetLastInsertId() int64 {
	return p.lastInsertId
}

// GetServerStatus returns the server status flags
func (p *OkPacket) GetServerStatus() uint16 {
	return p.serverStatus
}

// GetWarningCount returns the warning count
func (p *OkPacket) GetWarningCount() uint16 {
	return p.warningCount
}

// GetInfo returns the info string
func (p *OkPacket) GetInfo() string {
	return p.info
}
