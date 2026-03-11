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

// ParseOkPacket parses an OK packet from raw bytes into a Completion and updates context
func ParseOkPacket(data []byte, ctx ContextUpdater) (*protocol.Completion, error) {
	if len(data) < 7 {
		return nil, protocol.ErrMalformedPacket
	}

	pos := 1 // Skip 0x00 or 0xFE header

	affectedRows, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return nil, err
	}
	pos = newPos

	lastInsertId, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return nil, err
	}
	pos = newPos

	if pos+4 > len(data) {
		return nil, protocol.ErrMalformedPacket
	}
	serverStatus := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2
	warningCount := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	if ctx != nil {
		ctx.SetServerStatus(serverStatus)
		ctx.SetWarningCount(int(warningCount))
	}

	completion := &protocol.Completion{
		AffectedRows: int64(affectedRows),
		InsertID:     int64(lastInsertId),
		ServerStatus: serverStatus,
		WarningCount: warningCount,
		Loaded:       true,
	}

	if pos < len(data) {
		info, newPos, err := protocol.ReadLengthEncodedString(data, pos)
		if err == nil {
			completion.Message = info
			pos = newPos
		}
	}

	if pos < len(data) {
		stateLen, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
		if err == nil {
			pos = newPos
			if pos+int(stateLen) <= len(data) {
				if ctx != nil {
					parseSessionState(data[pos:pos+int(stateLen)], ctx)
				}
			}
		}
	}

	return completion, nil
}

// parseSessionState parses session state changes and updates context
func parseSessionState(data []byte, ctx ContextUpdater) {
	pos := 0
	for pos < len(data) {
		stateType := data[pos]
		pos++

		stateDataLen, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
		if err != nil || newPos+int(stateDataLen) > len(data) {
			break
		}
		pos = newPos
		pos += int(stateDataLen)

		switch stateType {
		case protocol.SESSION_TRACK_SCHEMA:
		case protocol.SESSION_TRACK_SYSTEM_VARIABLES:
		default:
		}
	}
}
