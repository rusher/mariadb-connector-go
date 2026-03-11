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
	_ = data[6]

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

	_ = data[pos+3]
	serverStatus := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2
	warningCount := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	if ctx != nil {
		ctx.SetServerStatus(serverStatus)
		ctx.SetWarningCount(int(warningCount))
	}

	completion := &protocol.Completion{}
	return completion, fillOkPacket(data, ctx, pos, affectedRows, lastInsertId, serverStatus, warningCount, completion)
}

// FillOkPacket parses an OK packet into a pre-allocated Completion.
func FillOkPacket(data []byte, ctx ContextUpdater, completion *protocol.Completion) error {
	_ = data[6]
	pos := 1
	affectedRows, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return err
	}
	pos = newPos
	lastInsertId, newPos, err := protocol.ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return err
	}
	pos = newPos
	_ = data[pos+3]
	serverStatus := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2
	warningCount := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2
	if ctx != nil {
		ctx.SetServerStatus(serverStatus)
		ctx.SetWarningCount(int(warningCount))
	}
	return fillOkPacket(data, ctx, pos, affectedRows, lastInsertId, serverStatus, warningCount, completion)
}

func fillOkPacket(data []byte, ctx ContextUpdater, pos int, affectedRows, lastInsertId uint64, serverStatus, warningCount uint16, completion *protocol.Completion) error {
	*completion = protocol.Completion{
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
	return nil
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

// ParseEOFPacket parses a traditional EOF packet from raw bytes into a Completion and updates context.
// Only called when CLIENT_DEPRECATE_EOF is not negotiated.
func ParseEOFPacket(data []byte, ctx ContextUpdater) (*protocol.Completion, error) {
	_ = data[4]

	pos := 1 // Skip 0xFE header

	warningCount := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2
	serverStatus := uint16(data[pos]) | uint16(data[pos+1])<<8

	if ctx != nil {
		ctx.SetServerStatus(serverStatus)
		ctx.SetWarningCount(int(warningCount))
	}

	return &protocol.Completion{
		WarningCount: warningCount,
		ServerStatus: serverStatus,
		Loaded:       true,
	}, nil
}

// FillEOFPacket parses a traditional EOF packet into a pre-allocated Completion.
func FillEOFPacket(data []byte, ctx ContextUpdater, completion *protocol.Completion) error {
	_ = data[4]
	pos := 1
	warningCount := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2
	serverStatus := uint16(data[pos]) | uint16(data[pos+1])<<8
	if ctx != nil {
		ctx.SetServerStatus(serverStatus)
		ctx.SetWarningCount(int(warningCount))
	}
	*completion = protocol.Completion{
		WarningCount: warningCount,
		ServerStatus: serverStatus,
		Loaded:       true,
	}
	return nil
}
