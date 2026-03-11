// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package server

import (
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// ParseEOFPacket parses a traditional EOF packet from raw bytes into a Completion and updates context
// Note: This is only called for traditional EOF packets (not when CLIENT_DEPRECATE_EOF is enabled)
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
