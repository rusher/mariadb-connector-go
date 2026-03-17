// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package handshake

import "github.com/mariadb-connector-go/mariadb/internal/protocol"

// CapabilityConfig is the interface that connection configuration must satisfy
// to participate in capability negotiation.
type CapabilityConfig interface {
	AllowMultiStatements() bool
	UseAffectedRows() bool
	AllowLocalInfile() bool
	UseCompression() bool
	UseBulkStmts() bool
}

// InitializeClientCapabilities builds the capability bitmask for a new connection
// by intersecting the desired client capabilities with what the server supports.
func InitializeClientCapabilities(config CapabilityConfig, serverCapabilities uint64, database string) uint64 {
	capabilities := protocol.IGNORE_SPACE |
		protocol.CLIENT_PROTOCOL_41 |
		protocol.TRANSACTIONS |
		protocol.SECURE_CONNECTION |
		protocol.MULTI_RESULTS |
		protocol.PS_MULTI_RESULTS |
		protocol.PLUGIN_AUTH |
		protocol.CONNECT_ATTRS |
		protocol.PLUGIN_AUTH_LENENC_CLIENT_DATA |
		protocol.CLIENT_INTERACTIVE |
		protocol.EXTENDED_METADATA |
		protocol.CLIENT_DEPRECATE_EOF |
		protocol.STMT_BULK_OPERATIONS |
		protocol.CACHE_METADATA


	if !config.UseAffectedRows() {
		capabilities |= protocol.FOUND_ROWS
	}
	if config.AllowMultiStatements() {
		capabilities |= protocol.MULTI_STATEMENTS
	}
	if config.AllowLocalInfile() {
		capabilities |= protocol.LOCAL_FILES
	}
	if config.UseCompression() {
		capabilities |= protocol.COMPRESS
	}
	if database != "" {
		capabilities |= protocol.CONNECT_WITH_DB
	}

	return capabilities & serverCapabilities
}
