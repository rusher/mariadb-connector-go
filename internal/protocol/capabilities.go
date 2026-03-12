// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

// DefaultClientCapabilities is the baseline set of capabilities requested on every connection.
const DefaultClientCapabilities uint64 = IGNORE_SPACE |
	CLIENT_PROTOCOL_41 |
	TRANSACTIONS |
	SECURE_CONNECTION |
	MULTI_RESULTS |
	PS_MULTI_RESULTS |
	PLUGIN_AUTH |
	CONNECT_ATTRS |
	PLUGIN_AUTH_LENENC_CLIENT_DATA |
	CLIENT_DEPRECATE_EOF

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
	capabilities := IGNORE_SPACE |
		CLIENT_PROTOCOL_41 |
		TRANSACTIONS |
		SECURE_CONNECTION |
		MULTI_RESULTS |
		PS_MULTI_RESULTS |
		PLUGIN_AUTH |
		CONNECT_ATTRS |
		PLUGIN_AUTH_LENENC_CLIENT_DATA |
		CLIENT_INTERACTIVE |
		CACHE_METADATA |
		EXTENDED_METADATA |
		CLIENT_DEPRECATE_EOF

	if !config.UseAffectedRows() {
		capabilities |= FOUND_ROWS
	}
	if config.AllowMultiStatements() {
		capabilities |= MULTI_STATEMENTS
	}
	if config.AllowLocalInfile() {
		capabilities |= LOCAL_FILES
	}
	if config.UseCompression() {
		capabilities |= COMPRESS
	}
	if database != "" {
		capabilities |= CONNECT_WITH_DB
	}

	return capabilities & serverCapabilities
}
