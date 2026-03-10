// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

// CapabilityConfig interface for capability initialization
type CapabilityConfig interface {
	AllowMultiQueries() bool
	UseAffectedRows() bool
	AllowLocalInfile() bool
	UseCompression() bool
	UseBulkStmts() bool
}

// InitializeClientCapabilities initializes client capabilities based on configuration
func InitializeClientCapabilities(config CapabilityConfig, serverCapabilities uint64, database string) uint64 {
	// Base capabilities - always enabled
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

	// Optional capabilities based on configuration
	if !config.UseAffectedRows() {
		capabilities |= FOUND_ROWS
	}
	if config.AllowMultiQueries() {
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

	// Return negotiated capabilities (client AND server)
	return capabilities & serverCapabilities
}
