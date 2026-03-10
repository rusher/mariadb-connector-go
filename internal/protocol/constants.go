// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"errors"
)

// Common errors
var (
	ErrMalformedPacket = errors.New("malformed packet")
)

// MySQL protocol version
const ProtocolVersion = 10

// Maximum packet size (16MB - 1)
const MaxPacketSize = 0xffffff

// Command types
const (
	COM_SLEEP               = 0x00
	COM_QUIT                = 0x01
	COM_INIT_DB             = 0x02
	COM_QUERY               = 0x03
	COM_FIELD_LIST          = 0x04
	COM_CREATE_DB           = 0x05
	COM_DROP_DB             = 0x06
	COM_REFRESH             = 0x07
	COM_SHUTDOWN            = 0x08
	COM_STATISTICS          = 0x09
	COM_PROCESS_INFO        = 0x0a
	COM_CONNECT             = 0x0b
	COM_PROCESS_KILL        = 0x0c
	COM_DEBUG               = 0x0d
	COM_PING                = 0x0e
	COM_TIME                = 0x0f
	COM_DELAYED_INSERT      = 0x10
	COM_CHANGE_USER         = 0x11
	COM_BINLOG_DUMP         = 0x12
	COM_TABLE_DUMP          = 0x13
	COM_CONNECT_OUT         = 0x14
	COM_REGISTER_SLAVE      = 0x15
	COM_STMT_PREPARE        = 0x16
	COM_STMT_EXECUTE        = 0x17
	COM_STMT_SEND_LONG_DATA = 0x18
	COM_STMT_CLOSE          = 0x19
	COM_STMT_RESET          = 0x1a
	COM_SET_OPTION          = 0x1b
	COM_STMT_FETCH          = 0x1c
	COM_DAEMON              = 0x1d
	COM_BINLOG_DUMP_GTID    = 0x1e
	COM_RESET_CONNECTION    = 0x1f
)

// Client capability flags (MySQL standard - 32 bits)
const (
	CLIENT_MYSQL                        = 1       // Is client mysql
	FOUND_ROWS                          = 2       // Use found rows instead of affected rows
	LONG_FLAG                           = 4       // Get all column flags
	CONNECT_WITH_DB                     = 8       // One can specify db on connect
	NO_SCHEMA                           = 16      // Don't allow database.table.column
	COMPRESS                            = 32      // Use compression protocol
	ODBC                                = 64      // ODBC client
	LOCAL_FILES                         = 128     // Can use LOAD DATA LOCAL
	IGNORE_SPACE                        = 256     // Ignore spaces before '('
	CLIENT_PROTOCOL_41                  = 512     // Use 4.1 protocol
	CLIENT_INTERACTIVE                  = 1024    // Is interactive client
	SSL                                 = 2048    // Switch to SSL after handshake
	IGNORE_SIGPIPE                      = 4096    // IGNORE sigpipes
	TRANSACTIONS                        = 8192    // Transactions
	RESERVED                            = 16384   // Reserved - not used
	SECURE_CONNECTION                   = 32768   // New 4.1 authentication
	MULTI_STATEMENTS                    = 1 << 16 // Enable/disable multi-stmt support
	MULTI_RESULTS                       = 1 << 17 // Enable/disable multi-results
	PS_MULTI_RESULTS                    = 1 << 18 // Enable/disable multi-results for PrepareStatement
	PLUGIN_AUTH                         = 1 << 19 // Client supports plugin authentication
	CONNECT_ATTRS                       = 1 << 20 // Client send connection attributes
	PLUGIN_AUTH_LENENC_CLIENT_DATA      = 1 << 21 // Authentication data length is a length auth integer
	CLIENT_CAN_HANDLE_EXPIRED_PASSWORDS = 1 << 22 // Client can handle expired passwords
	CLIENT_SESSION_TRACK                = 1 << 23 // Server send session tracking info
	CLIENT_DEPRECATE_EOF                = 1 << 24 // EOF packet deprecated
	PROGRESS_OLD                        = 1 << 29 // Client support progress indicator (before 10.2)
)

// MariaDB specific capabilities (extended - bits 32-37, use uint64)
const (
	PROGRESS             uint64 = 1 << 32 // Client progression
	MARIADB_RESERVED     uint64 = 1 << 33 // Not used anymore - reserved
	STMT_BULK_OPERATIONS uint64 = 1 << 34 // Permit COM_STMT_BULK commands
	EXTENDED_METADATA    uint64 = 1 << 35 // Metadata extended information
	CACHE_METADATA       uint64 = 1 << 36 // Permit metadata caching
	BULK_UNIT_RESULTS    uint64 = 1 << 37 // Permit returning all bulk individual results
)

// Server status flags
const (
	SERVER_STATUS_IN_TRANS             = 1 << 0
	SERVER_STATUS_AUTOCOMMIT           = 1 << 1
	SERVER_MORE_RESULTS_EXISTS         = 1 << 3
	SERVER_STATUS_NO_GOOD_INDEX_USED   = 1 << 4
	SERVER_STATUS_NO_INDEX_USED        = 1 << 5
	SERVER_STATUS_CURSOR_EXISTS        = 1 << 6
	SERVER_STATUS_LAST_ROW_SENT        = 1 << 7
	SERVER_STATUS_DB_DROPPED           = 1 << 8
	SERVER_STATUS_NO_BACKSLASH_ESCAPES = 1 << 9
	SERVER_STATUS_METADATA_CHANGED     = 1 << 10
	SERVER_QUERY_WAS_SLOW              = 1 << 11
	SERVER_PS_OUT_PARAMS               = 1 << 12
	SERVER_STATUS_IN_TRANS_READONLY    = 1 << 13
	SERVER_SESSION_STATE_CHANGED       = 1 << 14
)

// Column flags
const (
	NOT_NULL_FLAG         = 1 << 0
	PRI_KEY_FLAG          = 1 << 1
	UNIQUE_KEY_FLAG       = 1 << 2
	MULTIPLE_KEY_FLAG     = 1 << 3
	BLOB_FLAG             = 1 << 4
	UNSIGNED_FLAG         = 1 << 5
	ZEROFILL_FLAG         = 1 << 6
	BINARY_FLAG           = 1 << 7
	ENUM_FLAG             = 1 << 8
	AUTO_INCREMENT_FLAG   = 1 << 9
	TIMESTAMP_FLAG        = 1 << 10
	SET_FLAG              = 1 << 11
	NO_DEFAULT_VALUE_FLAG = 1 << 12
	ON_UPDATE_NOW_FLAG    = 1 << 13
	NUM_FLAG              = 1 << 15
)

// MySQL data types
const (
	MYSQL_TYPE_DECIMAL     = 0x00
	MYSQL_TYPE_TINY        = 0x01
	MYSQL_TYPE_SHORT       = 0x02
	MYSQL_TYPE_LONG        = 0x03
	MYSQL_TYPE_FLOAT       = 0x04
	MYSQL_TYPE_DOUBLE      = 0x05
	MYSQL_TYPE_NULL        = 0x06
	MYSQL_TYPE_TIMESTAMP   = 0x07
	MYSQL_TYPE_LONGLONG    = 0x08
	MYSQL_TYPE_INT24       = 0x09
	MYSQL_TYPE_DATE        = 0x0a
	MYSQL_TYPE_TIME        = 0x0b
	MYSQL_TYPE_DATETIME    = 0x0c
	MYSQL_TYPE_YEAR        = 0x0d
	MYSQL_TYPE_NEWDATE     = 0x0e
	MYSQL_TYPE_VARCHAR     = 0x0f
	MYSQL_TYPE_BIT         = 0x10
	MYSQL_TYPE_TIMESTAMP2  = 0x11
	MYSQL_TYPE_DATETIME2   = 0x12
	MYSQL_TYPE_TIME2       = 0x13
	MYSQL_TYPE_JSON        = 0xf5
	MYSQL_TYPE_NEWDECIMAL  = 0xf6
	MYSQL_TYPE_ENUM        = 0xf7
	MYSQL_TYPE_SET         = 0xf8
	MYSQL_TYPE_TINY_BLOB   = 0xf9
	MYSQL_TYPE_MEDIUM_BLOB = 0xfa
	MYSQL_TYPE_LONG_BLOB   = 0xfb
	MYSQL_TYPE_BLOB        = 0xfc
	MYSQL_TYPE_VAR_STRING  = 0xfd
	MYSQL_TYPE_STRING      = 0xfe
	MYSQL_TYPE_GEOMETRY    = 0xff
)

// Character sets
const (
	CHARSET_UTF8MB4 = 45
	CHARSET_UTF8    = 33
	CHARSET_BINARY  = 63
)

// Default client capabilities (uint64 to support MariaDB extended capabilities)
// Based on MariaDB Java connector ConnectionHelper.initializeBaseCapabilities()
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

// Compatibility aliases
const (
	CLIENT_LONG_PASSWORD                  = CLIENT_MYSQL
	CLIENT_FOUND_ROWS                     = FOUND_ROWS
	CLIENT_LONG_FLAG                      = LONG_FLAG
	CLIENT_CONNECT_WITH_DB                = CONNECT_WITH_DB
	CLIENT_NO_SCHEMA                      = NO_SCHEMA
	CLIENT_COMPRESS                       = COMPRESS
	CLIENT_ODBC                           = ODBC
	CLIENT_LOCAL_FILES                    = LOCAL_FILES
	CLIENT_IGNORE_SPACE                   = IGNORE_SPACE
	CLIENT_TRANSACTIONS                   = TRANSACTIONS
	CLIENT_RESERVED                       = RESERVED
	CLIENT_SECURE_CONNECTION              = SECURE_CONNECTION
	CLIENT_MULTI_STATEMENTS               = MULTI_STATEMENTS
	CLIENT_MULTI_RESULTS                  = MULTI_RESULTS
	CLIENT_PS_MULTI_RESULTS               = PS_MULTI_RESULTS
	CLIENT_PLUGIN_AUTH                    = PLUGIN_AUTH
	CLIENT_CONNECT_ATTRS                  = CONNECT_ATTRS
	CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA = PLUGIN_AUTH_LENENC_CLIENT_DATA
)
