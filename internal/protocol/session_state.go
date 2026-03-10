// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

// Session state change types
// See https://mariadb.com/kb/en/ok_packet/#session-state-information
const (
	SESSION_TRACK_SYSTEM_VARIABLES            byte = 0x00
	SESSION_TRACK_SCHEMA                      byte = 0x01
	SESSION_TRACK_STATE_CHANGE                byte = 0x02
	SESSION_TRACK_GTIDS                       byte = 0x03
	SESSION_TRACK_TRANSACTION_CHARACTERISTICS byte = 0x04
	SESSION_TRACK_TRANSACTION_STATE           byte = 0x05
)
