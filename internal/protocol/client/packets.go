// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

// Package clientpkt provides constructors for MySQL/MariaDB client-to-server packets.
// Every function returns a []byte whose first 4 bytes are reserved for the packet
// header (3-byte length + 1-byte sequence number).  The caller passes the slice
// directly to PacketWriter.Write, which fills the header in-place and issues a
// single Write syscall — no extra allocation, no extra copy.
package clientpkt

import (
	"database/sql/driver"
	"reflect"
	"time"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

const hdrSize = 4 // bytes reserved for packet header

// NewQuery returns a COM_QUERY packet for the given SQL string.
func NewQuery(query string) []byte {
	buf := make([]byte, hdrSize+1+len(query))
	buf[hdrSize] = protocol.COM_QUERY
	copy(buf[hdrSize+1:], query)
	return buf
}

// NewPrepare returns a COM_STMT_PREPARE packet.
func NewPrepare(query string) []byte {
	buf := make([]byte, hdrSize+1+len(query))
	buf[hdrSize] = protocol.COM_STMT_PREPARE
	copy(buf[hdrSize+1:], query)
	return buf
}

// NewExecute returns a COM_STMT_EXECUTE packet.
// stmtID 0xFFFFFFFF is the sentinel for pipelined prepare+execute.
func NewExecute(stmtID uint32, args []driver.NamedValue) ([]byte, error) {
	argSlice := make([]interface{}, len(args))
	for i, nv := range args {
		argSlice[i] = nv.Value
	}

	buf := make([]byte, 0, hdrSize+10+len(argSlice)*10)
	buf = append(buf, 0, 0, 0, 0) // header reservation

	buf = append(buf, protocol.COM_STMT_EXECUTE)

	// Statement ID (4 bytes)
	buf = append(buf,
		byte(stmtID),
		byte(stmtID>>8),
		byte(stmtID>>16),
		byte(stmtID>>24),
	)

	buf = append(buf, 0x00)       // flags: CURSOR_TYPE_NO_CURSOR
	buf = append(buf, 1, 0, 0, 0) // iteration-count: always 1

	if len(argSlice) > 0 {
		// NULL bitmap
		nullBitmapLen := (len(argSlice) + 7) / 8
		nullBitmap := make([]byte, nullBitmapLen)
		for i, arg := range argSlice {
			if isNull(arg) {
				nullBitmap[i/8] |= 1 << (i % 8)
			}
		}
		buf = append(buf, nullBitmap...)

		buf = append(buf, 0x01) // new-params-bound flag

		for _, arg := range argSlice {
			ft, flags := paramType(arg)
			buf = append(buf, ft, flags)
		}

		for _, arg := range argSlice {
			if isNull(arg) {
				continue
			}
			enc, err := encodeParam(arg)
			if err != nil {
				return nil, err
			}
			buf = append(buf, enc...)
		}
	}

	return buf, nil
}

// SetStmtID overwrites the statement ID field in a packet returned by NewExecute.
// The statement ID occupies bytes [hdrSize+1 : hdrSize+5] (after the command byte).
func SetStmtID(buf []byte, stmtID uint32) {
	buf[hdrSize+1] = byte(stmtID)
	buf[hdrSize+2] = byte(stmtID >> 8)
	buf[hdrSize+3] = byte(stmtID >> 16)
	buf[hdrSize+4] = byte(stmtID >> 24)
}

// NewStmtClose returns a COM_STMT_CLOSE packet.
func NewStmtClose(stmtID uint32) []byte {
	buf := make([]byte, hdrSize+5)
	buf[hdrSize] = protocol.COM_STMT_CLOSE
	protocol.PutUint32(buf[hdrSize+1:], stmtID)
	return buf
}

// NewPing returns a COM_PING packet.
func NewPing() []byte {
	buf := make([]byte, hdrSize+1)
	buf[hdrSize] = protocol.COM_PING
	return buf
}

// NewQuit returns a COM_QUIT packet.
func NewQuit() []byte {
	buf := make([]byte, hdrSize+1)
	buf[hdrSize] = protocol.COM_QUIT
	return buf
}

// NewResetConnection returns a COM_RESET_CONNECTION packet.
func NewResetConnection() []byte {
	buf := make([]byte, hdrSize+1)
	buf[hdrSize] = protocol.COM_RESET_CONNECTION
	return buf
}

// ── helpers ──────────────────────────────────────────────────────────────────

func isNull(v interface{}) bool {
	if v == nil {
		return true
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Ptr, reflect.Slice, reflect.Map, reflect.Chan, reflect.Func, reflect.Interface:
		return rv.IsNil()
	}
	return false
}

func paramType(arg interface{}) (byte, byte) {
	if arg == nil {
		return protocol.MYSQL_TYPE_NULL, 0
	}
	switch arg.(type) {
	case int, int8, int16, int32, int64:
		return protocol.MYSQL_TYPE_LONGLONG, 0
	case uint, uint8, uint16, uint32, uint64:
		return protocol.MYSQL_TYPE_LONGLONG, 0x80
	case float32:
		return protocol.MYSQL_TYPE_FLOAT, 0
	case float64:
		return protocol.MYSQL_TYPE_DOUBLE, 0
	case string:
		return protocol.MYSQL_TYPE_VAR_STRING, 0
	case []byte:
		return protocol.MYSQL_TYPE_BLOB, 0
	case time.Time:
		return protocol.MYSQL_TYPE_DATETIME, 0
	default:
		return protocol.MYSQL_TYPE_VAR_STRING, 0
	}
}

func encodeParam(arg interface{}) ([]byte, error) {
	// delegate to the existing encoder in protocol
	return protocol.EncodeParamValue(arg)
}
