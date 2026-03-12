// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"database/sql/driver"
	"encoding/binary"
	"reflect"
	"time"
)

// NewQuery returns a COM_QUERY packet for the given SQL string.
// buf is a caller-supplied scratch buffer (e.g. PacketWriter.Buf()): reused
// in-place when its capacity is sufficient, otherwise a fresh slice is allocated.
func NewQuery(buf []byte, query string) []byte {
	need := hdrSize + 1 + len(query)
	if cap(buf) >= need {
		buf = buf[:need]
	} else {
		buf = make([]byte, need)
	}
	buf[hdrSize] = COM_QUERY
	copy(buf[hdrSize+1:], query)
	return buf
}

// NewPrepare returns a COM_STMT_PREPARE packet.
// buf is a caller-supplied scratch buffer (e.g. PacketWriter.Buf()): reused
// in-place when its capacity is sufficient, otherwise a fresh slice is allocated.
func NewPrepare(buf []byte, query string) []byte {
	need := hdrSize + 1 + len(query)
	if cap(buf) >= need {
		buf = buf[:need]
	} else {
		buf = make([]byte, need)
	}
	buf[hdrSize] = COM_STMT_PREPARE
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

	buf = append(buf, COM_STMT_EXECUTE)

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
			if cmdIsNull(arg) {
				nullBitmap[i/8] |= 1 << (i % 8)
			}
		}
		buf = append(buf, nullBitmap...)

		buf = append(buf, 0x01) // new-params-bound flag

		for _, arg := range argSlice {
			ft, flags := cmdParamType(arg)
			buf = append(buf, ft, flags)
		}

		for _, arg := range argSlice {
			if cmdIsNull(arg) {
				continue
			}
			enc, err := EncodeParamValue(arg)
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
func NewStmtClose(buf []byte, stmtID uint32) []byte {
	const need = hdrSize + 5
	if cap(buf) >= need {
		buf = buf[:need]
	} else {
		buf = make([]byte, need)
	}
	buf[hdrSize] = COM_STMT_CLOSE
	binary.LittleEndian.PutUint32(buf[hdrSize+1:], stmtID)
	return buf
}

// NewPing returns a COM_PING packet.
func NewPing(buf []byte) []byte {
	const need = hdrSize + 1
	if cap(buf) >= need {
		buf = buf[:need]
	} else {
		buf = make([]byte, need)
	}
	buf[hdrSize] = COM_PING
	return buf
}

// NewQuit returns a COM_QUIT packet.
func NewQuit(buf []byte) []byte {
	const need = hdrSize + 1
	if cap(buf) >= need {
		buf = buf[:need]
	} else {
		buf = make([]byte, need)
	}
	buf[hdrSize] = COM_QUIT
	return buf
}

// NewResetConnection returns a COM_RESET_CONNECTION packet.
func NewResetConnection(buf []byte) []byte {
	const need = hdrSize + 1
	if cap(buf) >= need {
		buf = buf[:need]
	} else {
		buf = make([]byte, need)
	}
	buf[hdrSize] = COM_RESET_CONNECTION
	return buf
}

// ── helpers ──────────────────────────────────────────────────────────────────

func cmdIsNull(v interface{}) bool {
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

func cmdParamType(arg interface{}) (byte, byte) {
	if arg == nil {
		return MYSQL_TYPE_NULL, 0
	}
	switch arg.(type) {
	case int, int8, int16, int32, int64:
		return MYSQL_TYPE_LONGLONG, 0
	case uint, uint8, uint16, uint32, uint64:
		return MYSQL_TYPE_LONGLONG, 0x80
	case float32:
		return MYSQL_TYPE_FLOAT, 0
	case float64:
		return MYSQL_TYPE_DOUBLE, 0
	case string:
		return MYSQL_TYPE_VAR_STRING, 0
	case []byte:
		return MYSQL_TYPE_BLOB, 0
	case time.Time:
		return MYSQL_TYPE_DATETIME, 0
	default:
		return MYSQL_TYPE_VAR_STRING, 0
	}
}
