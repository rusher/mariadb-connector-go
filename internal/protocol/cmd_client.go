// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
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

// NewQueryParam builds a COM_QUERY packet by substituting ? placeholders with
// formatted SQL literals, writing directly into buf (the writer's scratch buffer)
// to avoid intermediate string allocations.
//
// It performs a single pass over the query, eliminating both the pre-scan done by
// countPlaceholders and the string() conversion that a two-step approach requires.
// Mismatch between placeholder count and len(args) is detected at the end of the scan.
func NewQueryParam(buf []byte, query string, args []driver.NamedValue, noBackslashEscapes bool) ([]byte, error) {
	// Reuse buf up to the header+command prefix; grow if needed.
	if cap(buf) < hdrSize+1 {
		buf = make([]byte, 0, hdrSize+1+len(query)+len(args)*8)
	}
	buf = buf[:hdrSize+1]
	buf[hdrSize] = COM_QUERY

	argIdx := 0
	i := 0
	for i < len(query) {
		c := query[i]
		switch {
		case c == '?':
			if argIdx >= len(args) {
				return nil, fmt.Errorf("sql: expected %d arguments, got fewer", argIdx+1)
			}
			var err error
			buf, err = appendArgValue(buf, args[argIdx].Value, noBackslashEscapes)
			if err != nil {
				return nil, err
			}
			argIdx++
			i++

		case c == '\'' || c == '"':
			quote := c
			buf = append(buf, c)
			i++
			for i < len(query) {
				c = query[i]
				buf = append(buf, c)
				i++
				if c == '\\' && !noBackslashEscapes && i < len(query) {
					buf = append(buf, query[i])
					i++
				} else if c == quote {
					if i < len(query) && query[i] == quote {
						buf = append(buf, query[i])
						i++
					} else {
						break
					}
				}
			}

		case c == '`':
			buf = append(buf, c)
			i++
			for i < len(query) {
				c = query[i]
				buf = append(buf, c)
				i++
				if c == '`' {
					if i < len(query) && query[i] == '`' {
						buf = append(buf, query[i])
						i++
					} else {
						break
					}
				}
			}

		case c == '-' && i+1 < len(query) && query[i+1] == '-':
			buf = append(buf, query[i], query[i+1])
			i += 2
			for i < len(query) && query[i] != '\n' {
				buf = append(buf, query[i])
				i++
			}

		case c == '#':
			buf = append(buf, c)
			i++
			for i < len(query) && query[i] != '\n' {
				buf = append(buf, query[i])
				i++
			}

		case c == '/' && i+1 < len(query) && query[i+1] == '*':
			buf = append(buf, query[i], query[i+1])
			i += 2
			for i+1 < len(query) {
				if query[i] == '*' && query[i+1] == '/' {
					buf = append(buf, query[i], query[i+1])
					i += 2
					break
				}
				buf = append(buf, query[i])
				i++
			}

		default:
			buf = append(buf, c)
			i++
		}
	}

	if argIdx != len(args) {
		return nil, fmt.Errorf("sql: expected %d arguments, got %d", argIdx, len(args))
	}
	return buf, nil
}

// appendArgValue appends the SQL literal representation of v to buf.
func appendArgValue(buf []byte, v driver.Value, noBackslashEscapes bool) ([]byte, error) {
	switch val := v.(type) {
	case nil:
		return append(buf, "NULL"...), nil
	case bool:
		if val {
			return append(buf, '1'), nil
		}
		return append(buf, '0'), nil
	case int64:
		return strconv.AppendInt(buf, val, 10), nil
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return nil, driver.ErrSkip
		}
		return strconv.AppendFloat(buf, val, 'g', -1, 64), nil
	case string:
		return appendQuotedString(buf, val, noBackslashEscapes), nil
	case []byte:
		if val == nil {
			return append(buf, "NULL"...), nil
		}
		buf = append(buf, "_binary'"...)
		buf = appendEscapedBytes(buf, val, noBackslashEscapes)
		return append(buf, '\''), nil
	case time.Time:
		if val.IsZero() {
			return append(buf, "'0000-00-00'"...), nil
		}
		buf = val.AppendFormat(append(buf, '\''), "2006-01-02 15:04:05.999999")
		return append(buf, '\''), nil
	}
	return nil, driver.ErrSkip
}

// appendQuotedString appends a single-quoted, escaped SQL string to buf.
func appendQuotedString(buf []byte, s string, noBackslashEscapes bool) []byte {
	buf = append(buf, '\'')
	if noBackslashEscapes {
		for i := 0; i < len(s); i++ {
			if s[i] == '\'' {
				buf = append(buf, '\'', '\'')
			} else {
				buf = append(buf, s[i])
			}
		}
	} else {
		for i := 0; i < len(s); i++ {
			switch s[i] {
			case 0:
				buf = append(buf, '\\', '0')
			case '\n':
				buf = append(buf, '\\', 'n')
			case '\r':
				buf = append(buf, '\\', 'r')
			case '\x1a':
				buf = append(buf, '\\', 'Z')
			case '\'':
				buf = append(buf, '\\', '\'')
			case '"':
				buf = append(buf, '\\', '"')
			case '\\':
				buf = append(buf, '\\', '\\')
			default:
				buf = append(buf, s[i])
			}
		}
	}
	return append(buf, '\'')
}

// appendEscapedBytes escapes raw bytes for use inside a quoted SQL string.
func appendEscapedBytes(buf, v []byte, noBackslashEscapes bool) []byte {
	if noBackslashEscapes {
		for _, c := range v {
			if c == '\'' {
				buf = append(buf, '\'', '\'')
			} else {
				buf = append(buf, c)
			}
		}
		return buf
	}
	for _, c := range v {
		switch c {
		case 0:
			buf = append(buf, '\\', '0')
		case '\n':
			buf = append(buf, '\\', 'n')
		case '\r':
			buf = append(buf, '\\', 'r')
		case '\x1a':
			buf = append(buf, '\\', 'Z')
		case '\'':
			buf = append(buf, '\\', '\'')
		case '"':
			buf = append(buf, '\\', '"')
		case '\\':
			buf = append(buf, '\\', '\\')
		default:
			buf = append(buf, c)
		}
	}
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

// NewExecute returns a COM_STMT_EXECUTE packet written into buf.
// stmtID 0xFFFFFFFF is the sentinel for pipelined prepare+execute.
func NewExecute(buf []byte, stmtID uint32, args []driver.NamedValue) ([]byte, error) {
	buf = buf[:hdrSize]
	buf = append(buf, COM_STMT_EXECUTE)

	// Statement ID (4 bytes, little-endian)
	buf = append(buf,
		byte(stmtID),
		byte(stmtID>>8),
		byte(stmtID>>16),
		byte(stmtID>>24),
	)

	buf = append(buf, 0x00)       // flags: CURSOR_TYPE_NO_CURSOR
	buf = append(buf, 1, 0, 0, 0) // iteration-count: always 1

	if len(args) > 0 {
		// NULL bitmap
		nullBitmapLen := (len(args) + 7) / 8
		nullBitmap := make([]byte, nullBitmapLen)
		for i, nv := range args {
			if cmdIsNull(nv.Value) {
				nullBitmap[i/8] |= 1 << (i % 8)
			}
		}
		buf = append(buf, nullBitmap...)

		buf = append(buf, 0x01) // new-params-bound flag

		for _, nv := range args {
			ft, flags := cmdParamType(nv.Value)
			buf = append(buf, ft, flags)
		}

		for _, nv := range args {
			if cmdIsNull(nv.Value) {
				continue
			}
			var err error
			buf, err = AppendParamValue(buf, nv.Value)
			if err != nil {
				return nil, err
			}
		}
	}

	return buf, nil
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
	case bool:
		return MYSQL_TYPE_TINY, 0
	case int8:
		return MYSQL_TYPE_TINY, 0
	case int16:
		return MYSQL_TYPE_SHORT, 0
	case int32:
		return MYSQL_TYPE_LONG, 0
	case int, int64:
		return MYSQL_TYPE_LONGLONG, 0
	case uint8:
		return MYSQL_TYPE_TINY, 0x80
	case uint16:
		return MYSQL_TYPE_SHORT, 0x80
	case uint32:
		return MYSQL_TYPE_LONG, 0x80
	case uint, uint64:
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
