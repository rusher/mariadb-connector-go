// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"database/sql/driver"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

// ParseBinaryRowDirect parses a binary-protocol row directly into dest, avoiding the
// intermediate []interface{} allocation that ParseBinaryRow requires.
func ParseBinaryRowDirect(data []byte, columns []ColumnDefinition, dest []driver.Value) error {
	if data[0] != 0x00 {
		return fmt.Errorf("invalid binary row packet")
	}
	pos := 1
	numColumns := len(columns)
	nullBitmapLen := (numColumns + 7 + 2) / 8
	_ = data[pos+nullBitmapLen-1]
	nullBitmap := data[pos : pos+nullBitmapLen]
	pos += nullBitmapLen

	for i := range columns {
		bytePos := (i + 2) / 8
		bitPos := (i + 2) % 8
		if nullBitmap[bytePos]&(1<<bitPos) != 0 {
			dest[i] = nil
			continue
		}
		var err error
		pos, err = readBinaryValue(data, pos, &columns[i], &dest[i])
		if err != nil {
			return fmt.Errorf("failed to read column %d: %w", i, err)
		}
	}
	return nil
}

// ParseBinaryRow parses a row in binary protocol (prepared statement)
func ParseBinaryRow(data []byte, columns []ColumnDefinition) ([]interface{}, error) {
	if data[0] != 0x00 {
		return nil, fmt.Errorf("invalid binary row packet")
	}

	pos := 1
	numColumns := len(columns)

	nullBitmapLen := (numColumns + 7 + 2) / 8
	_ = data[pos+nullBitmapLen-1]
	nullBitmap := data[pos : pos+nullBitmapLen]
	pos += nullBitmapLen

	values := make([]driver.Value, numColumns)

	for i := range columns {
		bytePos := (i + 2) / 8
		bitPos := (i + 2) % 8
		if nullBitmap[bytePos]&(1<<bitPos) != 0 {
			values[i] = nil
			continue
		}

		var err error
		pos, err = readBinaryValue(data, pos, &columns[i], &values[i])
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}
	}

	result := make([]interface{}, numColumns)
	for i, v := range values {
		result[i] = v
	}
	return result, nil
}

// readBinaryValue reads a single typed value from a binary protocol row packet
// and writes it directly into *out, avoiding interface{} boxing on the return path.
func readBinaryValue(data []byte, pos int, col *ColumnDefinition, out *driver.Value) (int, error) {
	switch col.Type {
	case MYSQL_TYPE_TINY:
		val := data[pos]
		// TINYINT(1) is treated as bool
		if col.Length == 1 {
			*out = val != 0
			return pos + 1, nil
		}
		if col.Flags&UNSIGNED_FLAG != 0 {
			*out = uint64(val)
		} else {
			*out = int64(int8(val))
		}
		return pos + 1, nil

	case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
		_ = data[pos+1]
		val := binary.LittleEndian.Uint16(data[pos:])
		if col.Flags&UNSIGNED_FLAG != 0 {
			*out = uint64(val)
		} else {
			*out = int64(int16(val))
		}
		return pos + 2, nil

	case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
		_ = data[pos+3]
		val := binary.LittleEndian.Uint32(data[pos:])
		if col.Flags&UNSIGNED_FLAG != 0 {
			*out = uint64(val)
		} else {
			*out = int64(int32(val))
		}
		return pos + 4, nil

	case MYSQL_TYPE_LONGLONG:
		_ = data[pos+7]
		val := binary.LittleEndian.Uint64(data[pos:])
		if col.Flags&UNSIGNED_FLAG != 0 {
			*out = val
		} else {
			*out = int64(val)
		}
		return pos + 8, nil

	case MYSQL_TYPE_FLOAT:
		_ = data[pos+3]
		bits := binary.LittleEndian.Uint32(data[pos:])
		*out = float64(math.Float32frombits(bits))
		return pos + 4, nil

	case MYSQL_TYPE_DOUBLE:
		_ = data[pos+7]
		bits := binary.LittleEndian.Uint64(data[pos:])
		*out = math.Float64frombits(bits)
		return pos + 8, nil

	case MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
		length := int(data[pos])
		pos++

		if length == 0 {
			*out = time.Time{}
			return pos, nil
		}

		_ = data[pos+length-1]

		year := int(binary.LittleEndian.Uint16(data[pos:]))
		month := int(data[pos+2])
		day := int(data[pos+3])
		hour, minute, second, microsecond := 0, 0, 0, 0

		if length >= 7 {
			hour = int(data[pos+4])
			minute = int(data[pos+5])
			second = int(data[pos+6])
		}
		if length == 11 {
			microsecond = int(binary.LittleEndian.Uint32(data[pos+7:]))
		}

		*out = time.Date(year, time.Month(month), day, hour, minute, second, microsecond*1000, time.UTC)
		return pos + length, nil

	case MYSQL_TYPE_TIME:
		length := int(data[pos])
		pos++

		if length == 0 {
			*out = time.Duration(0)
			return pos, nil
		}

		_ = data[pos+length-1]

		isNegative := data[pos] == 1
		days := int64(binary.LittleEndian.Uint32(data[pos+1:]))
		hours := int64(data[pos+5])
		minutes := int64(data[pos+6])
		seconds := int64(data[pos+7])
		microseconds := int64(0)

		if length == 12 {
			microseconds = int64(binary.LittleEndian.Uint32(data[pos+8:]))
		}

		duration := time.Duration(days*24*3600+hours*3600+minutes*60+seconds)*time.Second +
			time.Duration(microseconds)*time.Microsecond
		if isNegative {
			duration = -duration
		}
		*out = duration
		return pos + length, nil

	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING,
		MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_BLOB, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB,
		MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_JSON:
		vLen, vStart := ReadLengthEncodedInteger(data, pos)
		vEnd := vStart + int(vLen)
		// binary charset (63) → []byte; JSON is always text even with binary charset
		if col.Charset == 63 && col.Type != MYSQL_TYPE_JSON {
			*out = data[vStart:vEnd]
		} else {
			*out = string(data[vStart:vEnd])
		}
		return vEnd, nil

	default:
		return pos, fmt.Errorf("unsupported field type: %d", col.Type)
	}
}

// AppendParamValue appends the binary-protocol encoding of arg to buf and
// returns the extended slice. No intermediate allocation is made for fixed-size
// types (int64, float64, bool, time.Time, …) — the value is written directly
// into whatever capacity buf already has. This is the preferred hot-path
// called by NewExecute; callers should pre-allocate buf with enough capacity.
func AppendParamValue(buf []byte, arg interface{}) ([]byte, error) {
	switch v := arg.(type) {
	case bool:
		if v {
			return append(buf, 1), nil
		}
		return append(buf, 0), nil
	case int8:
		return append(buf, byte(v)), nil
	case int16:
		return binary.LittleEndian.AppendUint16(buf, uint16(v)), nil
	case int32:
		return binary.LittleEndian.AppendUint32(buf, uint32(v)), nil
	case int64:
		return binary.LittleEndian.AppendUint64(buf, uint64(v)), nil
	case uint8:
		return append(buf, v), nil
	case uint16:
		return binary.LittleEndian.AppendUint16(buf, v), nil
	case uint32:
		return binary.LittleEndian.AppendUint32(buf, v), nil
	case uint64:
		return binary.LittleEndian.AppendUint64(buf, v), nil
	case float32:
		return binary.LittleEndian.AppendUint32(buf, math.Float32bits(v)), nil
	case float64:
		return binary.LittleEndian.AppendUint64(buf, math.Float64bits(v)), nil
	case string:
		return WriteLengthEncodedString(buf, v), nil
	case []byte:
		buf = WriteLengthEncodedInteger(buf, uint64(len(v)))
		return append(buf, v...), nil
	case time.Time:
		if v.IsZero() {
			return append(buf, 0), nil
		}
		year, month, day := v.Date()
		hour, min, sec := v.Clock()
		micro := v.Nanosecond() / 1000
		if micro != 0 {
			buf = append(buf, 11,
				byte(year), byte(year>>8),
				byte(month), byte(day),
				byte(hour), byte(min), byte(sec),
				0, 0, 0, 0,
			)
			binary.LittleEndian.PutUint32(buf[len(buf)-4:], uint32(micro))
			return buf, nil
		}
		if hour != 0 || min != 0 || sec != 0 {
			return append(buf, 7,
				byte(year), byte(year>>8),
				byte(month), byte(day),
				byte(hour), byte(min), byte(sec),
			), nil
		}
		return append(buf, 4,
			byte(year), byte(year>>8),
			byte(month), byte(day),
		), nil
	default:
		str := fmt.Sprintf("%v", v)
		return WriteLengthEncodedString(buf, str), nil
	}
}

// EncodeParamValue encodes a parameter value into a fresh []byte.
// Prefer AppendParamValue for hot-paths to avoid the extra allocation.
func EncodeParamValue(arg interface{}) ([]byte, error) {
	return AppendParamValue(nil, arg)
}
