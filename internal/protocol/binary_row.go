// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

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

	values := make([]interface{}, numColumns)

	for i := range columns {
		bytePos := (i + 2) / 8
		bitPos := (i + 2) % 8
		if nullBitmap[bytePos]&(1<<bitPos) != 0 {
			values[i] = nil
			continue
		}

		var err error
		values[i], pos, err = readBinaryValue(data, pos, &columns[i])
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}
	}

	return values, nil
}

// readBinaryValue reads a single typed value from a binary protocol row packet.
func readBinaryValue(data []byte, pos int, col *ColumnDefinition) (interface{}, int, error) {
	switch col.Type {
	case MYSQL_TYPE_TINY:
		val := data[pos]
		// TINYINT(1) is treated as bool
		if col.Length == 1 {
			return val != 0, pos + 1, nil
		}
		if col.Flags&UNSIGNED_FLAG != 0 {
			return uint64(val), pos + 1, nil
		}
		return int64(int8(val)), pos + 1, nil

	case MYSQL_TYPE_SHORT, MYSQL_TYPE_YEAR:
		_ = data[pos+1]
		val := binary.LittleEndian.Uint16(data[pos:])
		if col.Flags&UNSIGNED_FLAG != 0 {
			return uint64(val), pos + 2, nil
		}
		return int64(int16(val)), pos + 2, nil

	case MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
		_ = data[pos+3]
		val := binary.LittleEndian.Uint32(data[pos:])
		if col.Flags&UNSIGNED_FLAG != 0 {
			return uint64(val), pos + 4, nil
		}
		return int64(int32(val)), pos + 4, nil

	case MYSQL_TYPE_LONGLONG:
		_ = data[pos+7]
		val := binary.LittleEndian.Uint64(data[pos:])
		if col.Flags&UNSIGNED_FLAG != 0 {
			return val, pos + 8, nil
		}
		return int64(val), pos + 8, nil

	case MYSQL_TYPE_FLOAT:
		_ = data[pos+3]
		bits := binary.LittleEndian.Uint32(data[pos:])
		return float64(math.Float32frombits(bits)), pos + 4, nil

	case MYSQL_TYPE_DOUBLE:
		_ = data[pos+7]
		bits := binary.LittleEndian.Uint64(data[pos:])
		return math.Float64frombits(bits), pos + 8, nil

	case MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
		// DATE/DATETIME/TIMESTAMP are length-encoded in binary protocol
		// Length byte followed by date/time components
		length := int(data[pos])
		pos++

		if length == 0 {
			// Zero date: 0000-00-00 00:00:00
			return time.Time{}, pos, nil
		}

		// Length 4: year(2) month(1) day(1)
		// Length 7: year(2) month(1) day(1) hour(1) minute(1) second(1)
		// Length 11: year(2) month(1) day(1) hour(1) minute(1) second(1) microsecond(4)
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

		t := time.Date(year, time.Month(month), day, hour, minute, second, microsecond*1000, time.UTC)
		return t, pos + length, nil

	case MYSQL_TYPE_TIME:
		// TIME is length-encoded in binary protocol
		// Length 8: is_negative(1) days(4) hours(1) minutes(1) seconds(1)
		// Length 12: is_negative(1) days(4) hours(1) minutes(1) seconds(1) microseconds(4)
		length := int(data[pos])
		pos++

		if length == 0 {
			return time.Duration(0), pos, nil
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

		return duration, pos + length, nil

	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING,
		MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL,
		MYSQL_TYPE_BLOB, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB,
		MYSQL_TYPE_BIT, MYSQL_TYPE_ENUM, MYSQL_TYPE_SET, MYSQL_TYPE_GEOMETRY, MYSQL_TYPE_JSON:
		vLen, vStart, err := ReadLengthEncodedInteger(data, pos)
		if err != nil {
			return nil, pos, err
		}
		vEnd := vStart + int(vLen)
		// binary charset (63) → []byte; JSON is always text even with binary charset
		if col.Charset == 63 && col.Type != MYSQL_TYPE_JSON {
			return data[vStart:vEnd], vEnd, nil
		}
		return string(data[vStart:vEnd]), vEnd, nil

	default:
		return nil, pos, fmt.Errorf("unsupported field type: %d", col.Type)
	}
}

// EncodeParamValue encodes a parameter value for a binary protocol packet.
func EncodeParamValue(arg interface{}) ([]byte, error) {
	switch v := arg.(type) {
	case string:
		return WriteLengthEncodedString(nil, v), nil
	case []byte:
		buf := WriteLengthEncodedInteger(nil, uint64(len(v)))
		return append(buf, v...), nil
	case int64:
		buf := make([]byte, 8)
		binary.LittleEndian.PutUint64(buf, uint64(v))
		return buf, nil
	case float64:
		// For DECIMAL/DOUBLE types, encode as 8-byte IEEE 754 double
		buf := make([]byte, 8)
		bits := math.Float64bits(v)
		binary.LittleEndian.PutUint64(buf, bits)
		return buf, nil
	case float32:
		// For FLOAT types, encode as 4-byte IEEE 754 float
		buf := make([]byte, 4)
		bits := math.Float32bits(v)
		binary.LittleEndian.PutUint32(buf, bits)
		return buf, nil
	case time.Time:
		if v.IsZero() {
			return []byte{0}, nil
		}
		year, month, day := v.Date()
		hour, min, sec := v.Clock()
		micro := v.Nanosecond() / 1000
		if micro != 0 {
			buf := make([]byte, 12)
			buf[0] = 11
			binary.LittleEndian.PutUint16(buf[1:], uint16(year))
			buf[3] = byte(month)
			buf[4] = byte(day)
			buf[5] = byte(hour)
			buf[6] = byte(min)
			buf[7] = byte(sec)
			binary.LittleEndian.PutUint32(buf[8:], uint32(micro))
			return buf, nil
		}
		if hour != 0 || min != 0 || sec != 0 {
			buf := make([]byte, 8)
			buf[0] = 7
			binary.LittleEndian.PutUint16(buf[1:], uint16(year))
			buf[3] = byte(month)
			buf[4] = byte(day)
			buf[5] = byte(hour)
			buf[6] = byte(min)
			buf[7] = byte(sec)
			return buf, nil
		}
		buf := make([]byte, 5)
		buf[0] = 4
		binary.LittleEndian.PutUint16(buf[1:], uint16(year))
		buf[3] = byte(month)
		buf[4] = byte(day)
		return buf, nil
	default:
		// Convert to string as fallback
		str := fmt.Sprintf("%v", v)
		return WriteLengthEncodedString(nil, str), nil
	}
}
