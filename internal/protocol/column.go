// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"time"
	"unsafe"
)

// ColumnDefinition represents a column definition packet.
// Hot-path numeric fields are placed first so they occupy the first cache line
// during row parsing (ParseTextRow / ParseBinaryRow never touch Name).
// Name is a zero-copy alias into raw packet bytes (no string allocation per column).
type ColumnDefinition struct {
	// Hot-path: accessed on every row — kept first for cache locality
	Type     byte
	Decimals byte
	Flags    uint16
	Charset  uint16
	Length   uint32
	// Cold-path: name decoded lazily / zero-copy (only Columns() call)
	raw  []byte // keeps packet backing array alive so Name remains valid
	Name string // zero-copy via unsafe.String — no allocation
	// Extended metadata (rare: MARIADB_CLIENT_EXTENDED_TYPE_INFO)
	ExtendedType string
	Format       string
}

// skipLengthEncoded advances pos past a length-encoded string without allocating.
func skipLengthEncoded(data []byte, pos int) (int, error) {
	n, newPos, err := ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return pos, err
	}
	end := newPos + int(n)
	if end > len(data) {
		return pos, fmt.Errorf("column definition: field exceeds packet boundary")
	}
	return end, nil
}

// FillColumnDefinition parses a column definition packet into a pre-allocated col.
// Catalog, Schema, Table, OrgTable and OrgName are skipped to avoid allocations.
// Name is decoded zero-copy: it aliases the packet bytes via unsafe.String so no
// string allocation occurs. col.raw keeps the packet backing array alive.
func FillColumnDefinition(data []byte, col *ColumnDefinition) error {
	pos := 0

	var err error
	// Skip: catalog, schema, table, org_table (4 length-encoded strings)
	for range 4 {
		if pos, err = skipLengthEncoded(data, pos); err != nil {
			return fmt.Errorf("failed to skip column field: %w", err)
		}
	}

	// Name: zero-copy alias into the raw packet bytes — no string allocation.
	nameLen, nameStart, err := ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return fmt.Errorf("failed to read name length: %w", err)
	}
	nameEnd := nameStart + int(nameLen)

	col.raw = data // keep backing array alive as long as ColumnDefinition is live
	//nolint:unsafeptr
	col.Name = unsafe.String(unsafe.SliceData(data[nameStart:nameEnd]), int(nameLen))
	pos = nameEnd

	// Skip: org_name
	if pos, err = skipLengthEncoded(data, pos); err != nil {
		return fmt.Errorf("failed to skip org_name: %w", err)
	}

	// Length of fixed-length fields (1 byte)
	fixedLen := data[pos]
	pos++

	_ = data[pos+int(fixedLen)-1]

	// Character set (2 bytes)
	col.Charset = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	// Column length (4 bytes)
	col.Length = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	// Column type (1 byte)
	col.Type = data[pos]
	pos++

	// Flags (2 bytes)
	col.Flags = binary.LittleEndian.Uint16(data[pos:])
	pos += 2

	// Decimals (1 byte)
	col.Decimals = data[pos]
	pos++

	if pos < len(data) {
		extType, newPos, err2 := ReadLengthEncodedString(data, pos)
		if err2 == nil && extType != "" {
			col.ExtendedType = extType
			pos = newPos
		}
		if pos < len(data) {
			format, _, err2 := ReadLengthEncodedString(data, pos)
			if err2 == nil && format != "" {
				col.Format = format
			}
		}
	}

	return nil
}

// ParseColumnDefinition parses a column definition packet into a new ColumnDefinition.
func ParseColumnDefinition(data []byte) (*ColumnDefinition, error) {
	col := &ColumnDefinition{}
	if err := FillColumnDefinition(data, col); err != nil {
		return nil, err
	}
	return col, nil
}

// TypeToString converts a MySQL type to a string
func TypeToString(t byte) string {
	switch t {
	case MYSQL_TYPE_DECIMAL:
		return "DECIMAL"
	case MYSQL_TYPE_TINY:
		return "TINYINT"
	case MYSQL_TYPE_SHORT:
		return "SMALLINT"
	case MYSQL_TYPE_LONG:
		return "INT"
	case MYSQL_TYPE_FLOAT:
		return "FLOAT"
	case MYSQL_TYPE_DOUBLE:
		return "DOUBLE"
	case MYSQL_TYPE_NULL:
		return "NULL"
	case MYSQL_TYPE_TIMESTAMP:
		return "TIMESTAMP"
	case MYSQL_TYPE_LONGLONG:
		return "BIGINT"
	case MYSQL_TYPE_INT24:
		return "MEDIUMINT"
	case MYSQL_TYPE_DATE:
		return "DATE"
	case MYSQL_TYPE_TIME:
		return "TIME"
	case MYSQL_TYPE_DATETIME:
		return "DATETIME"
	case MYSQL_TYPE_YEAR:
		return "YEAR"
	case MYSQL_TYPE_VARCHAR:
		return "VARCHAR"
	case MYSQL_TYPE_BIT:
		return "BIT"
	case MYSQL_TYPE_JSON:
		return "JSON"
	case MYSQL_TYPE_NEWDECIMAL:
		return "DECIMAL"
	case MYSQL_TYPE_ENUM:
		return "ENUM"
	case MYSQL_TYPE_SET:
		return "SET"
	case MYSQL_TYPE_TINY_BLOB:
		return "TINYBLOB"
	case MYSQL_TYPE_MEDIUM_BLOB:
		return "MEDIUMBLOB"
	case MYSQL_TYPE_LONG_BLOB:
		return "LONGBLOB"
	case MYSQL_TYPE_BLOB:
		return "BLOB"
	case MYSQL_TYPE_VAR_STRING:
		return "VARCHAR"
	case MYSQL_TYPE_STRING:
		return "CHAR"
	case MYSQL_TYPE_GEOMETRY:
		return "GEOMETRY"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", t)
	}
}

// TypeToScanTypeWithColumn returns the Go type that can be used to scan this column
// This version takes the full column definition to handle TINYINT(1) as bool
func TypeToScanTypeWithColumn(col *ColumnDefinition) reflect.Type {
	switch col.Type {
	case MYSQL_TYPE_TINY:
		if col.Length == 1 {
			return reflect.TypeOf(false)
		}
		return reflect.TypeOf(int64(0))
	case MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_INT24:
		return reflect.TypeOf(int64(0))
	case MYSQL_TYPE_LONGLONG:
		return reflect.TypeOf(int64(0))
	case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
		return reflect.TypeOf(float64(0))
	case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
		return reflect.TypeOf("")
	case MYSQL_TYPE_DATE, MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
		return reflect.TypeOf(time.Time{})
	case MYSQL_TYPE_TIME:
		return reflect.TypeOf(time.Duration(0))
	case MYSQL_TYPE_YEAR:
		return reflect.TypeOf(int64(0))
	case MYSQL_TYPE_VARCHAR, MYSQL_TYPE_VAR_STRING, MYSQL_TYPE_STRING:
		return reflect.TypeOf("")
	case MYSQL_TYPE_BLOB, MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB:
		return reflect.TypeOf([]byte{})
	case MYSQL_TYPE_BIT:
		return reflect.TypeOf([]byte{})
	case MYSQL_TYPE_JSON:
		return reflect.TypeOf("")
	case MYSQL_TYPE_ENUM, MYSQL_TYPE_SET:
		return reflect.TypeOf("")
	case MYSQL_TYPE_GEOMETRY:
		return reflect.TypeOf([]byte{})
	default:
		return nil
	}
}

// ParseTextRow parses a row in text protocol
func ParseTextRow(data []byte, columns []ColumnDefinition) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	pos := 0

	for i := range columns {
		if data[pos] == 0xfb {
			values[i] = nil
			pos++
			continue
		}

		str, newPos, err := ReadLengthEncodedString(data, pos)
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}
		pos = newPos

		if columns[i].Type == MYSQL_TYPE_TINY && columns[i].Length == 1 {
			values[i] = str != "0" && str != ""
		} else {
			values[i] = str
		}
	}

	return values, nil
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

// readBinaryValue reads a single value in binary protocol
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
		str, newPos, err := ReadLengthEncodedString(data, pos)
		if err != nil {
			return nil, pos, err
		}
		return str, newPos, nil

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
	default:
		// Convert to string as fallback
		str := fmt.Sprintf("%v", v)
		return WriteLengthEncodedString(nil, str), nil
	}
}
