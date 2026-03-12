// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
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
	// Extended metadata (zero-copy slices into the packet buffer kept alive by raw)
	ExtendedType []byte
	Format       []byte
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
//
// extMetadata must be true when the EXTENDED_METADATA capability was negotiated.
// The server then inserts an optional tagged block between the 6 strings and the
// fixed fields, matching the Java connector ColumnDecoder.decode() logic:
//
//	0x00        → no extended info
//	<lenenc N>  → N-byte sub-packet of {tag(1) + lenenc-string} items
//	              tag 0 = ExtendedType (e.g. "uuid")
//	              tag 1 = Format
func FillColumnDefinition(data []byte, col *ColumnDefinition, extMetadata bool) error {
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

	// Extended metadata block — present only when EXTENDED_METADATA was negotiated.
	// Layout: one byte that is either 0x00 (no info) or the first byte of a
	// length-encoded integer giving the total size of the sub-packet that follows.
	if extMetadata {
		if pos >= len(data) {
			return fmt.Errorf("column definition truncated before extended metadata marker")
		}
		if data[pos] != 0x00 {
			// Revert and read the whole block as a length-encoded byte sequence.
			blockLen, blockStart, err2 := ReadLengthEncodedInteger(data, pos)
			if err2 != nil {
				return fmt.Errorf("failed to read extended metadata block length: %w", err2)
			}
			blockEnd := blockStart + int(blockLen)
			if blockEnd > len(data) {
				return fmt.Errorf("extended metadata block exceeds packet boundary")
			}
			for sub := blockStart; sub < blockEnd; {
				tag := data[sub]
				sub++
				vLen, vStart, err3 := ReadLengthEncodedInteger(data, sub)
				if err3 != nil {
					break
				}
				vEnd := vStart + int(vLen)
				switch tag {
				case 0:
					col.ExtendedType = data[vStart:vEnd]
				case 1:
					col.Format = data[vStart:vEnd]
				}
				sub = vEnd
			}
			pos = blockEnd
		} else {
			pos++ // consume the 0x00 no-extended-info marker
		}
	}

	// Fixed-length fields marker (always 0x0c = 12)
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

	return nil
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

// ParseTextRow parses a row in text protocol, returning typed Go values.
func ParseTextRow(data []byte, columns []ColumnDefinition) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	pos := 0

	for i := range columns {
		if data[pos] == 0xfb {
			values[i] = nil
			pos++
			continue
		}

		vLen, vStart, err := ReadLengthEncodedInteger(data, pos)
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}
		vEnd := vStart + int(vLen)
		pos = vEnd

		raw := data[vStart:vEnd]
		col := &columns[i]
		switch col.Type {
		case MYSQL_TYPE_TINY:
			if col.Length == 1 {
				values[i] = vLen > 0 && raw[0] != '0'
			} else {
				values[i] = textInt64(raw)
			}
		case MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_INT24, MYSQL_TYPE_LONGLONG, MYSQL_TYPE_YEAR:
			values[i] = textInt64(raw)
		case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
			values[i] = textFloat64(raw)
		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
			values[i] = string(raw)
		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			values[i] = textDate(raw)
		case MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
			values[i] = textDatetime(raw)
		case MYSQL_TYPE_TIME:
			values[i] = textDuration(raw)
		case MYSQL_TYPE_JSON:
			values[i] = string(raw)
		default: // VARCHAR, BLOB, BIT, ENUM, SET, GEOMETRY, …
			if col.Charset == 63 { // binary charset → []byte (zero-copy)
				values[i] = raw
			} else {
				values[i] = string(raw)
			}
		}
	}

	return values, nil
}

// ── text-protocol parsing helpers (zero-alloc) ───────────────────────────────

// textInt64 parses a decimal integer (optionally signed) from ASCII bytes.
func textInt64(raw []byte) int64 {
	if len(raw) == 0 {
		return 0
	}
	neg := raw[0] == '-'
	start := 0
	if neg {
		start = 1
	}
	var v int64
	for _, b := range raw[start:] {
		v = v*10 + int64(b-'0')
	}
	if neg {
		return -v
	}
	return v
}

// textFloat64 parses a floating-point value from ASCII bytes (zero-copy).
func textFloat64(raw []byte) float64 {
	f, _ := strconv.ParseFloat(unsafe.String(unsafe.SliceData(raw), len(raw)), 64)
	return f
}

// textDate parses "YYYY-MM-DD" → time.Time or nil for zero dates.
func textDate(raw []byte) interface{} {
	if len(raw) < 10 {
		return nil
	}
	y := textDig4(raw, 0)
	mo := textDig2(raw, 5)
	d := textDig2(raw, 8)
	if y == 0 && mo == 0 && d == 0 {
		return nil
	}
	return time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC)
}

// textDatetime parses "YYYY-MM-DD HH:MM:SS[.micro]" → time.Time or nil.
func textDatetime(raw []byte) interface{} {
	if len(raw) < 19 {
		return nil
	}
	y := textDig4(raw, 0)
	mo := textDig2(raw, 5)
	d := textDig2(raw, 8)
	h := textDig2(raw, 11)
	mi := textDig2(raw, 14)
	s := textDig2(raw, 17)
	ns := 0
	if len(raw) > 20 && raw[19] == '.' {
		frac := raw[20:]
		micro := 0
		for k, b := range frac {
			if k >= 6 {
				break
			}
			micro = micro*10 + int(b-'0')
		}
		for k := len(frac); k < 6; k++ {
			micro *= 10
		}
		ns = micro * 1000
	}
	if y == 0 && mo == 0 && d == 0 {
		return nil
	}
	return time.Date(y, time.Month(mo), d, h, mi, s, ns, time.UTC)
}

// textDuration parses "[-]H+:MM:SS[.micro]" → time.Duration.
func textDuration(raw []byte) time.Duration {
	if len(raw) == 0 {
		return 0
	}
	neg := raw[0] == '-'
	s := raw
	if neg {
		s = raw[1:]
	}
	// hours can exceed 2 digits (max TIME is 838:59:59)
	colon := 0
	for colon < len(s) && s[colon] != ':' {
		colon++
	}
	if len(s) < colon+6 {
		return 0
	}
	h := textInt64(s[:colon])
	mi := int64(textDig2(s, colon+1))
	se := int64(textDig2(s, colon+4))
	var micros int64
	if len(s) > colon+6 && s[colon+6] == '.' {
		frac := s[colon+7:]
		for k, b := range frac {
			if k >= 6 {
				break
			}
			micros = micros*10 + int64(b-'0')
		}
		for k := len(frac); k < 6; k++ {
			micros *= 10
		}
	}
	dur := time.Duration(h)*time.Hour +
		time.Duration(mi)*time.Minute +
		time.Duration(se)*time.Second +
		time.Duration(micros)*time.Microsecond
	if neg {
		return -dur
	}
	return dur
}

// textDig4 reads a 4-digit decimal integer at offset off (no bounds check).
func textDig4(b []byte, off int) int {
	return int(b[off]-'0')*1000 + int(b[off+1]-'0')*100 + int(b[off+2]-'0')*10 + int(b[off+3]-'0')
}

// textDig2 reads a 2-digit decimal integer at offset off (no bounds check).
func textDig2(b []byte, off int) int {
	return int(b[off]-'0')*10 + int(b[off+1]-'0')
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
		vLen, vStart, err := ReadLengthEncodedInteger(data, pos)
		if err != nil {
			return nil, pos, err
		}
		vEnd := vStart + int(vLen)
		// binary charset (63) → []byte; JSON is always text even with binary charset
		if col.Charset == 63 && col.Type != MYSQL_TYPE_JSON {
			cp := make([]byte, vLen)
			copy(cp, data[vStart:vEnd])
			return cp, vEnd, nil
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
	default:
		// Convert to string as fallback
		str := fmt.Sprintf("%v", v)
		return WriteLengthEncodedString(nil, str), nil
	}
}
