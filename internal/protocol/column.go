// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
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
