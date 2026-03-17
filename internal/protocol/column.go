// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"fmt"
	"reflect"
	"time"
)

// ColumnDefinition represents a column definition packet.
// Hot-path numeric fields are placed first so they occupy the first cache line
// during row parsing (ParseTextRow / ParseBinaryRow never touch Name).
type ColumnDefinition struct {
	// Hot-path: accessed on every row — kept first for cache locality
	Type     byte
	Decimals byte
	Flags    uint16
	Charset  uint16
	Length   uint32
	// Cold-path: column name (copied out of the packet so scratch buffers can be reused)
	Name string
	// Extended metadata (copied; only populated when EXTENDED_METADATA is negotiated)
	ExtendedType []byte
	Format       []byte
}

// skipIdentifier advances pos past a length-encoded identifier.
// SQL identifiers have a maximum length of 256, so the encoding is either:
//
//	< 0xfb  → 1-byte length, value is the byte itself (common path)
//	  0xfc  → 2-byte LE length follows
//
// No error return: column definition packets from the server are trusted.
func skipIdentifier(data []byte, pos int) int {
	l := int(data[pos])
	pos++
	if l < 0xfb {
		return pos + l
	}
	// 0xfc: two-byte LE length
	l = int(data[pos]) | int(data[pos+1])<<8
	return pos + 2 + l
}

// FillColumnDefinition parses a column definition packet into a pre-allocated col.
// Catalog, Schema, Table, OrgTable and OrgName are skipped without allocations.
// Name is copied out of the packet buffer so callers may use ReadScratch.
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

	// Skip: catalog, schema, table, org_table (4 length-encoded identifiers)
	for range 4 {
		pos = skipIdentifier(data, pos)
	}

	// Name: copy out of packet so scratch buffer can be reused.
	nameLen := int(data[pos])
	pos++
	if nameLen >= 0xfb {
		nameLen = int(data[pos]) | int(data[pos+1])<<8
		pos += 2
	}
	col.Name = string(data[pos : pos+nameLen])
	pos += nameLen

	// Skip: org_name
	pos = skipIdentifier(data, pos)

	// Extended metadata block — present only when EXTENDED_METADATA was negotiated.
	// Layout: one byte that is either 0x00 (no info) or the first byte of a
	// length-encoded integer giving the total size of the sub-packet that follows.
	if extMetadata {
		if pos >= len(data) {
			return fmt.Errorf("column definition truncated before extended metadata marker")
		}
		if data[pos] != 0x00 {
			// Revert and read the whole block as a length-encoded byte sequence.
			blockLen, blockStart := ReadLengthEncodedInteger(data, pos)
			blockEnd := blockStart + int(blockLen)
			if blockEnd > len(data) {
				return fmt.Errorf("extended metadata block exceeds packet boundary")
			}
			for sub := blockStart; sub < blockEnd; {
				tag := data[sub]
				sub++
				vLen, vStart := ReadLengthEncodedInteger(data, sub)
				vEnd := vStart + int(vLen)
				switch tag {
				case 0:
					col.ExtendedType = append([]byte(nil), data[vStart:vEnd]...)
				case 1:
					col.Format = append([]byte(nil), data[vStart:vEnd]...)
				}
				sub = vEnd
			}
			pos = blockEnd
		} else {
			pos++ // consume the 0x00 no-extended-info marker
		}
	}

	// Fixed-length fields block is always 0x0c (12 bytes). Skip the length marker.
	// One bounds check on the last byte we read lets the compiler eliminate the rest.
	pos++ // skip 0x0c
	_ = data[pos+9]
	col.Charset = uint16(data[pos]) | uint16(data[pos+1])<<8
	col.Length = uint32(data[pos+2]) | uint32(data[pos+3])<<8 | uint32(data[pos+4])<<16 | uint32(data[pos+5])<<24
	col.Type = data[pos+6]
	col.Flags = uint16(data[pos+7]) | uint16(data[pos+8])<<8
	col.Decimals = data[pos+9]

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
