// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package unit

import (
	"bytes"
	"testing"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// buildColDefPacket constructs a minimal column definition packet.
//
//	catalog  = "def"
//	schema, table, org_table = "" (empty)
//	name, org_name = the given name string
//	extBlock = raw bytes of the extended-metadata section (nil = no ext section, caller inserts \x00)
//	charset, colLength, colType, flags, decimals = the fixed fields
func buildColDefPacket(name string, extBlock []byte, charset uint16, colLength uint32, colType, flags uint16, decimals byte) []byte {
	var p []byte

	// catalog = "def"
	p = append(p, 0x03, 'd', 'e', 'f')

	// schema, table, org_table = "" (length 0)
	p = append(p, 0x00, 0x00, 0x00)

	// name
	p = append(p, byte(len(name)))
	p = append(p, []byte(name)...)

	// org_name = same as name
	p = append(p, byte(len(name)))
	p = append(p, []byte(name)...)

	// extended metadata section
	p = append(p, extBlock...)

	// fixed-length marker (0x0c = 12)
	p = append(p, 0x0c)

	// charset (2 bytes LE)
	p = append(p, byte(charset), byte(charset>>8))

	// column length (4 bytes LE)
	p = append(p, byte(colLength), byte(colLength>>8), byte(colLength>>16), byte(colLength>>24))

	// type (1 byte)
	p = append(p, byte(colType))

	// flags (2 bytes LE)
	p = append(p, byte(flags), byte(flags>>8))

	// decimals (1 byte)
	p = append(p, decimals)

	// filler (2 bytes) — always present in real packets
	p = append(p, 0x00, 0x00)

	return p
}

// extNoInfo is the single 0x00 byte inserted when extMetadata is negotiated but
// no extended info is present for this column.
var extNoInfo = []byte{0x00}

// buildExtBlock builds the length-prefixed extended metadata block.
// items is a flat sequence of: tag(byte), value(string).
func buildExtBlock(items ...interface{}) []byte {
	var body []byte
	for i := 0; i+1 < len(items); i += 2 {
		tag := items[i].(byte)
		val := items[i+1].(string)
		body = append(body, tag)
		body = append(body, byte(len(val)))
		body = append(body, []byte(val)...)
	}
	// prepend length as a single-byte lenenc integer
	return append([]byte{byte(len(body))}, body...)
}

func TestFillColumnDefinition_NoExtMetadata(t *testing.T) {
	pkt := buildColDefPacket("id", nil, 33, 11, protocol.MYSQL_TYPE_LONGLONG, 0, 0)
	var col protocol.ColumnDefinition
	if err := protocol.FillColumnDefinition(pkt, &col, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if col.Name != "id" {
		t.Errorf("Name: got %q, want %q", col.Name, "id")
	}
	if col.Type != protocol.MYSQL_TYPE_LONGLONG {
		t.Errorf("Type: got %d, want %d", col.Type, protocol.MYSQL_TYPE_LONGLONG)
	}
	if col.ExtendedType != nil {
		t.Errorf("ExtendedType: got %q, want nil", col.ExtendedType)
	}
	if col.Format != nil {
		t.Errorf("Format: got %q, want nil", col.Format)
	}
}

func TestFillColumnDefinition_ExtMetadata_NoInfo(t *testing.T) {
	// extMetadata negotiated but this column has no extended info (0x00 marker).
	pkt := buildColDefPacket("name", extNoInfo, 33, 255, protocol.MYSQL_TYPE_VAR_STRING, 0, 0)
	var col protocol.ColumnDefinition
	if err := protocol.FillColumnDefinition(pkt, &col, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if col.Name != "name" {
		t.Errorf("Name: got %q, want %q", col.Name, "name")
	}
	if col.Type != protocol.MYSQL_TYPE_VAR_STRING {
		t.Errorf("Type: got %d, want %d", col.Type, protocol.MYSQL_TYPE_VAR_STRING)
	}
	if col.ExtendedType != nil {
		t.Errorf("ExtendedType: got %q, want nil", col.ExtendedType)
	}
	if col.Format != nil {
		t.Errorf("Format: got %q, want nil", col.Format)
	}
}

func TestFillColumnDefinition_ExtMetadata_UUIDType(t *testing.T) {
	// extTypeName = "uuid" (tag 0), no format — matches Java UuidColumn detection logic.
	extBlock := buildExtBlock(byte(0), "uuid")
	pkt := buildColDefPacket("id", extBlock, 33, 36, protocol.MYSQL_TYPE_VAR_STRING, 0, 0)
	var col protocol.ColumnDefinition
	if err := protocol.FillColumnDefinition(pkt, &col, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(col.ExtendedType, []byte("uuid")) {
		t.Errorf("ExtendedType: got %q, want %q", col.ExtendedType, "uuid")
	}
	if col.Format != nil {
		t.Errorf("Format: got %q, want nil", col.Format)
	}
}

func TestFillColumnDefinition_ExtMetadata_TypeAndFormat(t *testing.T) {
	// extTypeName = "uuid" (tag 0) + extTypeFormat = "json" (tag 1)
	extBlock := buildExtBlock(byte(0), "uuid", byte(1), "json")
	pkt := buildColDefPacket("col", extBlock, 33, 36, protocol.MYSQL_TYPE_VAR_STRING, 0, 0)
	var col protocol.ColumnDefinition
	if err := protocol.FillColumnDefinition(pkt, &col, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(col.ExtendedType, []byte("uuid")) {
		t.Errorf("ExtendedType: got %q, want %q", col.ExtendedType, "uuid")
	}
	if !bytes.Equal(col.Format, []byte("json")) {
		t.Errorf("Format: got %q, want %q", col.Format, "json")
	}
}

func TestFillColumnDefinition_ExtMetadata_UnknownTag_Skipped(t *testing.T) {
	// tag 99 (unknown) followed by tag 0 "uuid" — unknown tag must be skipped correctly.
	extBlock := buildExtBlock(byte(99), "ignored", byte(0), "uuid")
	pkt := buildColDefPacket("col", extBlock, 33, 36, protocol.MYSQL_TYPE_VAR_STRING, 0, 0)
	var col protocol.ColumnDefinition
	if err := protocol.FillColumnDefinition(pkt, &col, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !bytes.Equal(col.ExtendedType, []byte("uuid")) {
		t.Errorf("ExtendedType: got %q, want %q", col.ExtendedType, "uuid")
	}
}

func TestFillColumnDefinition_FixedFields(t *testing.T) {
	// Verify charset, length, flags, decimals are parsed correctly.
	pkt := buildColDefPacket("amount", extNoInfo, 45 /*utf8mb4*/, 10, protocol.MYSQL_TYPE_NEWDECIMAL, 0x0020 /*unsigned*/, 2)
	var col protocol.ColumnDefinition
	if err := protocol.FillColumnDefinition(pkt, &col, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if col.Charset != 45 {
		t.Errorf("Charset: got %d, want 45", col.Charset)
	}
	if col.Length != 10 {
		t.Errorf("Length: got %d, want 10", col.Length)
	}
	if col.Type != protocol.MYSQL_TYPE_NEWDECIMAL {
		t.Errorf("Type: got %d, want %d", col.Type, protocol.MYSQL_TYPE_NEWDECIMAL)
	}
	if col.Flags != 0x0020 {
		t.Errorf("Flags: got 0x%04x, want 0x0020", col.Flags)
	}
	if col.Decimals != 2 {
		t.Errorf("Decimals: got %d, want 2", col.Decimals)
	}
}
