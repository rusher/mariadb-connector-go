// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
)

// NullLength is the sentinel value returned by ReadLengthEncodedInteger when the
// length-encoded field carries the NULL marker (0xfb). Callers must check for
// this value before using the length to index into a data slice.
const NullLength = ^uint64(0)

// ReadLengthEncodedInteger reads a length-encoded integer.
// Returns (NullLength, newPos) for the NULL marker (0xfb).
func ReadLengthEncodedInteger(data []byte, pos int) (uint64, int) {
	first := data[pos]
	pos++

	switch {
	case first < 0xfb:
		return uint64(first), pos

	case first == 0xfb:
		return NullLength, pos

	case first == 0xfc:
		_ = data[pos+1]
		val := uint64(data[pos]) | uint64(data[pos+1])<<8
		return val, pos + 2

	case first == 0xfd:
		_ = data[pos+2]
		val := uint64(data[pos]) | uint64(data[pos+1])<<8 | uint64(data[pos+2])<<16
		return val, pos + 3

	default: // 0xfe
		_ = data[pos+7]
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		return val, pos + 8
	}
}

// ReadLengthEncodedString reads a length-encoded string
func ReadLengthEncodedString(data []byte, pos int) (string, int, error) {
	length, newPos := ReadLengthEncodedInteger(data, pos)
	if length == NullLength {
		return "", newPos, nil
	}
	if length > 0 {
		_ = data[newPos+int(length)-1]
	}
	str := string(data[newPos : newPos+int(length)])
	return str, newPos + int(length), nil
}

// WriteLengthEncodedInteger writes a length-encoded integer
func WriteLengthEncodedInteger(buf []byte, value uint64) []byte {
	switch {
	case value < 251:
		return append(buf, byte(value))

	case value < 1<<16:
		return append(buf, 0xfc, byte(value), byte(value>>8))

	case value < 1<<24:
		return append(buf, 0xfd, byte(value), byte(value>>8), byte(value>>16))

	default:
		b := make([]byte, 9)
		b[0] = 0xfe
		binary.LittleEndian.PutUint64(b[1:], value)
		return append(buf, b...)
	}
}

// WriteLengthEncodedString writes a length-encoded string
func WriteLengthEncodedString(buf []byte, str string) []byte {
	buf = WriteLengthEncodedInteger(buf, uint64(len(str)))
	return append(buf, str...)
}

// ReadNullTerminatedString reads a null-terminated string
func ReadNullTerminatedString(data []byte, pos int) (string, int, error) {
	start := pos
	for pos < len(data) && data[pos] != 0 {
		pos++
	}

	if pos >= len(data) {
		return "", start, fmt.Errorf("null terminator not found")
	}

	return string(data[start:pos]), pos + 1, nil
}
