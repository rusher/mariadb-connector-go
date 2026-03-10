// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
)

// ReadBuffer provides utility methods for reading MySQL protocol data
// Based on MariaDB Java connector ReadableByteBuf
type ReadBuffer struct {
	data []byte
	pos  int
}

// NewReadBuffer creates a new ReadBuffer from byte slice
func NewReadBuffer(data []byte) *ReadBuffer {
	return &ReadBuffer{
		data: data,
		pos:  0,
	}
}

// ReadableBytes returns the number of bytes remaining
func (b *ReadBuffer) ReadableBytes() int {
	return len(b.data) - b.pos
}

// Skip skips n bytes
func (b *ReadBuffer) Skip(n int) error {
	if b.pos+n > len(b.data) {
		return fmt.Errorf("buffer underflow: trying to skip %d bytes, only %d available", n, b.ReadableBytes())
	}
	b.pos += n
	return nil
}

// SkipByte skips 1 byte
func (b *ReadBuffer) SkipByte() error {
	return b.Skip(1)
}

// ReadByte reads a single byte
func (b *ReadBuffer) ReadByte() (byte, error) {
	if b.pos >= len(b.data) {
		return 0, fmt.Errorf("buffer underflow: no bytes available")
	}
	val := b.data[b.pos]
	b.pos++
	return val, nil
}

// ReadUint8 reads an unsigned 8-bit integer
func (b *ReadBuffer) ReadUint8() (uint8, error) {
	return b.ReadByte()
}

// ReadUint16 reads an unsigned 16-bit integer (little-endian)
func (b *ReadBuffer) ReadUint16() (uint16, error) {
	if b.pos+2 > len(b.data) {
		return 0, fmt.Errorf("buffer underflow: need 2 bytes, only %d available", b.ReadableBytes())
	}
	val := binary.LittleEndian.Uint16(b.data[b.pos:])
	b.pos += 2
	return val, nil
}

// ReadUint32 reads an unsigned 32-bit integer (little-endian)
func (b *ReadBuffer) ReadUint32() (uint32, error) {
	if b.pos+4 > len(b.data) {
		return 0, fmt.Errorf("buffer underflow: need 4 bytes, only %d available", b.ReadableBytes())
	}
	val := binary.LittleEndian.Uint32(b.data[b.pos:])
	b.pos += 4
	return val, nil
}

// ReadUint64 reads an unsigned 64-bit integer (little-endian)
func (b *ReadBuffer) ReadUint64() (uint64, error) {
	if b.pos+8 > len(b.data) {
		return 0, fmt.Errorf("buffer underflow: need 8 bytes, only %d available", b.ReadableBytes())
	}
	val := binary.LittleEndian.Uint64(b.data[b.pos:])
	b.pos += 8
	return val, nil
}

// ReadLengthEncodedInt reads a length-encoded integer
// See https://mariadb.com/kb/en/protocol-data-types/#length-encoded-integers
func (b *ReadBuffer) ReadLengthEncodedInt() (uint64, error) {
	if b.pos >= len(b.data) {
		return 0, fmt.Errorf("buffer underflow: no bytes available")
	}

	first := b.data[b.pos]
	b.pos++

	switch first {
	case 0xfb:
		// NULL value
		return 0, nil
	case 0xfc:
		// 2-byte integer
		if b.pos+2 > len(b.data) {
			return 0, fmt.Errorf("buffer underflow: need 2 bytes for length-encoded int")
		}
		val := uint64(b.data[b.pos]) | uint64(b.data[b.pos+1])<<8
		b.pos += 2
		return val, nil
	case 0xfd:
		// 3-byte integer
		if b.pos+3 > len(b.data) {
			return 0, fmt.Errorf("buffer underflow: need 3 bytes for length-encoded int")
		}
		val := uint64(b.data[b.pos]) | uint64(b.data[b.pos+1])<<8 | uint64(b.data[b.pos+2])<<16
		b.pos += 3
		return val, nil
	case 0xfe:
		// 8-byte integer
		if b.pos+8 > len(b.data) {
			return 0, fmt.Errorf("buffer underflow: need 8 bytes for length-encoded int")
		}
		val := binary.LittleEndian.Uint64(b.data[b.pos:])
		b.pos += 8
		return val, nil
	default:
		// 1-byte integer (already read)
		return uint64(first), nil
	}
}

// ReadLengthEncodedString reads a length-encoded string
func (b *ReadBuffer) ReadLengthEncodedString() (string, error) {
	length, err := b.ReadLengthEncodedInt()
	if err != nil {
		return "", err
	}

	if length == 0 {
		return "", nil
	}

	return b.ReadString(int(length))
}

// ReadString reads a string of specified length
func (b *ReadBuffer) ReadString(length int) (string, error) {
	if b.pos+length > len(b.data) {
		return "", fmt.Errorf("buffer underflow: need %d bytes, only %d available", length, b.ReadableBytes())
	}

	str := string(b.data[b.pos : b.pos+length])
	b.pos += length
	return str, nil
}

// ReadBytes reads n bytes
func (b *ReadBuffer) ReadBytes(n int) ([]byte, error) {
	if b.pos+n > len(b.data) {
		return nil, fmt.Errorf("buffer underflow: need %d bytes, only %d available", n, b.ReadableBytes())
	}

	bytes := b.data[b.pos : b.pos+n]
	b.pos += n
	return bytes, nil
}

// ReadNullTerminatedString reads a null-terminated string
func (b *ReadBuffer) ReadNullTerminatedString() (string, error) {
	start := b.pos
	for b.pos < len(b.data) {
		if b.data[b.pos] == 0 {
			str := string(b.data[start:b.pos])
			b.pos++ // Skip the null terminator
			return str, nil
		}
		b.pos++
	}
	return "", fmt.Errorf("null terminator not found")
}

// Peek returns the next byte without advancing the position
func (b *ReadBuffer) Peek() (byte, error) {
	if b.pos >= len(b.data) {
		return 0, fmt.Errorf("buffer underflow: no bytes available")
	}
	return b.data[b.pos], nil
}

// Position returns the current read position
func (b *ReadBuffer) Position() int {
	return b.pos
}

// SetPosition sets the read position
func (b *ReadBuffer) SetPosition(pos int) error {
	if pos < 0 || pos > len(b.data) {
		return fmt.Errorf("invalid position: %d (buffer size: %d)", pos, len(b.data))
	}
	b.pos = pos
	return nil
}

// Remaining returns the remaining bytes as a slice
func (b *ReadBuffer) Remaining() []byte {
	if b.pos >= len(b.data) {
		return []byte{}
	}
	return b.data[b.pos:]
}

// Reset resets the buffer to the beginning
func (b *ReadBuffer) Reset() {
	b.pos = 0
}
