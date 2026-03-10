// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// PacketReader reads MySQL protocol packets
type PacketReader struct {
	reader   io.Reader
	sequence *uint8 // Shared sequence pointer
	logger   Logger // Debug logger
}

// NewPacketReader creates a new PacketReader with shared sequence
func NewPacketReader(r io.Reader, seq *uint8) *PacketReader {
	return &PacketReader{
		reader:   r,
		sequence: seq,
		logger:   GetLogger(),
	}
}

// ReadPacket reads a single packet from the connection
func (pr *PacketReader) ReadPacket() ([]byte, error) {
	// Read packet header (3 bytes length + 1 byte sequence)
	header := make([]byte, 4)
	if _, err := io.ReadFull(pr.reader, header); err != nil {
		return nil, fmt.Errorf("failed to read packet header: %w", err)
	}

	// Parse packet length (first 3 bytes, little-endian)
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16

	// Verify sequence number
	sequence := header[3]
	if sequence != *pr.sequence {
		return nil, fmt.Errorf("sequence mismatch: expected %d, got %d", *pr.sequence, sequence)
	}

	receivedSeq := *pr.sequence
	*pr.sequence++

	// Read packet data
	data := make([]byte, length)
	if _, err := io.ReadFull(pr.reader, data); err != nil {
		return nil, fmt.Errorf("failed to read packet data: %w", err)
	}

	// Handle multi-packet (packets of exactly MaxPacketSize)
	if length == MaxPacketSize {
		// Read continuation packets
		for {
			nextData, err := pr.ReadPacket()
			if err != nil {
				return nil, err
			}
			data = append(data, nextData...)

			// Stop if this packet is smaller than max size
			if len(nextData) < MaxPacketSize {
				break
			}
		}
	}

	// Log received packet
	if pr.logger.IsEnabled() {
		pr.logger.LogReceive(data, receivedSeq)
	}

	return data, nil
}

// ResetSequence resets the sequence number
func (pr *PacketReader) ResetSequence() {
	*pr.sequence = 0
}

// PacketWriter writes MySQL protocol packets
type PacketWriter struct {
	writer   io.Writer
	sequence *uint8 // Shared sequence pointer
	logger   Logger // Debug logger
}

// NewPacketWriter creates a new PacketWriter with shared sequence
func NewPacketWriter(w io.Writer, seq *uint8) *PacketWriter {
	return &PacketWriter{
		writer:   w,
		sequence: seq,
		logger:   GetLogger(),
	}
}

// WritePacket writes a single packet to the connection
// Must send empty packet if len is zero, and if size is exactly 16M (MaxPacketSize),
// an empty packet must be sent to indicate that transmission is complete
func (pw *PacketWriter) WritePacket(data []byte) error {
	dataLen := len(data)

	// Log sent packet (before splitting)
	if pw.logger.IsEnabled() {
		pw.logger.LogSend(data, *pw.sequence)
	}

	// Handle empty packet case - must still send a packet with 0 length
	if dataLen == 0 {
		header := make([]byte, 4)
		header[0] = 0
		header[1] = 0
		header[2] = 0
		header[3] = *pw.sequence
		*pw.sequence++

		if _, err := pw.writer.Write(header); err != nil {
			return fmt.Errorf("failed to write empty packet header: %w", err)
		}
		return nil
	}

	// Split into multiple packets if necessary
	for dataLen > 0 {
		// Determine chunk size
		chunkSize := dataLen
		if chunkSize > MaxPacketSize {
			chunkSize = MaxPacketSize
		}

		// Build packet header
		header := make([]byte, 4)
		header[0] = byte(chunkSize)
		header[1] = byte(chunkSize >> 8)
		header[2] = byte(chunkSize >> 16)
		header[3] = *pw.sequence
		*pw.sequence++

		// Write header
		if _, err := pw.writer.Write(header); err != nil {
			return fmt.Errorf("failed to write packet header: %w", err)
		}

		// Write data chunk
		offset := len(data) - dataLen
		if _, err := pw.writer.Write(data[offset : offset+chunkSize]); err != nil {
			return fmt.Errorf("failed to write packet data: %w", err)
		}

		dataLen -= chunkSize

		// If we just sent exactly MaxPacketSize bytes, send an empty packet to indicate completion
		if chunkSize == MaxPacketSize && dataLen == 0 {
			header := make([]byte, 4)
			header[0] = 0
			header[1] = 0
			header[2] = 0
			header[3] = *pw.sequence
			*pw.sequence++

			if _, err := pw.writer.Write(header); err != nil {
				return fmt.Errorf("failed to write terminating empty packet header: %w", err)
			}
		}
	}

	return nil
}

// ResetSequence resets the sequence number
func (pw *PacketWriter) ResetSequence() {
	*pw.sequence = 0
}

// ReadLengthEncodedInteger reads a length-encoded integer
func ReadLengthEncodedInteger(data []byte, pos int) (uint64, int, error) {
	if pos >= len(data) {
		return 0, pos, fmt.Errorf("insufficient data for length-encoded integer")
	}

	first := data[pos]
	pos++

	switch {
	case first < 0xfb:
		return uint64(first), pos, nil

	case first == 0xfc:
		if pos+2 > len(data) {
			return 0, pos, fmt.Errorf("insufficient data for 2-byte length-encoded integer")
		}
		val := uint64(data[pos]) | uint64(data[pos+1])<<8
		return val, pos + 2, nil

	case first == 0xfd:
		if pos+3 > len(data) {
			return 0, pos, fmt.Errorf("insufficient data for 3-byte length-encoded integer")
		}
		val := uint64(data[pos]) | uint64(data[pos+1])<<8 | uint64(data[pos+2])<<16
		return val, pos + 3, nil

	case first == 0xfe:
		if pos+8 > len(data) {
			return 0, pos, fmt.Errorf("insufficient data for 8-byte length-encoded integer")
		}
		val := binary.LittleEndian.Uint64(data[pos : pos+8])
		return val, pos + 8, nil

	default:
		return 0, pos, fmt.Errorf("invalid length-encoded integer marker: 0x%x", first)
	}
}

// ReadLengthEncodedString reads a length-encoded string
func ReadLengthEncodedString(data []byte, pos int) (string, int, error) {
	length, newPos, err := ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return "", pos, err
	}

	if newPos+int(length) > len(data) {
		return "", pos, fmt.Errorf("insufficient data for length-encoded string")
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
	return append(buf, []byte(str)...)
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

// Helper functions for reading/writing integers
func GetUint16(data []byte) uint16 {
	return binary.LittleEndian.Uint16(data)
}

func GetUint32(data []byte) uint32 {
	return binary.LittleEndian.Uint32(data)
}

func GetUint64(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

func PutUint16(data []byte, value uint16) {
	binary.LittleEndian.PutUint16(data, value)
}

func PutUint32(data []byte, value uint32) {
	binary.LittleEndian.PutUint32(data, value)
}

func PutUint64(data []byte, value uint64) {
	binary.LittleEndian.PutUint64(data, value)
}
