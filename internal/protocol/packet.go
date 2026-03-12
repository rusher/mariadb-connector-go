// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"encoding/binary"
	"fmt"
	"io"
)

// HdrSize is the number of bytes reserved at the start of every packet buffer
// for the MySQL protocol header (3-byte length + 1-byte sequence number).
const HdrSize = 4

// hdrSize is the package-local alias.
const hdrSize = HdrSize

// PacketReader reads MySQL protocol packets
type PacketReader struct {
	reader   io.Reader
	sequence *uint8  // Shared sequence pointer
	logger   Logger  // Debug logger
	hdr      [4]byte // reused per-call to avoid per-read heap allocation
	scratch  []byte  // reusable buffer for transient reads (do not retain across calls)
}

// NewPacketReader creates a new PacketReader with shared sequence
func NewPacketReader(r io.Reader, seq *uint8) *PacketReader {
	return &PacketReader{
		reader:   r,
		sequence: seq,
		logger:   GetLogger(),
	}
}

// ReadScratch reads a packet reusing an internal buffer. The returned slice is
// valid only until the next ReadScratch call — callers must not retain it.
// Use for packets whose payload is consumed immediately (column defs, EOF, OK).
func (pr *PacketReader) ReadScratch() ([]byte, error) {
	if _, err := io.ReadFull(pr.reader, pr.hdr[:]); err != nil {
		return nil, fmt.Errorf("failed to read packet header: %w", err)
	}

	length := int(pr.hdr[0]) | int(pr.hdr[1])<<8 | int(pr.hdr[2])<<16
	sequence := pr.hdr[3]
	if sequence != *pr.sequence {
		return nil, fmt.Errorf("sequence mismatch: expected %d, got %d", *pr.sequence, sequence)
	}
	receivedSeq := *pr.sequence
	*pr.sequence++

	if cap(pr.scratch) >= length {
		pr.scratch = pr.scratch[:length]
	} else {
		pr.scratch = make([]byte, length)
	}
	if _, err := io.ReadFull(pr.reader, pr.scratch); err != nil {
		return nil, fmt.Errorf("failed to read packet data: %w", err)
	}

	// Multi-packet: fall back to a fresh allocation since we must concatenate.
	if length == MaxPacketSize {
		more, err := pr.ReadPacket()
		if err != nil {
			return nil, err
		}
		data := make([]byte, length+len(more))
		copy(data, pr.scratch)
		copy(data[length:], more)
		return data, nil
	}

	if pr.logger.IsEnabled() {
		pr.logger.LogReceive(pr.scratch, receivedSeq)
	}
	return pr.scratch, nil
}

// ReadPacket reads a single packet from the connection
func (pr *PacketReader) ReadPacket() ([]byte, error) {
	if _, err := io.ReadFull(pr.reader, pr.hdr[:]); err != nil {
		return nil, fmt.Errorf("failed to read packet header: %w", err)
	}

	// Parse packet length (first 3 bytes, little-endian)
	length := int(pr.hdr[0]) | int(pr.hdr[1])<<8 | int(pr.hdr[2])<<16

	// Verify sequence number
	sequence := pr.hdr[3]
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
	sequence *uint8  // Shared sequence pointer
	logger   Logger  // Debug logger
	hdr      [4]byte // reused per-call to avoid per-write heap allocation
	scratch  []byte  // reusable send buffer — do not retain across Write calls
}

// NewPacketWriter creates a new PacketWriter with shared sequence
func NewPacketWriter(w io.Writer, seq *uint8) *PacketWriter {
	return &PacketWriter{
		writer:   w,
		sequence: seq,
		logger:   GetLogger(),
		scratch:  make([]byte, 16*1024),
	}
}

// Buf returns the writer's scratch buffer for use by packet constructors.
// The capacity is 16 KiB; packet builders use it in-place when the payload fits.
// The returned slice must not be retained across Write calls.
func (pw *PacketWriter) Buf() []byte {
	return pw.scratch
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
		pw.hdr = [4]byte{0, 0, 0, *pw.sequence}
		*pw.sequence++
		if _, err := pw.writer.Write(pw.hdr[:]); err != nil {
			return fmt.Errorf("failed to write empty packet header: %w", err)
		}
		return nil
	}

	// Split into multiple packets if necessary
	for dataLen > 0 {
		chunkSize := dataLen
		if chunkSize > MaxPacketSize {
			chunkSize = MaxPacketSize
		}

		pw.hdr[0] = byte(chunkSize)
		pw.hdr[1] = byte(chunkSize >> 8)
		pw.hdr[2] = byte(chunkSize >> 16)
		pw.hdr[3] = *pw.sequence
		*pw.sequence++

		if _, err := pw.writer.Write(pw.hdr[:]); err != nil {
			return fmt.Errorf("failed to write packet header: %w", err)
		}

		offset := len(data) - dataLen
		if _, err := pw.writer.Write(data[offset : offset+chunkSize]); err != nil {
			return fmt.Errorf("failed to write packet data: %w", err)
		}

		dataLen -= chunkSize

		if chunkSize == MaxPacketSize && dataLen == 0 {
			pw.hdr = [4]byte{0, 0, 0, *pw.sequence}
			*pw.sequence++
			if _, err := pw.writer.Write(pw.hdr[:]); err != nil {
				return fmt.Errorf("failed to write terminating empty packet header: %w", err)
			}
		}
	}

	return nil
}

// Write writes a packet whose first 4 bytes are reserved for the header.
// For payloads ≤ MaxPacketSize (the common case) it fills buf[0:4] in-place
// and issues a single Write syscall — zero extra allocations, zero extra copies.
// For larger payloads it falls back to WritePacket.
func (pw *PacketWriter) Write(buf []byte) error {
	payload := buf[hdrSize:]
	payloadLen := len(payload)

	if pw.logger.IsEnabled() {
		pw.logger.LogSend(payload, *pw.sequence)
	}

	if payloadLen <= MaxPacketSize {
		buf[0] = byte(payloadLen)
		buf[1] = byte(payloadLen >> 8)
		buf[2] = byte(payloadLen >> 16)
		buf[3] = *pw.sequence
		*pw.sequence++
		_, err := pw.writer.Write(buf)
		return err
	}

	// Rare: payload exceeds 16 MB — fall back to multi-packet path.
	return pw.WritePacket(payload)
}

// ResetSequence resets the sequence number
func (pw *PacketWriter) ResetSequence() {
	*pw.sequence = 0
}

// ReadLengthEncodedInteger reads a length-encoded integer
func ReadLengthEncodedInteger(data []byte, pos int) (uint64, int, error) {
	first := data[pos]
	pos++

	switch {
	case first < 0xfb:
		return uint64(first), pos, nil

	case first == 0xfc:
		_ = data[pos+1]
		val := uint64(data[pos]) | uint64(data[pos+1])<<8
		return val, pos + 2, nil

	case first == 0xfd:
		_ = data[pos+2]
		val := uint64(data[pos]) | uint64(data[pos+1])<<8 | uint64(data[pos+2])<<16
		return val, pos + 3, nil

	case first == 0xfe:
		_ = data[pos+7]
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

// ServerMessage represents a message from the server
type ServerMessage interface {
	GetSequence() uint8
}

// ClientMessage represents a message to the server
type ClientMessage interface {
	Encode() ([]byte, error)
}

// Completion represents the result of a query execution.
// Returned by both OK and EOF packets, and by result sets
// (which are terminated by an OK or EOF packet).
type Completion struct {
	// From OK/EOF packets
	AffectedRows int64
	InsertID     int64
	WarningCount uint16
	ServerStatus uint16
	Message      string

	// Populated for result sets
	Columns []ColumnDefinition
	Binary  bool
	Rows    [][]byte // pre-fetched row packets
	Loaded  bool     // true if all row packets are in memory (false = still streaming)
}

// HasResultSet returns true if this completion carries a result set
func (c *Completion) HasResultSet() bool {
	return len(c.Columns) > 0
}

// LastInsertId implements driver.Result
func (c *Completion) LastInsertId() (int64, error) {
	return c.InsertID, nil
}

// RowsAffected implements driver.Result
func (c *Completion) RowsAffected() (int64, error) {
	return c.AffectedRows, nil
}
