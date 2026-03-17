// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
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
	hdr      [4]byte // reused per-call to avoid per-read heap allocation
	scratch  []byte  // reusable buffer for transient reads (do not retain across calls)
}

// NewPacketReader creates a new PacketReader with shared sequence
func NewPacketReader(r io.Reader, seq *uint8) *PacketReader {
	return &PacketReader{
		reader:   r,
		sequence: seq,
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
	*pr.sequence++

	if cap(pr.scratch) >= length {
		pr.scratch = pr.scratch[:length]
	} else {
		pr.scratch = make([]byte, length)
	}
	if _, err := io.ReadFull(pr.reader, pr.scratch); err != nil {
		return nil, fmt.Errorf("failed to read packet data: %w", err)
	}

	// Multi-packet: must allocate and concatenate all continuation packets
	if length == MaxPacketSize {
		data := make([]byte, length, length*2)
		copy(data, pr.scratch)

		for {
			// Read next packet header
			if _, err := io.ReadFull(pr.reader, pr.hdr[:]); err != nil {
				return nil, fmt.Errorf("failed to read packet header: %w", err)
			}

			nextLen := int(pr.hdr[0]) | int(pr.hdr[1])<<8 | int(pr.hdr[2])<<16
			sequence := pr.hdr[3]
			if sequence != *pr.sequence {
				return nil, fmt.Errorf("sequence mismatch: expected %d, got %d", *pr.sequence, sequence)
			}
			*pr.sequence++

			// Read packet data
			nextData := make([]byte, nextLen)
			if _, err := io.ReadFull(pr.reader, nextData); err != nil {
				return nil, fmt.Errorf("failed to read packet data: %w", err)
			}

			data = append(data, nextData...)

			// Stop if this packet is smaller than max size
			if nextLen < MaxPacketSize {
				break
			}
		}
		return data, nil
	}

	return pr.scratch, nil
}

// peeker is satisfied by *bufio.Reader.
type peeker interface {
	Peek(n int) ([]byte, error)
}

// PeekIsTerminator reports whether the next packet on the wire is a result-set
// terminator (0xff ERR or 0xfe EOF/OK-as-EOF with length < 0xffffff) without
// consuming any bytes. Returns false if the underlying reader does not support
// Peek or fewer than 5 bytes are buffered.
func (pr *PacketReader) PeekIsTerminator() bool {
	p, ok := pr.reader.(peeker)
	if !ok {
		return false
	}
	hdr, err := p.Peek(5)
	if err != nil || len(hdr) < 5 {
		return false
	}
	if hdr[3] != *pr.sequence {
		return false
	}
	length := int(hdr[0]) | int(hdr[1])<<8 | int(hdr[2])<<16
	if length < 1 {
		return false
	}
	first := hdr[4]
	return first == 0xff || (first == 0xfe && length < 0xffffff)
}

// ResetSequence resets the sequence number
func (pr *PacketReader) ResetSequence() {
	*pr.sequence = 0
}

// PacketWriter writes MySQL protocol packets
type PacketWriter struct {
	writer   io.Writer
	sequence *uint8  // Shared sequence pointer
	hdr      [4]byte // reused per-call to avoid per-write heap allocation
	scratch  []byte  // reusable send buffer — do not retain across Write calls
}

// NewPacketWriter creates a new PacketWriter with shared sequence
func NewPacketWriter(w io.Writer, seq *uint8) *PacketWriter {
	return &PacketWriter{
		writer:   w,
		sequence: seq,
		scratch:  make([]byte, 64*1024),
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
