// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// Logger interface for protocol logging
type Logger interface {
	LogSend(data []byte, sequence uint8)
	LogReceive(data []byte, sequence uint8)
	IsEnabled() bool
}

// DebugLogger implements Logger with hex dump output
type DebugLogger struct {
	enabled bool
	logger  *log.Logger
}

// NewDebugLogger creates a new debug logger
func NewDebugLogger(enabled bool) *DebugLogger {
	return &DebugLogger{
		enabled: enabled,
		logger:  log.New(os.Stdout, "[MariaDB] ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}

// IsEnabled returns whether logging is enabled
func (l *DebugLogger) IsEnabled() bool {
	return l.enabled
}

// LogSend logs a packet being sent to the server
func (l *DebugLogger) LogSend(data []byte, sequence uint8) {
	if !l.enabled {
		return
	}

	l.logger.Printf("==> SEND (seq=%d, len=%d)\n%s", sequence, len(data), hexDump(data, 0, len(data)))
}

// LogReceive logs a packet received from the server
func (l *DebugLogger) LogReceive(data []byte, sequence uint8) {
	if !l.enabled {
		return
	}

	l.logger.Printf("<== RECV (seq=%d, len=%d)\n%s", sequence, len(data), hexDump(data, 0, len(data)))
}

// hexDump creates a hex dump string similar to MariaDB Java connector
// Output format:
//
//	+--------------------------------------------------+
//	|  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |
//
// +------+--------------------------------------------------+------------------+
// |000000| 5F 00 00 00 03 73 65 74  20 61 75 74 6F 63 6F 6D | _....set autocom |
// |000010| 6D 69 74 3D 31 2C 20 73  65 73 73 69 6F 6E 5F 74 | mit=1, session_t |
// +------+--------------------------------------------------+------------------+
func hexDump(data []byte, offset, length int) string {
	if len(data) == 0 {
		return ""
	}

	var sb strings.Builder

	// Header
	sb.WriteString("       +--------------------------------------------------+\n")
	sb.WriteString("       |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n")
	sb.WriteString("+------+--------------------------------------------------+------------------+\n")

	pos := offset
	line := 0
	hexChars := make([]byte, 16)
	asciiChars := make([]byte, 16)

	for pos < offset+length {
		// Start of line
		if pos%16 == 0 {
			sb.WriteString(fmt.Sprintf("|%06X| ", line*16))
		}

		// Hex value
		byteVal := data[pos]
		sb.WriteString(fmt.Sprintf("%02X ", byteVal))

		// Store for ASCII representation
		posInLine := pos % 16
		hexChars[posInLine] = byteVal
		if byteVal > 31 && byteVal < 127 {
			asciiChars[posInLine] = byteVal
		} else {
			asciiChars[posInLine] = '.'
		}

		// Extra space after 8 bytes
		if posInLine == 7 {
			sb.WriteString(" ")
		}

		// End of line or end of data
		if posInLine == 15 || pos == offset+length-1 {
			// Pad if incomplete line
			remaining := 15 - posInLine
			if remaining > 0 {
				// Add padding for hex section
				for i := 0; i < remaining; i++ {
					sb.WriteString("   ")
					// Add extra space after position 7
					if posInLine+i+1 == 8 {
						sb.WriteString(" ")
					}
				}
			}

			// ASCII representation
			sb.WriteString("| ")
			// Write actual ASCII chars
			for i := 0; i <= posInLine; i++ {
				sb.WriteByte(asciiChars[i])
			}
			// Pad remaining ASCII positions
			for i := posInLine + 1; i < 16; i++ {
				sb.WriteByte(' ')
			}
			sb.WriteString(" |\n")

			line++
		}

		pos++
	}

	// Footer
	sb.WriteString("+------+--------------------------------------------------+------------------+\n")

	return sb.String()
}

// NullLogger is a no-op logger
type NullLogger struct{}

func (l *NullLogger) LogSend(data []byte, sequence uint8)    {}
func (l *NullLogger) LogReceive(data []byte, sequence uint8) {}
func (l *NullLogger) IsEnabled() bool                        { return false }

// Global logger instance
var globalLogger Logger = &NullLogger{}

// SetLogger sets the global logger
func SetLogger(logger Logger) {
	if logger == nil {
		globalLogger = &NullLogger{}
	} else {
		globalLogger = logger
	}
}

// GetLogger returns the global logger
func GetLogger() Logger {
	return globalLogger
}
