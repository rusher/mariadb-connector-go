// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"log"
	"os"
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
