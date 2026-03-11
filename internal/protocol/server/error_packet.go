// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package server

import "fmt"

// MySQLError represents a MySQL/MariaDB server error
type MySQLError struct {
	Code     uint16
	SQLState string
	Message  string
}

// Error implements the error interface
func (e *MySQLError) Error() string {
	if e.SQLState != "" {
		return fmt.Sprintf("Error %d (%s): %s", e.Code, e.SQLState, e.Message)
	}
	return fmt.Sprintf("Error %d: %s", e.Code, e.Message)
}

// ParseErrorPacket parses an error packet from raw bytes and returns it as an error
func ParseErrorPacket(data []byte) error {
	if len(data) < 3 {
		return fmt.Errorf("error packet too short")
	}

	if data[0] != 0xff {
		return fmt.Errorf("not an error packet")
	}

	pos := 1

	errorCode := uint16(data[pos]) | uint16(data[pos+1])<<8
	pos += 2

	sqlState := "HY000"
	if pos < len(data) && data[pos] == '#' {
		pos++
		if pos+5 <= len(data) {
			sqlState = string(data[pos : pos+5])
			pos += 5
		}
	}

	message := ""
	if pos < len(data) {
		message = string(data[pos:])
	}

	return &MySQLError{
		Code:     errorCode,
		SQLState: sqlState,
		Message:  message,
	}
}
