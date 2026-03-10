// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"database/sql/driver"
	"fmt"
)

// Result implements driver.Result interface
type Result struct {
	affectedRows int64
	lastInsertID int64
}

// LastInsertId returns the last insert ID
func (r *Result) LastInsertId() (int64, error) {
	return r.lastInsertID, nil
}

// RowsAffected returns the number of rows affected
func (r *Result) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}

// ParseResultPacket parses an OK or ERR packet
func ParseResultPacket(data []byte) (driver.Result, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("empty result packet")
	}

	switch data[0] {
	case 0x00:
		// OK packet
		return parseOKPacket(data)

	case 0xff:
		// Error packet
		return nil, ParseErrorPacket(data)

	default:
		return nil, fmt.Errorf("unexpected result packet type: 0x%x", data[0])
	}
}

// parseOKPacket parses an OK packet
func parseOKPacket(data []byte) (*Result, error) {
	if len(data) < 7 {
		return nil, fmt.Errorf("OK packet too short")
	}

	pos := 1 // Skip header byte (0x00)

	// Affected rows (length-encoded integer)
	affectedRows, newPos, err := ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return nil, fmt.Errorf("failed to read affected rows: %w", err)
	}
	pos = newPos

	// Last insert ID (length-encoded integer)
	lastInsertID, newPos, err := ReadLengthEncodedInteger(data, pos)
	if err != nil {
		return nil, fmt.Errorf("failed to read last insert ID: %w", err)
	}

	return &Result{
		affectedRows: int64(affectedRows),
		lastInsertID: int64(lastInsertID),
	}, nil
}

// ParseErrorPacket parses an error packet
func ParseErrorPacket(data []byte) error {
	if len(data) < 3 {
		return fmt.Errorf("error packet too short")
	}

	if data[0] != 0xff {
		return fmt.Errorf("not an error packet")
	}

	pos := 1

	// Error code (2 bytes)
	errorCode := GetUint16(data[pos:])
	pos += 2

	var sqlState string
	var message string

	// Check for SQL state marker '#'
	if pos < len(data) && data[pos] == '#' {
		pos++ // Skip '#'
		if pos+5 <= len(data) {
			sqlState = string(data[pos : pos+5])
			pos += 5
		}
	}

	// Error message
	if pos < len(data) {
		message = string(data[pos:])
	}

	return &MySQLError{
		Code:     errorCode,
		SQLState: sqlState,
		Message:  message,
	}
}

// MySQLError represents a MySQL error
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
