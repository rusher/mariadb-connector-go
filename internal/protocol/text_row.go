// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"fmt"
	"strconv"
	"time"
	"unsafe"
)

// ParseTextRow parses a row in text protocol, returning typed Go values.
func ParseTextRow(data []byte, columns []ColumnDefinition) ([]interface{}, error) {
	values := make([]interface{}, len(columns))
	pos := 0

	for i := range columns {
		if data[pos] == 0xfb {
			values[i] = nil
			pos++
			continue
		}

		vLen, vStart, err := ReadLengthEncodedInteger(data, pos)
		if err != nil {
			return nil, fmt.Errorf("failed to read column %d: %w", i, err)
		}
		vEnd := vStart + int(vLen)
		pos = vEnd

		raw := data[vStart:vEnd]
		col := &columns[i]
		switch col.Type {
		case MYSQL_TYPE_TINY:
			if col.Length == 1 {
				values[i] = vLen > 0 && raw[0] != '0'
			} else {
				values[i] = textInt64(raw)
			}
		case MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_INT24, MYSQL_TYPE_LONGLONG, MYSQL_TYPE_YEAR:
			values[i] = textInt64(raw)
		case MYSQL_TYPE_FLOAT, MYSQL_TYPE_DOUBLE:
			values[i] = textFloat64(raw)
		case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
			values[i] = string(raw)
		case MYSQL_TYPE_DATE, MYSQL_TYPE_NEWDATE:
			values[i] = textDate(raw)
		case MYSQL_TYPE_DATETIME, MYSQL_TYPE_TIMESTAMP:
			values[i] = textDatetime(raw)
		case MYSQL_TYPE_TIME:
			values[i] = textDuration(raw)
		case MYSQL_TYPE_JSON:
			values[i] = string(raw)
		default: // VARCHAR, BLOB, BIT, ENUM, SET, GEOMETRY, …
			if col.Charset == 63 { // binary charset → []byte (zero-copy)
				values[i] = raw
			} else {
				values[i] = string(raw)
			}
		}
	}

	return values, nil
}

// textInt64 parses a decimal integer (optionally signed) from ASCII bytes.
func textInt64(raw []byte) int64 {
	if len(raw) == 0 {
		return 0
	}
	neg := raw[0] == '-'
	start := 0
	if neg {
		start = 1
	}
	var v int64
	for _, b := range raw[start:] {
		v = v*10 + int64(b-'0')
	}
	if neg {
		return -v
	}
	return v
}

// textFloat64 parses a floating-point value from ASCII bytes (zero-copy).
func textFloat64(raw []byte) float64 {
	f, _ := strconv.ParseFloat(unsafe.String(unsafe.SliceData(raw), len(raw)), 64)
	return f
}

// textDate parses "YYYY-MM-DD" → time.Time or nil for zero dates.
func textDate(raw []byte) interface{} {
	if len(raw) < 10 {
		return nil
	}
	y := textDig4(raw, 0)
	mo := textDig2(raw, 5)
	d := textDig2(raw, 8)
	if y == 0 && mo == 0 && d == 0 {
		return nil
	}
	return time.Date(y, time.Month(mo), d, 0, 0, 0, 0, time.UTC)
}

// textDatetime parses "YYYY-MM-DD HH:MM:SS[.micro]" → time.Time or nil.
func textDatetime(raw []byte) interface{} {
	if len(raw) < 19 {
		return nil
	}
	y := textDig4(raw, 0)
	mo := textDig2(raw, 5)
	d := textDig2(raw, 8)
	h := textDig2(raw, 11)
	mi := textDig2(raw, 14)
	s := textDig2(raw, 17)
	ns := 0
	if len(raw) > 20 && raw[19] == '.' {
		frac := raw[20:]
		micro := 0
		for k, b := range frac {
			if k >= 6 {
				break
			}
			micro = micro*10 + int(b-'0')
		}
		for k := len(frac); k < 6; k++ {
			micro *= 10
		}
		ns = micro * 1000
	}
	if y == 0 && mo == 0 && d == 0 {
		return nil
	}
	return time.Date(y, time.Month(mo), d, h, mi, s, ns, time.UTC)
}

// textDuration parses "[-]H+:MM:SS[.micro]" → time.Duration.
func textDuration(raw []byte) time.Duration {
	if len(raw) == 0 {
		return 0
	}
	neg := raw[0] == '-'
	s := raw
	if neg {
		s = raw[1:]
	}
	// hours can exceed 2 digits (max TIME is 838:59:59)
	colon := 0
	for colon < len(s) && s[colon] != ':' {
		colon++
	}
	if len(s) < colon+6 {
		return 0
	}
	h := textInt64(s[:colon])
	mi := int64(textDig2(s, colon+1))
	se := int64(textDig2(s, colon+4))
	var micros int64
	if len(s) > colon+6 && s[colon+6] == '.' {
		frac := s[colon+7:]
		for k, b := range frac {
			if k >= 6 {
				break
			}
			micros = micros*10 + int64(b-'0')
		}
		for k := len(frac); k < 6; k++ {
			micros *= 10
		}
	}
	dur := time.Duration(h)*time.Hour +
		time.Duration(mi)*time.Minute +
		time.Duration(se)*time.Second +
		time.Duration(micros)*time.Microsecond
	if neg {
		return -dur
	}
	return dur
}

// textDig4 reads a 4-digit decimal integer at offset off (no bounds check).
func textDig4(b []byte, off int) int {
	return int(b[off]-'0')*1000 + int(b[off+1]-'0')*100 + int(b[off+2]-'0')*10 + int(b[off+3]-'0')
}

// textDig2 reads a 2-digit decimal integer at offset off (no bounds check).
func textDig2(b []byte, off int) int {
	return int(b[off]-'0')*10 + int(b[off+1]-'0')
}
