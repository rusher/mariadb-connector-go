// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"fmt"
	"strings"
)

// hexDump creates a hex dump string similar to MariaDB Java connector.
// Output format:
//
//	       +--------------------------------------------------+
//	       |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |
//	+------+--------------------------------------------------+------------------+
//	|000000| 5F 00 00 00 03 73 65 74  20 61 75 74 6F 63 6F 6D | _....set autocom |
//	+------+--------------------------------------------------+------------------+
func hexDump(data []byte, offset, length int) string {
	if len(data) == 0 {
		return ""
	}

	var sb strings.Builder

	sb.WriteString("       +--------------------------------------------------+\n")
	sb.WriteString("       |  0  1  2  3  4  5  6  7   8  9  a  b  c  d  e  f |\n")
	sb.WriteString("+------+--------------------------------------------------+------------------+\n")

	pos := offset
	line := 0
	hexChars := make([]byte, 16)
	asciiChars := make([]byte, 16)

	for pos < offset+length {
		if pos%16 == 0 {
			sb.WriteString(fmt.Sprintf("|%06X| ", line*16))
		}

		byteVal := data[pos]
		sb.WriteString(fmt.Sprintf("%02X ", byteVal))

		posInLine := pos % 16
		hexChars[posInLine] = byteVal
		if byteVal > 31 && byteVal < 127 {
			asciiChars[posInLine] = byteVal
		} else {
			asciiChars[posInLine] = '.'
		}

		if posInLine == 7 {
			sb.WriteString(" ")
		}

		if posInLine == 15 || pos == offset+length-1 {
			remaining := 15 - posInLine
			if remaining > 0 {
				for i := 0; i < remaining; i++ {
					sb.WriteString("   ")
					if posInLine+i+1 == 8 {
						sb.WriteString(" ")
					}
				}
			}

			sb.WriteString("| ")
			for i := 0; i <= posInLine; i++ {
				sb.WriteByte(asciiChars[i])
			}
			for i := posInLine + 1; i < 16; i++ {
				sb.WriteByte(' ')
			}
			sb.WriteString(" |\n")

			line++
		}

		pos++
	}

	sb.WriteString("+------+--------------------------------------------------+------------------+\n")
	return sb.String()
}
