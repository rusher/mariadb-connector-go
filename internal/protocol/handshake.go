// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package protocol

import (
	"bytes"
	"crypto/sha1"
	"fmt"
)

// HandshakePacket represents the initial handshake from server
type HandshakePacket struct {
	ProtocolVersion    byte
	ServerVersion      string
	ConnectionID       uint32
	Salt               []byte
	ServerCapabilities uint64 // uint64 to support MariaDB extended capabilities
	Charset            byte
	ServerStatus       uint16
	AuthPluginName     string
}

// ParseHandshakePacket parses the initial handshake packet from server
func ParseHandshakePacket(data []byte) (*HandshakePacket, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("handshake packet too short")
	}

	packet := &HandshakePacket{}
	pos := 0

	// Protocol version
	packet.ProtocolVersion = data[pos]
	pos++

	// Server version (null-terminated)
	serverVersion, newPos, err := ReadNullTerminatedString(data, pos)
	if err != nil {
		return nil, fmt.Errorf("failed to read server version: %w", err)
	}
	packet.ServerVersion = serverVersion
	pos = newPos

	// Connection ID (4 bytes)
	if pos+4 > len(data) {
		return nil, fmt.Errorf("invalid connection ID")
	}
	packet.ConnectionID = GetUint32(data[pos:])
	pos += 4

	// Auth plugin data part 1 (8 bytes)
	if pos+8 > len(data) {
		return nil, fmt.Errorf("invalid auth plugin data")
	}
	packet.Salt = make([]byte, 8)
	copy(packet.Salt, data[pos:pos+8])
	pos += 8

	// Filler (1 byte)
	pos++

	// Capability flags (lower 2 bytes)
	if pos+2 > len(data) {
		return nil, fmt.Errorf("invalid capability flags")
	}
	packet.ServerCapabilities = uint64(GetUint16(data[pos:]))
	pos += 2

	// Check if we have more data (protocol 4.1+)
	if pos < len(data) {
		// Character set
		packet.Charset = data[pos]
		pos++

		// Server status
		if pos+2 <= len(data) {
			packet.ServerStatus = GetUint16(data[pos:])
			pos += 2

			// Extended capability flags (upper 2 bytes)
			if pos+2 <= len(data) {
				packet.ServerCapabilities |= uint64(GetUint16(data[pos:])) << 16
				pos += 2

				// Auth plugin data length
				var authPluginDataLen byte
				if pos < len(data) {
					authPluginDataLen = data[pos]
					pos++

					// Reserved (10 bytes)
					if pos+10 <= len(data) {
						pos += 10

						// Auth plugin data part 2
						if authPluginDataLen > 8 {
							remaining := int(authPluginDataLen) - 8
							if pos+remaining <= len(data) {
								salt2 := make([]byte, remaining)
								copy(salt2, data[pos:pos+remaining])
								packet.Salt = append(packet.Salt, salt2...)
								pos += remaining
							}
						}

						// Auth plugin name (null-terminated)
						if pos < len(data) {
							authPluginName, _, err := ReadNullTerminatedString(data, pos)
							if err == nil {
								packet.AuthPluginName = authPluginName
							}
						}
					}
				}
			}
		}
	}

	// Default auth plugin if not specified
	if packet.AuthPluginName == "" {
		packet.AuthPluginName = "mysql_native_password"
	}

	return packet, nil
}

// Config interface for handshake - we need to extract these fields
type HandshakeConfig interface {
	GetUser() string
	GetPassword() string
	GetDatabase() string
}

// BuildHandshakeResponse builds the handshake response packet
// Based on MariaDB Java connector HandshakeResponse.java
func BuildHandshakeResponse(config HandshakeConfig, handshake *HandshakePacket) ([]byte, error) {
	var buf bytes.Buffer

	// Determine client capabilities (uint64 for MariaDB extended capabilities)
	clientCaps := DefaultClientCapabilities

	// Add CONNECT_WITH_DB if database is specified
	if config.GetDatabase() != "" {
		clientCaps |= CONNECT_WITH_DB
	}

	// Ensure we support the auth plugin
	clientCaps |= PLUGIN_AUTH
	clientCaps |= SECURE_CONNECTION

	// Client capabilities (4 bytes) - lower 32 bits
	capBytes := make([]byte, 4)
	PutUint32(capBytes, uint32(clientCaps))
	buf.Write(capBytes)

	// Max packet size (4 bytes) - 1GB
	maxPacketBytes := make([]byte, 4)
	PutUint32(maxPacketBytes, 1024*1024*1024)
	buf.Write(maxPacketBytes)

	// Character set (1 byte) - utf8mb4
	buf.WriteByte(CHARSET_UTF8MB4)

	// Reserved (19 bytes)
	buf.Write(make([]byte, 19))

	// Extended client capabilities (4 bytes) - upper 32 bits (MariaDB extended)
	extCapBytes := make([]byte, 4)
	PutUint32(extCapBytes, uint32(clientCaps>>32))
	buf.Write(extCapBytes)

	// Username (null-terminated)
	username := config.GetUser()
	if username == "" {
		username = "root"
	}
	buf.WriteString(username)
	buf.WriteByte(0)

	// Generate auth response based on plugin
	password := config.GetPassword()
	var authResponse []byte

	if handshake.AuthPluginName == "mysql_clear_password" {
		// Clear password - just send the password as-is
		authResponse = []byte(password)
	} else {
		// Default to mysql_native_password
		authResponse = generateAuthResponse(password, handshake.Salt)
	}

	// Write auth response with length
	if handshake.ServerCapabilities&CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
		// Length-encoded auth data
		buf.Write(WriteLengthEncodedInteger(nil, uint64(len(authResponse))))
		buf.Write(authResponse)
	} else if handshake.ServerCapabilities&CLIENT_SECURE_CONNECTION != 0 {
		// 1-byte length + auth data
		buf.WriteByte(byte(len(authResponse)))
		buf.Write(authResponse)
	} else {
		// Old protocol - null-terminated
		buf.Write(authResponse)
		buf.WriteByte(0)
	}

	// Database name (null-terminated) if CLIENT_CONNECT_WITH_DB
	if clientCaps&CLIENT_CONNECT_WITH_DB != 0 {
		buf.WriteString(config.GetDatabase())
		buf.WriteByte(0)
	}

	// Auth plugin name (null-terminated) if CLIENT_PLUGIN_AUTH
	if handshake.ServerCapabilities&PLUGIN_AUTH != 0 {
		pluginName := handshake.AuthPluginName
		if pluginName == "" {
			pluginName = "mysql_native_password"
		}
		buf.WriteString(pluginName)
		buf.WriteByte(0)
	}

	// Connection attributes if CLIENT_CONNECT_ATTRS
	if clientCaps&CONNECT_ATTRS != 0 && handshake.ServerCapabilities&CONNECT_ATTRS != 0 {
		// Build connection attributes
		var attrBuf bytes.Buffer

		// _client_name
		attrBuf.Write(WriteLengthEncodedString(nil, "_client_name"))
		attrBuf.Write(WriteLengthEncodedString(nil, "mariadb-connector-go"))

		// _client_version
		attrBuf.Write(WriteLengthEncodedString(nil, "_client_version"))
		attrBuf.Write(WriteLengthEncodedString(nil, "1.0.0"))

		// _os
		attrBuf.Write(WriteLengthEncodedString(nil, "_os"))
		attrBuf.Write(WriteLengthEncodedString(nil, "Linux"))

		// _platform
		attrBuf.Write(WriteLengthEncodedString(nil, "_platform"))
		attrBuf.Write(WriteLengthEncodedString(nil, "x86_64"))

		// Write total length of attributes
		buf.Write(WriteLengthEncodedInteger(nil, uint64(attrBuf.Len())))
		buf.Write(attrBuf.Bytes())
	}

	return buf.Bytes(), nil
}

// generateAuthResponse generates authentication response for mysql_native_password
// Based on MariaDB Java connector NativePasswordPlugin.encryptPassword
// Protocol:
// 1. Server sends a random array of bytes (the seed)
// 2. Client makes a sha1 digest of the password
// 3. Client hashes the output of step 2
// 4. Client digests the seed
// 5. Client updates the digest with the output from step 3
// 6. XOR of the output of step 5 and step 2 is sent to server
func generateAuthResponse(password string, salt []byte) []byte {
	if password == "" {
		return []byte{}
	}

	// Truncate salt to first 20 bytes (remove trailing null byte if present)
	truncatedSalt := salt
	if len(salt) > 20 {
		truncatedSalt = salt[:20]
	}
	// Remove trailing null byte
	for len(truncatedSalt) > 0 && truncatedSalt[len(truncatedSalt)-1] == 0 {
		truncatedSalt = truncatedSalt[:len(truncatedSalt)-1]
	}

	// Stage 1: SHA1(password)
	hash1 := sha1.Sum([]byte(password))

	// Stage 2: SHA1(SHA1(password))
	hash2 := sha1.Sum(hash1[:])

	// Stage 3: SHA1(salt + SHA1(SHA1(password)))
	h := sha1.New()
	h.Write(truncatedSalt)
	h.Write(hash2[:])
	hash3 := h.Sum(nil)

	// XOR stage1 and stage3
	response := make([]byte, 20)
	for i := 0; i < 20; i++ {
		response[i] = hash1[i] ^ hash3[i]
	}

	return response
}

// ParseAuthResult parses the authentication result packet
func ParseAuthResult(data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("empty auth result")
	}

	switch data[0] {
	case 0x00:
		// OK packet - authentication successful
		return nil

	case 0xfe:
		// Auth switch request or additional auth data
		return fmt.Errorf("auth switch not yet implemented")

	case 0xff:
		// Error packet
		return ParseErrorPacket(data)

	default:
		return fmt.Errorf("unexpected auth result packet type: 0x%x", data[0])
	}
}
