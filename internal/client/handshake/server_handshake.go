// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package handshake

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"fmt"

	"github.com/mariadb-connector-go/mariadb/internal/protocol"
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
	_ = data[3]

	packet := &HandshakePacket{}
	pos := 0

	packet.ProtocolVersion = data[pos]
	pos++

	serverVersion, newPos, err := protocol.ReadNullTerminatedString(data, pos)
	if err != nil {
		return nil, fmt.Errorf("failed to read server version: %w", err)
	}
	packet.ServerVersion = serverVersion
	pos = newPos

	_ = data[pos+3]
	packet.ConnectionID = binary.LittleEndian.Uint32(data[pos:])
	pos += 4

	_ = data[pos+7]
	packet.Salt = make([]byte, 8)
	copy(packet.Salt, data[pos:pos+8])
	pos += 8

	pos++ // filler

	_ = data[pos+1]
	packet.ServerCapabilities = uint64(binary.LittleEndian.Uint16(data[pos:]))
	pos += 2

	if pos < len(data) {
		packet.Charset = data[pos]
		pos++

		if pos+2 <= len(data) {
			packet.ServerStatus = binary.LittleEndian.Uint16(data[pos:])
			pos += 2

			if pos+2 <= len(data) {
				serverCapabilities4Bytes := uint32(packet.ServerCapabilities) | (uint32(binary.LittleEndian.Uint16(data[pos:])) << 16)
				pos += 2

				var saltLength int
				if pos < len(data) {
					if serverCapabilities4Bytes&protocol.PLUGIN_AUTH != 0 {
						authPluginDataLen := int(data[pos])
						saltLength = authPluginDataLen - 9
						if saltLength < 12 {
							saltLength = 12
						}
					}
					pos++

					if pos+6 <= len(data) {
						pos += 6 // reserved

						var mariaDBAdditionalCaps uint32
						if pos+4 <= len(data) {
							mariaDBAdditionalCaps = binary.LittleEndian.Uint32(data[pos:])
							pos += 4
						}

						if serverCapabilities4Bytes&protocol.SECURE_CONNECTION != 0 {
							if saltLength > 0 && pos+saltLength <= len(data) {
								salt2 := make([]byte, saltLength)
								copy(salt2, data[pos:pos+saltLength])
								packet.Salt = append(packet.Salt, salt2...)
								pos += saltLength
							} else if saltLength == 0 && pos < len(data) {
								salt2, newPos, err := protocol.ReadNullTerminatedString(data, pos)
								if err == nil {
									packet.Salt = append(packet.Salt, []byte(salt2)...)
									pos = newPos
								}
							}
						}
						if pos < len(data) {
							pos++ // skip 1 byte after salt
						}

						if serverCapabilities4Bytes&protocol.CLIENT_MYSQL == 0 {
							packet.ServerCapabilities = uint64(serverCapabilities4Bytes) | (uint64(mariaDBAdditionalCaps) << 32)
						} else {
							packet.ServerCapabilities = uint64(serverCapabilities4Bytes)
						}

						if pos < len(data) && serverCapabilities4Bytes&protocol.PLUGIN_AUTH != 0 {
							authPluginName, _, err := protocol.ReadNullTerminatedString(data, pos)
							if err == nil {
								packet.AuthPluginName = authPluginName
							}
						}
					}
				}
			}
		}
	}

	if packet.AuthPluginName == "" {
		packet.AuthPluginName = "mysql_native_password"
	}

	return packet, nil
}

// HandshakeConfig interface for handshake
type HandshakeConfig interface {
	GetUser() string
	GetPassword() string
	GetDatabase() string
}

// BuildHandshakeResponse builds the handshake response packet
func BuildHandshakeResponse(config HandshakeConfig, handshake *HandshakePacket, clientCaps uint64) ([]byte, error) {
	var buf bytes.Buffer

	capBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(capBytes, uint32(clientCaps))
	buf.Write(capBytes)

	maxPacketBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(maxPacketBytes, 1024*1024*1024)
	buf.Write(maxPacketBytes)

	buf.WriteByte(protocol.CHARSET_UTF8MB4)
	buf.Write(make([]byte, 19)) // reserved

	extCapBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(extCapBytes, uint32(clientCaps>>32))
	buf.Write(extCapBytes)

	username := config.GetUser()
	if username == "" {
		username = "root"
	}
	buf.WriteString(username)
	buf.WriteByte(0)

	password := config.GetPassword()
	var authResponse []byte
	if handshake.AuthPluginName == "mysql_clear_password" {
		authResponse = []byte(password)
	} else {
		authResponse = generateAuthResponse(password, handshake.Salt)
	}

	if handshake.ServerCapabilities&protocol.CLIENT_PLUGIN_AUTH_LENENC_CLIENT_DATA != 0 {
		buf.Write(protocol.WriteLengthEncodedInteger(nil, uint64(len(authResponse))))
		buf.Write(authResponse)
	} else if handshake.ServerCapabilities&protocol.CLIENT_SECURE_CONNECTION != 0 {
		buf.WriteByte(byte(len(authResponse)))
		buf.Write(authResponse)
	} else {
		buf.Write(authResponse)
		buf.WriteByte(0)
	}

	if clientCaps&protocol.CLIENT_CONNECT_WITH_DB != 0 {
		buf.WriteString(config.GetDatabase())
		buf.WriteByte(0)
	}

	if handshake.ServerCapabilities&protocol.PLUGIN_AUTH != 0 {
		pluginName := handshake.AuthPluginName
		if pluginName == "" {
			pluginName = "mysql_native_password"
		}
		buf.WriteString(pluginName)
		buf.WriteByte(0)
	}

	if clientCaps&protocol.CONNECT_ATTRS != 0 && handshake.ServerCapabilities&protocol.CONNECT_ATTRS != 0 {
		var attrBuf bytes.Buffer
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "_client_name"))
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "mariadb-connector-go"))
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "_client_version"))
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "1.0.0"))
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "_os"))
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "Linux"))
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "_platform"))
		attrBuf.Write(protocol.WriteLengthEncodedString(nil, "x86_64"))
		buf.Write(protocol.WriteLengthEncodedInteger(nil, uint64(attrBuf.Len())))
		buf.Write(attrBuf.Bytes())
	}

	return buf.Bytes(), nil
}

// generateAuthResponse generates authentication response for mysql_native_password
func generateAuthResponse(password string, salt []byte) []byte {
	if password == "" {
		return []byte{}
	}

	truncatedSalt := salt
	if len(salt) > 20 {
		truncatedSalt = salt[:20]
	}
	for len(truncatedSalt) > 0 && truncatedSalt[len(truncatedSalt)-1] == 0 {
		truncatedSalt = truncatedSalt[:len(truncatedSalt)-1]
	}

	hash1 := sha1.Sum([]byte(password))
	hash2 := sha1.Sum(hash1[:])

	h := sha1.New()
	h.Write(truncatedSalt)
	h.Write(hash2[:])
	hash3 := h.Sum(nil)

	response := make([]byte, 20)
	for i := 0; i < 20; i++ {
		response[i] = hash1[i] ^ hash3[i]
	}
	return response
}
