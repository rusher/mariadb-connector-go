// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

// Package handshake implements the MySQL/MariaDB connection handshake and
// authentication exchange. It is independent of the client package: all
// connection-specific state is passed in via interfaces and callbacks so that
// no import cycle is introduced.
package handshake

import (
	"fmt"

	authpkg "github.com/mariadb-connector-go/mariadb/internal/client/auth"
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

// Config is the union of all configuration interfaces required during the
// handshake. *client.Config satisfies this interface.
type Config interface {
	CapabilityConfig      // capability negotiation
	HandshakeConfig       // user / password / database
	authpkg.PluginConfig  // auth plugin fields
	GetCharset() string   // for SET NAMES
	GetCollation() string // for SET NAMES … COLLATE (empty = omit)
	ValidateCharset() error
}

// ContextInitializer is called once the server handshake packet has been parsed
// and client capabilities have been negotiated. The returned ContextUpdater is
// used for all subsequent OK/EOF packet parsing during the handshake.
type ContextInitializer func(handshake *HandshakePacket, clientCaps uint64) protocol.ContextUpdater

// Perform runs the full initial handshake:
//  1. Reads and parses the server's initial handshake packet.
//  2. Negotiates client capabilities and calls initCtx to let the caller
//     create its connection context.
//  3. Sends the handshake response.
//  4. Runs the authentication exchange (including plugin switches).
//  5. Sends SET NAMES to configure the connection charset.
//
// Must be called immediately after a fresh TCP/Unix connection is established,
// before any other packets are exchanged.
func Perform(
	reader *protocol.PacketReader,
	writer *protocol.PacketWriter,
	sequence *uint8,
	config Config,
	initCtx ContextInitializer,
) error {
	// 1. Read and parse the server's initial handshake packet.
	data, err := reader.ReadScratch()
	if err != nil {
		return fmt.Errorf("failed to read handshake packet: %w", err)
	}

	handshake, err := ParseHandshakePacket(data)
	if err != nil {
		return fmt.Errorf("failed to parse handshake packet: %w", err)
	}

	// 2. Negotiate capabilities and initialise the caller's context.
	clientCaps := InitializeClientCapabilities(config, handshake.ServerCapabilities, config.GetDatabase())
	ctx := initCtx(handshake, clientCaps)

	// 3. Build and send the handshake response (sequence 1).
	response, err := BuildHandshakeResponse(config, handshake, clientCaps)
	if err != nil {
		return fmt.Errorf("failed to build handshake response: %w", err)
	}
	if err := writer.WritePacket(response); err != nil {
		return fmt.Errorf("failed to send handshake response: %w", err)
	}

	// 4. Read and handle the authentication result.
	authResult, err := reader.ReadScratch()
	if err != nil {
		return fmt.Errorf("failed to read auth result: %w", err)
	}
	if err := handleAuthResult(reader, writer, ctx, config, authResult, handshake.Salt, handshake.AuthPluginName, authMaxSwitch); err != nil {
		return fmt.Errorf("authentication failed: %w", err)
	}

	// 5. Reset sequence and configure charset.
	*sequence = 0
	if err := config.ValidateCharset(); err != nil {
		return fmt.Errorf("invalid charset: %w", err)
	}
	if err := setCharset(reader, writer, sequence, config, ctx); err != nil {
		return fmt.Errorf("failed to set charset: %w", err)
	}

	return nil
}

// setCharset sends SET NAMES (with optional COLLATE) to configure the charset.
func setCharset(
	reader *protocol.PacketReader,
	writer *protocol.PacketWriter,
	sequence *uint8,
	config Config,
	ctx protocol.ContextUpdater,
) error {
	var query string
	if col := config.GetCollation(); col != "" {
		query = fmt.Sprintf("SET NAMES %s COLLATE %s", config.GetCharset(), col)
	} else {
		query = fmt.Sprintf("SET NAMES %s", config.GetCharset())
	}

	*sequence = 0
	if err := writer.Write(protocol.NewQuery(writer.Buf(), query)); err != nil {
		return fmt.Errorf("failed to send SET NAMES query: %w", err)
	}

	data, err := reader.ReadScratch()
	if err != nil {
		return fmt.Errorf("failed to read SET NAMES response: %w", err)
	}
	if len(data) > 0 && data[0] == 0xff {
		return protocol.ParseErrorPacket(data)
	}
	if len(data) > 0 && data[0] == 0x00 {
		_, _ = protocol.ParseOkPacket(data, ctx)
	}
	return nil
}
