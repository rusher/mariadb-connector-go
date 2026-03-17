// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package handshake

import (
	"bytes"
	"fmt"

	authpkg "github.com/mariadb-connector-go/mariadb/internal/client/auth"
	"github.com/mariadb-connector-go/mariadb/internal/protocol"
)

const (
	iOK           = 0x00
	iAuthMoreData = 0x01
	iEOF          = 0xfe
	iERR          = 0xff
	authMaxSwitch = 10
)

// handleAuthResult processes the initial authentication packet and manages subsequent
// authentication flow. It reads the first authentication packet and hands off processing
// to the appropriate auth plugin.
func handleAuthResult(reader *protocol.PacketReader, writer *protocol.PacketWriter, ctx protocol.ContextUpdater, config authpkg.PluginConfig, data []byte, initialSeed []byte, initialPluginName string, remainingSwitch int) error {
	if remainingSwitch == 0 {
		return fmt.Errorf("maximum of %d authentication switches reached", authMaxSwitch)
	}

	if len(data) == 0 {
		return fmt.Errorf("empty auth response packet")
	}

	for {
		switch data[0] {
		case iOK:
			_, err := protocol.ParseOkPacket(data, ctx)
			return err

		case iERR:
			return protocol.ParseErrorPacket(data)

		case iEOF:
			pluginName, authData, err := parseAuthSwitchData(data, initialSeed)
			if err != nil {
				return fmt.Errorf("failed to parse auth switch data: %w", err)
			}

			authPlugin, exists := authpkg.GetAuthPlugin(pluginName)
			if !exists {
				return fmt.Errorf("authentication plugin '%s' is not supported", pluginName)
			}

			initialAuthResponse, err := authPlugin.InitAuth(authData, config)
			if err != nil {
				return fmt.Errorf("plugin InitAuth failed: %w", err)
			}

			if err := writer.WritePacket(initialAuthResponse); err != nil {
				return fmt.Errorf("failed to send auth switch response: %w", err)
			}

			data, err = reader.ReadScratch()
			if err != nil {
				return fmt.Errorf("failed to read auth response: %w", err)
			}
			if len(data) == 0 {
				return fmt.Errorf("empty auth response packet")
			}

			return handleAuthResult(reader, writer, ctx, config, data, authData, pluginName, remainingSwitch-1)
		}

		authPlugin, exists := authpkg.GetAuthPlugin(initialPluginName)
		if !exists {
			return fmt.Errorf("authentication plugin '%s' is not supported", initialPluginName)
		}

		// Strip iAuthMoreData prefix if present (see comment in original code)
		pluginData := data
		if len(data) > 0 && data[0] == iAuthMoreData {
			pluginData = data[1:]
		}

		nextPacket, done, err := authPlugin.ContinuationAuth(pluginData, initialSeed, config)
		if err != nil {
			return fmt.Errorf("plugin ContinuationAuth failed: %w", err)
		}

		if nextPacket != nil {
			if err := writer.WritePacket(nextPacket); err != nil {
				return fmt.Errorf("failed to send continuation auth packet: %w", err)
			}
			data, err = reader.ReadScratch()
			if err != nil {
				return fmt.Errorf("failed to read auth response: %w", err)
			}
			if len(data) == 0 {
				return fmt.Errorf("empty auth response packet")
			}
			continue
		}

		if done {
			return fmt.Errorf("plugin signaled done but no terminal packet received")
		}

		data, err = reader.ReadScratch()
		if err != nil {
			return fmt.Errorf("failed to read auth response: %w", err)
		}
		if len(data) == 0 {
			return fmt.Errorf("empty auth response packet")
		}
	}
}

// parseAuthSwitchData extracts the plugin name and seed from an auth-switch packet.
func parseAuthSwitchData(data []byte, initialSeed []byte) (string, []byte, error) {
	if len(data) == 1 {
		return "mysql_old_password", initialSeed, nil
	}

	pluginEndIndex := bytes.IndexByte(data[1:], 0x00)
	if pluginEndIndex < 0 {
		return "", nil, fmt.Errorf("invalid auth switch packet: no null terminator")
	}

	pluginName := string(data[1 : 1+pluginEndIndex])
	authData := data[1+pluginEndIndex+1:]

	if len(authData) > 0 && authData[len(authData)-1] == 0 {
		authData = authData[:len(authData)-1]
	}

	saved := make([]byte, len(authData))
	copy(saved, authData)

	return pluginName, saved, nil
}
