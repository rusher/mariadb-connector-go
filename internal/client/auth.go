// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

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
func (c *Client) handleAuthResult(data []byte, initialSeed []byte, initialPluginName string, remainingSwitch int) error {
	if remainingSwitch == 0 {
		return fmt.Errorf("maximum of %d authentication switches reached", authMaxSwitch)
	}

	if len(data) == 0 {
		return fmt.Errorf("empty auth response packet")
	}

	// Loop on continuationAuth until we receive a terminal packet (OK/ERR/EOF)
	for {
		// Check for terminal packets first
		switch data[0] {
		case iOK:
			// OK packet - authentication successful
			_, err := protocol.ParseOkPacket(data, c.context)
			return err

		case iERR:
			// Error packet
			return protocol.ParseErrorPacket(data)

		case iEOF:
			// Auth switch request
			pluginName, authData, err := c.parseAuthSwitchData(data, initialSeed)
			if err != nil {
				return fmt.Errorf("failed to parse auth switch data: %w", err)
			}

			// Get the authentication plugin from static registry
			authPlugin, exists := authpkg.GetAuthPlugin(pluginName)
			if !exists {
				return fmt.Errorf("authentication plugin '%s' is not supported", pluginName)
			}

			// Initialize authentication with the plugin
			initialAuthResponse, err := authPlugin.InitAuth(authData, c.config)
			if err != nil {
				return fmt.Errorf("plugin InitAuth failed: %w", err)
			}

			// Send the initial auth response
			if err := c.writeAuthSwitchPacket(initialAuthResponse); err != nil {
				return fmt.Errorf("failed to send auth switch response: %w", err)
			}

			// Read next packet
			data, err = c.reader.ReadPacket()
			if err != nil {
				return fmt.Errorf("failed to read auth response: %w", err)
			}
			if len(data) == 0 {
				return fmt.Errorf("empty auth response packet")
			}

			// Recursively handle the result with the new plugin
			return c.handleAuthResult(data, authData, pluginName, remainingSwitch-1)
		}

		// Not a terminal packet, let the plugin process it
		// Get the current authentication plugin
		authPlugin, exists := authpkg.GetAuthPlugin(initialPluginName)
		if !exists {
			return fmt.Errorf("authentication plugin '%s' is not supported", initialPluginName)
		}

		// If the packet starts with 0x01 (iAuthMoreData), strip it and pass the remaining data
		// MySQL is prepending all authentication with 0x01 since 8.0,
		// MariaDB was prepending only when required (when data begins with OK/EOF/ERR),
		// and since 11.4.9, 11.8.4, always add 0x01
		pluginData := data
		if len(data) > 0 && data[0] == iAuthMoreData {
			pluginData = data[1:]
		}

		nextPacket, done, err := authPlugin.ContinuationAuth(pluginData, initialSeed, c.config)
		if err != nil {
			return fmt.Errorf("plugin ContinuationAuth failed: %w", err)
		}

		// If plugin returned a packet to send, send it and read the response
		if nextPacket != nil {
			if err := c.writeAuthSwitchPacket(nextPacket); err != nil {
				return fmt.Errorf("failed to send continuation auth packet: %w", err)
			}
			data, err = c.reader.ReadPacket()
			if err != nil {
				return fmt.Errorf("failed to read auth response: %w", err)
			}
			if len(data) == 0 {
				return fmt.Errorf("empty auth response packet")
			}
			continue
		}

		// If plugin signals done but we haven't hit a terminal packet, that's an error
		if done {
			return fmt.Errorf("plugin signaled done but no terminal packet received")
		}

		// Plugin wants to read the next packet
		data, err = c.reader.ReadPacket()
		if err != nil {
			return fmt.Errorf("failed to read auth response: %w", err)
		}
		if len(data) == 0 {
			return fmt.Errorf("empty auth response packet")
		}
	}
}

// parseAuthSwitchData extracts the authentication plugin name and associated data
// from an authentication switch request packet.
func (c *Client) parseAuthSwitchData(data []byte, initialSeed []byte) (string, []byte, error) {
	if len(data) == 1 {
		// Special case for the old authentication protocol
		return "mysql_old_password", initialSeed, nil
	}

	pluginEndIndex := bytes.IndexByte(data[1:], 0x00)
	if pluginEndIndex < 0 {
		return "", nil, fmt.Errorf("invalid auth switch packet: no null terminator")
	}

	pluginName := string(data[1 : 1+pluginEndIndex])
	authData := data[1+pluginEndIndex+1:]

	// Remove trailing null byte if present
	if len(authData) > 0 && authData[len(authData)-1] == 0 {
		authData = authData[:len(authData)-1]
	}

	// Make a copy to avoid issues with buffer reuse
	savedAuthData := make([]byte, len(authData))
	copy(savedAuthData, authData)

	return pluginName, savedAuthData, nil
}

// writeAuthSwitchPacket sends an authentication switch response packet
func (c *Client) writeAuthSwitchPacket(data []byte) error {
	return c.writer.WritePacket(data)
}
