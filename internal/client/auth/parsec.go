// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha512"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// ParsecPlugin implements the parsec authentication plugin.
type ParsecPlugin struct{}

func (p *ParsecPlugin) PluginName() string { return "parsec" }

func (p *ParsecPlugin) InitAuth(_ []byte, _ PluginConfig) ([]byte, error) {
	return []byte{}, nil
}

func (p *ParsecPlugin) ContinuationAuth(packet []byte, authData []byte, cfg PluginConfig) ([]byte, bool, error) {
	if len(packet) < 2 {
		return nil, false, fmt.Errorf("parsec: invalid salt packet length")
	}
	pos := 0
	if packet[0] == 0x01 {
		pos++
	}
	if len(packet[pos:]) < 2 {
		return nil, false, fmt.Errorf("parsec: packet too short for algorithm and iterations")
	}
	firstByte := packet[pos]
	iterations := int(packet[pos+1])
	pos += 2
	if firstByte != 0x50 {
		return nil, false, fmt.Errorf("parsec: expected 'P' for PBKDF2, got 0x%02x", firstByte)
	}
	if iterations > 20 {
		return nil, false, fmt.Errorf("parsec: iteration count too high: %d", iterations)
	}
	salt := packet[pos:]
	iterationCount := 1024 << iterations
	derivedKey := pbkdf2.Key([]byte(cfg.GetPasswd()), salt, iterationCount, 32, sha512.New)
	privateKey := ed25519.NewKeyFromSeed(derivedKey)
	clientScramble := make([]byte, 32)
	if _, err := rand.Read(clientScramble); err != nil {
		return nil, false, fmt.Errorf("parsec: failed to generate client scramble: %w", err)
	}
	message := append(authData, clientScramble...)
	signature := ed25519.Sign(privateKey, message)
	response := make([]byte, 0, 32+64)
	response = append(response, clientScramble...)
	response = append(response, signature...)
	return response, true, nil
}
