// MariaDB Connector/Go - A MariaDB/MySQL-Driver for Go's database/sql package
//
// Copyright 2026 MariaDB Corporation Ab. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha512"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"golang.org/x/crypto/pbkdf2"
)

// authPluginConfig is the subset of connection config that authentication
// plugins need. This decouples plugins from the full *Config / *mysqlConn.
type authPluginConfig struct {
	passwd                  string
	allowNativePasswords    bool
	allowCleartextPasswords bool
	allowOldPasswords       bool
	allowPubKeyRetrieval    bool
	hasTLS                  bool
	isUnixSocket            bool
	pubKey                  *rsa.PublicKey
}

func newAuthPluginConfig(cfg *Config) authPluginConfig {
	return authPluginConfig{
		passwd:                  cfg.Passwd,
		allowNativePasswords:    cfg.AllowNativePasswords,
		allowCleartextPasswords: cfg.AllowCleartextPasswords,
		allowOldPasswords:       cfg.AllowOldPasswords,
		allowPubKeyRetrieval:    cfg.AllowPublicKeyRetrieval,
		hasTLS:                  cfg.TLS != nil,
		isUnixSocket:            cfg.Net == "unix",
		pubKey:                  cfg.pubKey,
	}
}

// AuthPlugin defines the interface for authentication plugins.
type AuthPlugin interface {
	// Name returns the MySQL protocol name of this plugin.
	Name() string

	// InitAuth produces the initial authentication response given the
	// server's auth challenge (authData) and the connection config.
	InitAuth(authData []byte, cfg authPluginConfig) ([]byte, error)

	// ContinuationAuth handles subsequent server packets during the
	// authentication handshake. It returns the next packet to send, whether
	// the authentication is done, and any error.
	// Plugins that do not need multi-round auth embed simpleAuth.
	ContinuationAuth(packet []byte, seed []byte, cfg authPluginConfig) (nextPacket []byte, done bool, err error)
}

// simpleAuth provides a default ContinuationAuth that signals completion.
// Plugins that only need InitAuth embed this.
type simpleAuth struct{}

func (s *simpleAuth) ContinuationAuth(_ []byte, _ []byte, _ authPluginConfig) ([]byte, bool, error) {
	return nil, true, nil
}

// ---------------------------------------------------------------------------
// Plugin: mysql_native_password
// ---------------------------------------------------------------------------

type nativePasswordPlugin struct{ simpleAuth }

func (p *nativePasswordPlugin) Name() string { return "mysql_native_password" }

func (p *nativePasswordPlugin) InitAuth(authData []byte, cfg authPluginConfig) ([]byte, error) {
	if !cfg.allowNativePasswords {
		return nil, ErrNativePassword
	}
	// https://dev.mysql.com/doc/dev/mysql-server/8.4.5/page_protocol_connection_phase_authentication_methods_native_password_authentication.html
	// Native password authentication only need and will need 20-byte challenge.
	authResp := scramblePassword(authData[:20], cfg.passwd)
	return authResp, nil
}

// ---------------------------------------------------------------------------
// Plugin: caching_sha2_password
// ---------------------------------------------------------------------------

type cachingSha2Plugin struct{ simpleAuth }

func (p *cachingSha2Plugin) Name() string { return "caching_sha2_password" }

func (p *cachingSha2Plugin) InitAuth(authData []byte, cfg authPluginConfig) ([]byte, error) {
	return scrambleSHA256Password(authData, cfg.passwd), nil
}

func (p *cachingSha2Plugin) ContinuationAuth(packet []byte, seed []byte, cfg authPluginConfig) ([]byte, bool, error) {
	if len(packet) == 0 {
		return nil, true, nil // auth successful
	}

	// Single-byte status packets from the server.
	if len(packet) == 1 {
		switch packet[0] {
		case cachingSha2PasswordFastAuthSuccess:
			// Server will follow up with an OK packet which the caller reads.
			return nil, true, nil

		case cachingSha2PasswordPerformFullAuthentication:
			if cfg.hasTLS || cfg.isUnixSocket {
				// Send cleartext password over secure channel.
				return append([]byte(cfg.passwd), 0), true, nil
			}
			if cfg.pubKey != nil {
				enc, err := encryptPassword(cfg.passwd, seed, cfg.pubKey)
				if err != nil {
					return nil, false, err
				}
				return enc, true, nil
			}
			if !cfg.allowPubKeyRetrieval {
				return nil, false, fmt.Errorf("caching_sha2_password requires either TLS or AllowPublicKeyRetrieval=true")
			}
			// Request public key from server.
			return []byte{cachingSha2PasswordRequestPublicKey}, false, nil

		default:
			return nil, false, ErrMalformPkt
		}
	}

	// Multi-byte packet: public key response. The caller's loop already
	// strips the iAuthMoreData (0x01) prefix, so packet contains raw PEM data.
	block, rest := pem.Decode(packet)
	if block == nil {
		return nil, false, fmt.Errorf("no pem data found, data: %s", rest)
	}
	pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, false, err
	}
	enc, err := encryptPassword(cfg.passwd, seed, pkix.(*rsa.PublicKey))
	if err != nil {
		return nil, false, err
	}
	return enc, true, nil
}

// ---------------------------------------------------------------------------
// Plugin: sha256_password
// ---------------------------------------------------------------------------

type sha256Plugin struct{ simpleAuth }

func (p *sha256Plugin) Name() string { return "sha256_password" }

func (p *sha256Plugin) InitAuth(authData []byte, cfg authPluginConfig) ([]byte, error) {
	if len(cfg.passwd) == 0 {
		return []byte{0}, nil
	}
	// unlike caching_sha2_password, sha256_password does not accept
	// cleartext password on unix transport.
	if cfg.hasTLS {
		// write cleartext auth packet
		return append([]byte(cfg.passwd), 0), nil
	}
	if cfg.pubKey != nil {
		return encryptPassword(cfg.passwd, authData, cfg.pubKey)
	}
	// request public key from server
	return []byte{1}, nil
}

func (p *sha256Plugin) ContinuationAuth(packet []byte, seed []byte, cfg authPluginConfig) ([]byte, bool, error) {
	if len(packet) == 0 {
		return nil, true, nil // auth successful
	}
	block, _ := pem.Decode(packet)
	if block == nil {
		return nil, false, fmt.Errorf("no Pem data found, data: %s", packet)
	}
	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, false, err
	}
	enc, err := encryptPassword(cfg.passwd, seed, pub.(*rsa.PublicKey))
	if err != nil {
		return nil, false, err
	}
	return enc, true, nil
}

// ---------------------------------------------------------------------------
// Plugin: mysql_clear_password
// ---------------------------------------------------------------------------

type clearPasswordPlugin struct{ simpleAuth }

func (p *clearPasswordPlugin) Name() string { return "mysql_clear_password" }

func (p *clearPasswordPlugin) InitAuth(_ []byte, cfg authPluginConfig) ([]byte, error) {
	if !cfg.allowCleartextPasswords {
		return nil, ErrCleartextPassword
	}
	// http://dev.mysql.com/doc/refman/5.7/en/cleartext-authentication-plugin.html
	// http://dev.mysql.com/doc/refman/5.7/en/pam-authentication-plugin.html
	return append([]byte(cfg.passwd), 0), nil
}

// ---------------------------------------------------------------------------
// Plugin: mysql_old_password
// ---------------------------------------------------------------------------

type oldPasswordPlugin struct{ simpleAuth }

func (p *oldPasswordPlugin) Name() string { return "mysql_old_password" }

func (p *oldPasswordPlugin) InitAuth(authData []byte, cfg authPluginConfig) ([]byte, error) {
	if !cfg.allowOldPasswords {
		return nil, ErrOldPassword
	}
	if len(cfg.passwd) == 0 {
		return nil, nil
	}
	// Note: there are edge cases where this should work but doesn't;
	// this is currently "wontfix":
	// https://github.com/mariadb-corporation/mariadb-connector-go/issues/184
	return append(scrambleOldPassword(authData[:8], cfg.passwd), 0), nil
}

// ---------------------------------------------------------------------------
// Plugin: client_ed25519
// ---------------------------------------------------------------------------

type ed25519Plugin struct{ simpleAuth }

func (p *ed25519Plugin) Name() string { return "client_ed25519" }

func (p *ed25519Plugin) InitAuth(authData []byte, cfg authPluginConfig) ([]byte, error) {
	if len(authData) != 32 {
		return nil, ErrMalformPkt
	}
	return authEd25519(authData, cfg.passwd)
}

// ---------------------------------------------------------------------------
// Plugin: parsec (MariaDB 11.6+)
// ---------------------------------------------------------------------------

type parsecPlugin struct{}

func (p *parsecPlugin) Name() string { return "parsec" }

func (p *parsecPlugin) InitAuth(_ []byte, _ authPluginConfig) ([]byte, error) {
	// Parsec always begins with an empty response; the real work happens
	// in ContinuationAuth after the server sends the salt.
	return []byte{}, nil
}

func (p *parsecPlugin) ContinuationAuth(packet []byte, authData []byte, cfg authPluginConfig) ([]byte, bool, error) {
	if len(packet) < 2 {
		return nil, false, fmt.Errorf("parsec: invalid salt packet length")
	}
	pos := 0
	// Skip iAuthMoreData marker if present.
	if packet[0] == iAuthMoreData {
		pos++
	}
	if len(packet[pos:]) < 2 {
		return nil, false, fmt.Errorf("parsec: packet too short for algorithm and iterations")
	}
	algByte := packet[pos]
	iterations := int(packet[pos+1])
	pos += 2
	if algByte != 0x50 { // 'P' for PBKDF2
		return nil, false, fmt.Errorf("parsec: expected 'P' (0x50) for PBKDF2, got 0x%02x", algByte)
	}
	if iterations > 20 {
		return nil, false, fmt.Errorf("parsec: iteration count too high: %d", iterations)
	}
	salt := packet[pos:]
	iterationCount := 1024 << iterations

	derivedKey := pbkdf2.Key([]byte(cfg.passwd), salt, iterationCount, 32, sha512.New)
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

// ---------------------------------------------------------------------------
// Plugin: dialog (PAM)
// ---------------------------------------------------------------------------

type pamPlugin struct{}

func (p *pamPlugin) Name() string { return "dialog" }

func (p *pamPlugin) InitAuth(_ []byte, cfg authPluginConfig) ([]byte, error) {
	if !cfg.allowCleartextPasswords {
		return nil, ErrCleartextPassword
	}
	return append([]byte(cfg.passwd), 0), nil
}

func (p *pamPlugin) ContinuationAuth(packet []byte, _ []byte, cfg authPluginConfig) ([]byte, bool, error) {
	if len(packet) == 0 {
		return nil, false, fmt.Errorf("empty PAM packet")
	}
	// The dialog plugin sends prompts prefixed with a type byte.
	// We respond with the password for every prompt. The server
	// signals completion by sending an OK packet, which the caller's
	// loop handles — so we always return done=false here.
	return append([]byte(cfg.passwd), 0), false, nil
}

// ---------------------------------------------------------------------------
// Plugin: auth_gssapi_client (stub)
// ---------------------------------------------------------------------------

type gssapiPlugin struct{}

func (p *gssapiPlugin) Name() string { return "auth_gssapi_client" }

func (p *gssapiPlugin) InitAuth(_ []byte, _ authPluginConfig) ([]byte, error) {
	return nil, fmt.Errorf("GSSAPI authentication is not supported")
}

func (p *gssapiPlugin) ContinuationAuth(_ []byte, _ []byte, _ authPluginConfig) ([]byte, bool, error) {
	return nil, false, fmt.Errorf("GSSAPI authentication is not supported")
}

// ---------------------------------------------------------------------------
// Plugin Registry
// ---------------------------------------------------------------------------

// Static plugin singletons.
var (
	pluginNativePassword = &nativePasswordPlugin{}
	pluginCachingSha2    = &cachingSha2Plugin{}
	pluginSha256         = &sha256Plugin{}
	pluginClearPassword  = &clearPasswordPlugin{}
	pluginOldPassword    = &oldPasswordPlugin{}
	pluginEd25519        = &ed25519Plugin{}
	pluginParsec         = &parsecPlugin{}
	pluginPAM            = &pamPlugin{}
	pluginGSSAPI         = &gssapiPlugin{}
)

// getAuthPlugin returns the AuthPlugin implementation for the given plugin
// name, or nil if the plugin is unknown.
func getAuthPlugin(name string) AuthPlugin {
	switch name {
	case "mysql_native_password":
		return pluginNativePassword
	case "caching_sha2_password":
		return pluginCachingSha2
	case "sha256_password":
		return pluginSha256
	case "mysql_clear_password":
		return pluginClearPassword
	case "mysql_old_password":
		return pluginOldPassword
	case "client_ed25519":
		return pluginEd25519
	case "parsec":
		return pluginParsec
	case "dialog":
		return pluginPAM
	case "auth_gssapi_client":
		return pluginGSSAPI
	default:
		return nil
	}
}
