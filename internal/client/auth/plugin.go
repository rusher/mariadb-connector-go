// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import (
	"crypto/rsa"
	"errors"
)

// PluginConfig is the subset of connection config that authentication plugins need.
// client.Config implements this interface.
type PluginConfig interface {
	GetPasswd() string
	GetAllowNativePasswords() bool
	GetAllowCleartextPasswords() bool
	HasTLS() bool
	GetServerPublicKey() *rsa.PublicKey
	GetAllowPublicKeyRetrieval() bool
}

// AuthPlugin defines the interface for authentication plugins.
type AuthPlugin interface {
	PluginName() string
	InitAuth(authData []byte, config PluginConfig) ([]byte, error)
	ContinuationAuth(authData []byte, seed []byte, config PluginConfig) (nextPacket []byte, done bool, err error)
}

// SimpleAuth provides a no-op ContinuationAuth for plugins that don't need it.
type SimpleAuth struct{}

func (s *SimpleAuth) ContinuationAuth(_ []byte, _ []byte, _ PluginConfig) ([]byte, bool, error) {
	return nil, true, nil
}

// Common authentication errors.
var (
	ErrNativePassword    = errors.New("mysql_native_password authentication is disabled")
	ErrCleartextPassword = errors.New("mysql_clear_password authentication is disabled")
)

// static plugin registry
var (
	pluginNativePassword = &NativePasswordPlugin{}
	pluginEd25519        = &Ed25519Plugin{}
	pluginParsec         = &ParsecPlugin{}
	pluginCachingSha2    = &CachingSha2Plugin{}
	pluginSha256Password = &Sha256Plugin{}
	pluginClearPassword  = &ClearPasswordPlugin{}
	pluginPAM            = &PamPlugin{}
	pluginGSSAPI         = &GssapiPlugin{}
)

// GetAuthPlugin returns the authentication plugin for the given name.
func GetAuthPlugin(name string) (AuthPlugin, bool) {
	switch name {
	case "mysql_native_password":
		return pluginNativePassword, true
	case "client_ed25519", "ed25519":
		return pluginEd25519, true
	case "parsec":
		return pluginParsec, true
	case "caching_sha2_password":
		return pluginCachingSha2, true
	case "sha256_password":
		return pluginSha256Password, true
	case "mysql_clear_password":
		return pluginClearPassword, true
	case "dialog":
		return pluginPAM, true
	case "auth_gssapi_client":
		return pluginGSSAPI, true
	default:
		return nil, false
	}
}
