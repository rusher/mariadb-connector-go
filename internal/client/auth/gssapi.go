// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import "fmt"

// GssapiPlugin implements the auth_gssapi_client authentication plugin.
type GssapiPlugin struct{}

func (p *GssapiPlugin) PluginName() string { return "auth_gssapi_client" }

func (p *GssapiPlugin) InitAuth(_ []byte, _ PluginConfig) ([]byte, error) {
	return nil, fmt.Errorf("GSSAPI authentication not implemented")
}

func (p *GssapiPlugin) ContinuationAuth(_ []byte, _ []byte, _ PluginConfig) ([]byte, bool, error) {
	return nil, false, fmt.Errorf("GSSAPI authentication not implemented")
}
