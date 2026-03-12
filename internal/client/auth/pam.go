// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import (
	"bytes"
	"fmt"
)

// PamPlugin implements the dialog (PAM) authentication plugin.
type PamPlugin struct{}

func (p *PamPlugin) PluginName() string { return "dialog" }

func (p *PamPlugin) InitAuth(_ []byte, cfg PluginConfig) ([]byte, error) {
	return []byte(cfg.GetPasswd() + "\x00"), nil
}

func (p *PamPlugin) ContinuationAuth(packet []byte, _ []byte, cfg PluginConfig) ([]byte, bool, error) {
	if len(packet) == 0 {
		return nil, false, fmt.Errorf("empty PAM packet")
	}
	parts := bytes.Split(packet[1:], []byte{0})
	if len(parts) < 1 {
		return nil, false, fmt.Errorf("invalid PAM packet format")
	}
	return []byte(cfg.GetPasswd() + "\x00"), false, nil
}
