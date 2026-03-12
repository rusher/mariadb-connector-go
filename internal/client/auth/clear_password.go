// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

// ClearPasswordPlugin implements the mysql_clear_password authentication plugin.
type ClearPasswordPlugin struct {
	SimpleAuth
}

func (p *ClearPasswordPlugin) PluginName() string { return "mysql_clear_password" }

func (p *ClearPasswordPlugin) InitAuth(_ []byte, cfg PluginConfig) ([]byte, error) {
	if !cfg.GetAllowCleartextPasswords() {
		return nil, ErrCleartextPassword
	}
	return append([]byte(cfg.GetPasswd()), 0), nil
}
