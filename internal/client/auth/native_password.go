// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import "crypto/sha1"

// NativePasswordPlugin implements the mysql_native_password authentication.
type NativePasswordPlugin struct {
	SimpleAuth
}

func (p *NativePasswordPlugin) PluginName() string { return "mysql_native_password" }

func (p *NativePasswordPlugin) InitAuth(authData []byte, cfg PluginConfig) ([]byte, error) {
	if !cfg.GetAllowNativePasswords() {
		return nil, ErrNativePassword
	}
	if cfg.GetPasswd() == "" {
		return []byte{}, nil
	}
	return scramblePassword(authData, cfg.GetPasswd()), nil
}

func scramblePassword(seed []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)
	crypt.Reset()
	crypt.Write(stage1)
	stage2 := crypt.Sum(nil)
	crypt.Reset()
	crypt.Write(seed)
	crypt.Write(stage2)
	scramble := crypt.Sum(nil)
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}
