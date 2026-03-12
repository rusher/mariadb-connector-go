// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import (
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

// Sha256Plugin implements the sha256_password authentication plugin.
type Sha256Plugin struct {
	SimpleAuth
}

func (p *Sha256Plugin) PluginName() string { return "sha256_password" }

func (p *Sha256Plugin) InitAuth(authData []byte, cfg PluginConfig) ([]byte, error) {
	if cfg.GetPasswd() == "" {
		return []byte{}, nil
	}
	if cfg.HasTLS() {
		return append([]byte(cfg.GetPasswd()), 0), nil
	}
	if pub := cfg.GetServerPublicKey(); pub != nil {
		enc, err := encryptPassword(cfg.GetPasswd(), authData, pub)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt password: %w", err)
		}
		return enc, nil
	}
	return []byte{1}, nil
}

func (p *Sha256Plugin) ContinuationAuth(packet []byte, authData []byte, cfg PluginConfig) ([]byte, bool, error) {
	block, _ := pem.Decode(packet)
	if block == nil {
		return nil, false, fmt.Errorf("invalid PEM data in auth response")
	}
	pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse public key: %w", err)
	}
	pubKey, ok := pkix.(*rsa.PublicKey)
	if !ok {
		return nil, false, fmt.Errorf("public key is not RSA")
	}
	enc, err := encryptPassword(cfg.GetPasswd(), authData, pubKey)
	if err != nil {
		return nil, false, fmt.Errorf("failed to encrypt password: %w", err)
	}
	return enc, true, nil
}
