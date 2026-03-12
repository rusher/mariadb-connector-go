// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import (
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"fmt"
)

const (
	cachingSha2FastAuth       = 3
	cachingSha2FullAuthNeeded = 4
)

// CachingSha2Plugin implements the caching_sha2_password authentication plugin.
type CachingSha2Plugin struct {
	SimpleAuth
}

func (p *CachingSha2Plugin) PluginName() string { return "caching_sha2_password" }

func (p *CachingSha2Plugin) InitAuth(authData []byte, cfg PluginConfig) ([]byte, error) {
	return scrambleSHA256Password(authData, cfg.GetPasswd()), nil
}

func (p *CachingSha2Plugin) ContinuationAuth(packet []byte, authData []byte, cfg PluginConfig) ([]byte, bool, error) {
	if len(packet) == 0 {
		return nil, false, fmt.Errorf("empty continuation packet")
	}
	switch packet[0] {
	case cachingSha2FastAuth:
		return nil, true, nil
	case cachingSha2FullAuthNeeded:
		if cfg.HasTLS() {
			return append([]byte(cfg.GetPasswd()), 0), true, nil
		}
		if pub := cfg.GetServerPublicKey(); pub != nil {
			enc, err := encryptPassword(cfg.GetPasswd(), authData, pub)
			if err != nil {
				return nil, false, err
			}
			return enc, true, nil
		}
		if !cfg.GetAllowPublicKeyRetrieval() {
			return nil, false, fmt.Errorf("caching_sha2_password requires either TLS or server public key. Enable AllowPublicKeyRetrieval=true to retrieve the public key from the server, or provide ServerPubKey in the connection string")
		}
		return []byte{2}, false, nil
	default:
		if packet[0] == 1 && len(packet) > 1 {
			block, _ := pem.Decode(packet[1:])
			if block == nil {
				return nil, false, fmt.Errorf("invalid PEM data")
			}
			pkix, err := x509.ParsePKIXPublicKey(block.Bytes)
			if err != nil {
				return nil, false, err
			}
			enc, err := encryptPassword(cfg.GetPasswd(), authData, pkix.(*rsa.PublicKey))
			if err != nil {
				return nil, false, err
			}
			return enc, true, nil
		}
		return nil, false, fmt.Errorf("unexpected packet: %v", packet)
	}
}

func scrambleSHA256Password(seed []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}
	message1 := sha256.Sum256([]byte(password))
	message1Hash := sha256.Sum256(message1[:])
	message2 := sha256.New()
	message2.Write(message1Hash[:])
	message2.Write(seed)
	hash := message2.Sum(nil)
	for i := range hash {
		hash[i] ^= message1[i]
	}
	return hash
}
