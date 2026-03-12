// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import (
	"crypto/sha512"
	"fmt"

	"filippo.io/edwards25519"
)

// Ed25519Plugin implements the client_ed25519 authentication plugin.
type Ed25519Plugin struct {
	SimpleAuth
}

func (p *Ed25519Plugin) PluginName() string { return "ed25519" }

func (p *Ed25519Plugin) InitAuth(authData []byte, cfg PluginConfig) ([]byte, error) {
	if cfg.GetPasswd() == "" {
		return []byte{}, nil
	}
	if len(authData) != 32 {
		return nil, fmt.Errorf("ed25519: invalid authData length: expected 32, got %d", len(authData))
	}
	h := sha512.Sum512([]byte(cfg.GetPasswd()))
	s, err := edwards25519.NewScalar().SetBytesWithClamping(h[:32])
	if err != nil {
		return nil, err
	}
	A := (&edwards25519.Point{}).ScalarBaseMult(s)
	mh := sha512.New()
	mh.Write(h[32:])
	mh.Write(authData)
	r, err := edwards25519.NewScalar().SetUniformBytes(mh.Sum(nil))
	if err != nil {
		return nil, err
	}
	R := (&edwards25519.Point{}).ScalarBaseMult(r)
	kh := sha512.New()
	kh.Write(R.Bytes())
	kh.Write(A.Bytes())
	kh.Write(authData)
	k, err := edwards25519.NewScalar().SetUniformBytes(kh.Sum(nil))
	if err != nil {
		return nil, err
	}
	S := k.MultiplyAdd(k, s, r)
	return append(R.Bytes(), S.Bytes()...), nil
}
