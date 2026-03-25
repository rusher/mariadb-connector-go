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
	"bytes"
	"testing"
)

func TestGetAuthPlugin(t *testing.T) {
	known := []string{
		"mysql_native_password",
		"caching_sha2_password",
		"sha256_password",
		"mysql_clear_password",
		"mysql_old_password",
		"client_ed25519",
		"parsec",
		"dialog",
		"auth_gssapi_client",
	}
	for _, name := range known {
		p := getAuthPlugin(name)
		if p == nil {
			t.Errorf("getAuthPlugin(%q) returned nil", name)
			continue
		}
		if p.Name() != name {
			t.Errorf("getAuthPlugin(%q).Name() = %q", name, p.Name())
		}
	}

	if p := getAuthPlugin("nonexistent_plugin"); p != nil {
		t.Errorf("getAuthPlugin(nonexistent) should return nil, got %v", p)
	}
}

func TestAuthPluginParsecInitAuth(t *testing.T) {
	p := getAuthPlugin("parsec")
	if p == nil {
		t.Fatal("parsec plugin not found")
	}
	cfg := authPluginConfig{passwd: "secret"}
	resp, err := p.InitAuth([]byte("ignored"), cfg)
	if err != nil {
		t.Fatal(err)
	}
	// parsec InitAuth returns empty response
	if len(resp) != 0 {
		t.Errorf("expected empty response, got %v", resp)
	}
}

func TestAuthPluginParsecContinuationAuth(t *testing.T) {
	p := getAuthPlugin("parsec")
	if p == nil {
		t.Fatal("parsec plugin not found")
	}
	cfg := authPluginConfig{passwd: "secret"}
	authData := make([]byte, 32)
	for i := range authData {
		authData[i] = byte(i)
	}

	// Simulate server salt packet: 'P' + iterations(0) + 16 bytes salt
	salt := make([]byte, 16)
	for i := range salt {
		salt[i] = byte(i + 100)
	}
	packet := append([]byte{0x50, 0x00}, salt...)

	resp, done, err := p.ContinuationAuth(packet, authData, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if !done {
		t.Error("expected done=true")
	}
	// Response should be 32 bytes client scramble + 64 bytes ed25519 signature
	if len(resp) != 96 {
		t.Errorf("expected 96 bytes response, got %d", len(resp))
	}
}

func TestAuthPluginParsecInvalidPacket(t *testing.T) {
	p := getAuthPlugin("parsec")
	if p == nil {
		t.Fatal("parsec plugin not found")
	}
	cfg := authPluginConfig{passwd: "secret"}

	// Too short packet
	_, _, err := p.ContinuationAuth([]byte{0x01}, nil, cfg)
	if err == nil {
		t.Error("expected error for short packet")
	}

	// Wrong algorithm byte
	_, _, err = p.ContinuationAuth([]byte{0x51, 0x00, 0x01, 0x02}, nil, cfg)
	if err == nil {
		t.Error("expected error for wrong algorithm byte")
	}
}

func TestAuthPluginPAMInitAuth(t *testing.T) {
	p := getAuthPlugin("dialog")
	if p == nil {
		t.Fatal("dialog plugin not found")
	}

	// Should fail without AllowCleartextPasswords
	cfg := authPluginConfig{passwd: "secret", allowCleartextPasswords: false}
	_, err := p.InitAuth(nil, cfg)
	if err != ErrCleartextPassword {
		t.Errorf("expected ErrCleartextPassword, got %v", err)
	}

	// Should succeed with AllowCleartextPasswords
	cfg.allowCleartextPasswords = true
	resp, err := p.InitAuth(nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	expected := append([]byte("secret"), 0)
	if !bytes.Equal(resp, expected) {
		t.Errorf("expected %v, got %v", expected, resp)
	}
}

func TestAuthPluginPAMContinuationAuth(t *testing.T) {
	p := getAuthPlugin("dialog")
	if p == nil {
		t.Fatal("dialog plugin not found")
	}
	cfg := authPluginConfig{passwd: "secret", allowCleartextPasswords: true}

	// Simulate a PAM prompt (type byte 0x02 = echo, last prompt)
	packet := append([]byte{0x02}, []byte("Password: ")...)
	resp, done, err := p.ContinuationAuth(packet, nil, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if done {
		t.Error("expected done=false; server signals completion via OK packet")
	}
	expected := append([]byte("secret"), 0)
	if !bytes.Equal(resp, expected) {
		t.Errorf("expected %v, got %v", expected, resp)
	}
}

func TestAuthPluginGSSAPINotSupported(t *testing.T) {
	p := getAuthPlugin("auth_gssapi_client")
	if p == nil {
		t.Fatal("auth_gssapi_client plugin not found")
	}
	cfg := authPluginConfig{}

	_, err := p.InitAuth(nil, cfg)
	if err == nil {
		t.Error("expected error from GSSAPI InitAuth")
	}

	_, _, err = p.ContinuationAuth(nil, nil, cfg)
	if err == nil {
		t.Error("expected error from GSSAPI ContinuationAuth")
	}
}

func TestAuthCachingSHA256PasswordNoPublicKeyRetrieval(t *testing.T) {
	_, mc := newRWMockConn(1)
	mc.cfg.User = "root"
	mc.cfg.Passwd = "secret"
	mc.cfg.AllowPublicKeyRetrieval = false

	p := getAuthPlugin("caching_sha2_password")
	cfg := newAuthPluginConfig(mc.cfg)

	// Simulate PerformFullAuthentication without TLS, unix, pubkey, or AllowPublicKeyRetrieval
	_, _, err := p.ContinuationAuth(
		[]byte{cachingSha2PasswordPerformFullAuthentication},
		[]byte{1, 2, 3},
		cfg,
	)
	if err == nil {
		t.Error("expected error when AllowPublicKeyRetrieval is false")
	}
}
