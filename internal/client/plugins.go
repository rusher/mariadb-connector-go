// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/x509"
	"encoding/pem"
	"fmt"

	"filippo.io/edwards25519"
	"golang.org/x/crypto/pbkdf2"
)

// ============================================================================
// Plugin Registry
// ============================================================================

// Static registry of authentication plugins
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

// GetAuthPlugin returns the authentication plugin for the given name
// This is a static registry - no dynamic registration
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

// ============================================================================
// Native Password Plugin
// ============================================================================

// NativePasswordPlugin implements the mysql_native_password authentication
type NativePasswordPlugin struct {
	SimpleAuth
}

func (p *NativePasswordPlugin) PluginName() string {
	return "mysql_native_password"
}

func (p *NativePasswordPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if !cfg.AllowNativePasswords {
		return nil, ErrNativePassword
	}
	if cfg.Passwd == "" {
		return []byte{}, nil
	}
	return scramblePassword(authData, cfg.Passwd), nil
}

// scramblePassword scrambles a password using MySQL native password algorithm
func scramblePassword(seed []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// stage2Hash = SHA1(stage1Hash)
	crypt.Reset()
	crypt.Write(stage1)
	stage2 := crypt.Sum(nil)

	// scrambleHash = SHA1(seed + stage2Hash)
	crypt.Reset()
	crypt.Write(seed)
	crypt.Write(stage2)
	scramble := crypt.Sum(nil)

	// token = stage1Hash XOR scrambleHash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// ============================================================================
// Ed25519 Plugin
// ============================================================================

// Ed25519Plugin implements the ed25519 authentication plugin
type Ed25519Plugin struct {
	SimpleAuth
}

func (p *Ed25519Plugin) PluginName() string {
	return "ed25519"
}

func (p *Ed25519Plugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if cfg.Passwd == "" {
		return []byte{}, nil
	}

	// Ed25519 requires exactly 32 bytes of authData (the scramble/challenge)
	if len(authData) != 32 {
		return nil, fmt.Errorf("ed25519: invalid authData length: expected 32, got %d", len(authData))
	}

	h := sha512.Sum512([]byte(cfg.Passwd))

	s, err := edwards25519.NewScalar().SetBytesWithClamping(h[:32])
	if err != nil {
		return nil, err
	}
	A := (&edwards25519.Point{}).ScalarBaseMult(s)

	mh := sha512.New()
	mh.Write(h[32:])
	mh.Write(authData)
	messageDigest := mh.Sum(nil)
	r, err := edwards25519.NewScalar().SetUniformBytes(messageDigest)
	if err != nil {
		return nil, err
	}

	R := (&edwards25519.Point{}).ScalarBaseMult(r)

	kh := sha512.New()
	kh.Write(R.Bytes())
	kh.Write(A.Bytes())
	kh.Write(authData)
	hramDigest := kh.Sum(nil)
	k, err := edwards25519.NewScalar().SetUniformBytes(hramDigest)
	if err != nil {
		return nil, err
	}

	S := k.MultiplyAdd(k, s, r)

	return append(R.Bytes(), S.Bytes()...), nil
}

// ============================================================================
// Parsec Plugin
// ============================================================================

// ParsecPlugin implements the parsec authentication plugin
type ParsecPlugin struct {
	AuthPlugin
}

func (p *ParsecPlugin) PluginName() string {
	return "parsec"
}

func (p *ParsecPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	return []byte{}, nil
}

func (p *ParsecPlugin) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
	if len(packet) < 2 {
		return nil, false, fmt.Errorf("parsec: invalid salt packet length")
	}

	pos := 0

	if packet[0] == 0x01 {
		pos++
	}

	if len(packet[pos:]) < 2 {
		return nil, false, fmt.Errorf("parsec: packet too short for algorithm and iterations")
	}

	firstByte := packet[pos]
	iterations := int(packet[pos+1])
	pos += 2

	if firstByte != 0x50 {
		return nil, false, fmt.Errorf("parsec: expected 'P' for PBKDF2, got 0x%02x", firstByte)
	}

	if iterations > 20 {
		return nil, false, fmt.Errorf("parsec: iteration count too high: %d", iterations)
	}

	salt := packet[pos:]

	iterationCount := 1024 << iterations
	derivedKey := pbkdf2.Key([]byte(cfg.Passwd), salt, iterationCount, 32, sha512.New)

	privateKey := ed25519.NewKeyFromSeed(derivedKey)

	// Generate client scramble (32 random bytes)
	clientScramble := make([]byte, 32)
	if _, err := rand.Read(clientScramble); err != nil {
		return nil, false, fmt.Errorf("parsec: failed to generate client scramble: %w", err)
	}

	// Sign: authData (from handshake) + clientScramble
	message := append(authData, clientScramble...)
	signature := ed25519.Sign(privateKey, message)

	// Response: clientScramble (32 bytes) + signature (64 bytes)
	response := make([]byte, 0, 32+64)
	response = append(response, clientScramble...)
	response = append(response, signature...)

	return response, true, nil
}

// ============================================================================
// Caching SHA2 Plugin
// ============================================================================

const (
	cachingSha2FastAuth       = 3
	cachingSha2FullAuthNeeded = 4
)

type CachingSha2Plugin struct {
	SimpleAuth
}

func (p *CachingSha2Plugin) PluginName() string {
	return "caching_sha2_password"
}

func (p *CachingSha2Plugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	return scrambleSHA256Password(authData, cfg.Passwd), nil
}

func (p *CachingSha2Plugin) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
	if len(packet) == 0 {
		return nil, false, fmt.Errorf("empty continuation packet")
	}

	switch packet[0] {
	case cachingSha2FastAuth:
		return nil, true, nil
	case cachingSha2FullAuthNeeded:
		if cfg.TLS != nil || cfg.TLSConfig != "" {
			return append([]byte(cfg.Passwd), 0), true, nil
		}
		// Use the server public key directly (it's already *rsa.PublicKey)
		encrypted, err := encryptPassword(cfg.Passwd, authData, cfg.ServerPublicKey)
		if err != nil {
			return nil, false, err
		}
		return encrypted, true, nil
		if !cfg.AllowPublicKeyRetrieval {
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
			pubKey := pkix.(*rsa.PublicKey)
			enc, err := encryptPassword(cfg.Passwd, authData, pubKey)
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

// ============================================================================
// SHA256 Password Plugin
// ============================================================================

type Sha256Plugin struct {
	SimpleAuth
}

func (p *Sha256Plugin) PluginName() string {
	return "sha256_password"
}

func (p *Sha256Plugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if cfg.Passwd == "" {
		return []byte{}, nil
	}

	if cfg.TLS != nil || cfg.TLSConfig != "" {
		return append([]byte(cfg.Passwd), 0), nil
	}

	if cfg.ServerPublicKey != nil {
		enc, err := encryptPassword(cfg.Passwd, authData, cfg.ServerPublicKey)
		if err != nil {
			return nil, fmt.Errorf("failed to encrypt password: %w", err)
		}
		return enc, nil
	}

	return []byte{1}, nil
}

func (p *Sha256Plugin) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
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

	enc, err := encryptPassword(cfg.Passwd, authData, pubKey)
	if err != nil {
		return nil, false, fmt.Errorf("failed to encrypt password: %w", err)
	}

	return enc, true, nil
}

// ============================================================================
// Cleartext Password Plugin
// ============================================================================

type ClearPasswordPlugin struct {
	SimpleAuth
}

func (p *ClearPasswordPlugin) PluginName() string {
	return "mysql_clear_password"
}

func (p *ClearPasswordPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	if !cfg.AllowCleartextPasswords {
		return nil, ErrCleartextPassword
	}
	return append([]byte(cfg.Passwd), 0), nil
}

// ============================================================================
// PAM Plugin
// ============================================================================

type PamPlugin struct {
	AuthPlugin
}

func (p *PamPlugin) PluginName() string {
	return "dialog"
}

func (p *PamPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	return []byte(cfg.Passwd + "\x00"), nil
}

func (p *PamPlugin) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
	if len(packet) == 0 {
		return nil, false, fmt.Errorf("empty PAM packet")
	}

	parts := bytes.Split(packet[1:], []byte{0})
	if len(parts) < 1 {
		return nil, false, fmt.Errorf("invalid PAM packet format")
	}

	return []byte(cfg.Passwd + "\x00"), false, nil
}

// ============================================================================
// GSSAPI Plugin
// ============================================================================

type GssapiPlugin struct {
	AuthPlugin
}

func (p *GssapiPlugin) PluginName() string {
	return "auth_gssapi_client"
}

func (p *GssapiPlugin) InitAuth(authData []byte, cfg *Config) ([]byte, error) {
	return nil, fmt.Errorf("GSSAPI authentication not implemented")
}

func (p *GssapiPlugin) ContinuationAuth(packet []byte, authData []byte, cfg *Config) ([]byte, bool, error) {
	return nil, false, fmt.Errorf("GSSAPI authentication not implemented")
}

// ============================================================================
// Helper Functions
// ============================================================================

// encryptPassword encrypts password using RSA public key
func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)

	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}

	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plain, nil)
}
