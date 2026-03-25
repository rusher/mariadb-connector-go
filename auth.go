// MariaDB Connector/Go - A MariaDB/MySQL-Driver for Go's database/sql package
//
// Copyright 2018 The Go-MySQL-Driver Authors. All rights reserved.
// Copyright 2026 MariaDB Corporation Ab. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

package mysql

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"sync"

	"filippo.io/edwards25519"
)

// server pub keys registry
var (
	serverPubKeyLock     sync.RWMutex
	serverPubKeyRegistry map[string]*rsa.PublicKey
)

// RegisterServerPubKey registers a server RSA public key which can be used to
// send data in a secure manner to the server without receiving the public key
// in a potentially insecure way from the server first.
// Registered keys can afterwards be used adding serverPubKey=<name> to the DSN.
//
// Note: The provided rsa.PublicKey instance is exclusively owned by the driver
// after registering it and may not be modified.
//
//	data, err := os.ReadFile("mykey.pem")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	block, _ := pem.Decode(data)
//	if block == nil || block.Type != "PUBLIC KEY" {
//		log.Fatal("failed to decode PEM block containing public key")
//	}
//
//	pub, err := x509.ParsePKIXPublicKey(block.Bytes)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if rsaPubKey, ok := pub.(*rsa.PublicKey); ok {
//		mysql.RegisterServerPubKey("mykey", rsaPubKey)
//	} else {
//		log.Fatal("not a RSA public key")
//	}
func RegisterServerPubKey(name string, pubKey *rsa.PublicKey) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry == nil {
		serverPubKeyRegistry = make(map[string]*rsa.PublicKey)
	}

	serverPubKeyRegistry[name] = pubKey
	serverPubKeyLock.Unlock()
}

// DeregisterServerPubKey removes the public key registered with the given name.
func DeregisterServerPubKey(name string) {
	serverPubKeyLock.Lock()
	if serverPubKeyRegistry != nil {
		delete(serverPubKeyRegistry, name)
	}
	serverPubKeyLock.Unlock()
}

func getServerPubKey(name string) (pubKey *rsa.PublicKey) {
	serverPubKeyLock.RLock()
	if v, ok := serverPubKeyRegistry[name]; ok {
		pubKey = v
	}
	serverPubKeyLock.RUnlock()
	return
}

// Hash password using pre 4.1 (old password) method
// https://github.com/atcurtis/mariadb/blob/master/mysys/my_rnd.c
type myRnd struct {
	seed1, seed2 uint32
}

const myRndMaxVal = 0x3FFFFFFF

// Pseudo random number generator
func newMyRnd(seed1, seed2 uint32) *myRnd {
	return &myRnd{
		seed1: seed1 % myRndMaxVal,
		seed2: seed2 % myRndMaxVal,
	}
}

// Tested to be equivalent to MariaDB's floating point variant
// http://play.golang.org/p/QHvhd4qved
// http://play.golang.org/p/RG0q4ElWDx
func (r *myRnd) NextByte() byte {
	r.seed1 = (r.seed1*3 + r.seed2) % myRndMaxVal
	r.seed2 = (r.seed1 + r.seed2 + 33) % myRndMaxVal

	return byte(uint64(r.seed1) * 31 / myRndMaxVal)
}

// Generate binary hash from byte string using insecure pre 4.1 method
func pwHash(password []byte) (result [2]uint32) {
	var add uint32 = 7
	var tmp uint32

	result[0] = 1345345333
	result[1] = 0x12345671

	for _, c := range password {
		// skip spaces and tabs in password
		if c == ' ' || c == '\t' {
			continue
		}

		tmp = uint32(c)
		result[0] ^= (((result[0] & 63) + add) * tmp) + (result[0] << 8)
		result[1] += (result[1] << 8) ^ result[0]
		add += tmp
	}

	// Remove sign bit (1<<31)-1)
	result[0] &= 0x7FFFFFFF
	result[1] &= 0x7FFFFFFF

	return
}

// Hash password using insecure pre 4.1 method
func scrambleOldPassword(scramble []byte, password string) []byte {
	scramble = scramble[:8]

	hashPw := pwHash([]byte(password))
	hashSc := pwHash(scramble)

	r := newMyRnd(hashPw[0]^hashSc[0], hashPw[1]^hashSc[1])

	var out [8]byte
	for i := range out {
		out[i] = r.NextByte() + 64
	}

	mask := r.NextByte()
	for i := range out {
		out[i] ^= mask
	}

	return out[:]
}

// Hash password using 4.1+ method (SHA1)
func scramblePassword(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// stage1Hash = SHA1(password)
	crypt := sha1.New()
	crypt.Write([]byte(password))
	stage1 := crypt.Sum(nil)

	// scrambleHash = SHA1(scramble + SHA1(stage1Hash))
	// inner Hash
	crypt.Reset()
	crypt.Write(stage1)
	hash := crypt.Sum(nil)

	// outer Hash
	crypt.Reset()
	crypt.Write(scramble)
	crypt.Write(hash)
	scramble = crypt.Sum(nil)

	// token = scrambleHash XOR stage1Hash
	for i := range scramble {
		scramble[i] ^= stage1[i]
	}
	return scramble
}

// Hash password using MySQL 8+ method (SHA256)
func scrambleSHA256Password(scramble []byte, password string) []byte {
	if len(password) == 0 {
		return nil
	}

	// XOR(SHA256(password), SHA256(SHA256(SHA256(password)), scramble))

	crypt := sha256.New()
	crypt.Write([]byte(password))
	message1 := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1)
	message1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(message1Hash)
	crypt.Write(scramble)
	message2 := crypt.Sum(nil)

	for i := range message1 {
		message1[i] ^= message2[i]
	}

	return message1
}

func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		j := i % len(seed)
		plain[i] ^= seed[j]
	}
	sha1 := sha1.New()
	return rsa.EncryptOAEP(sha1, rand.Reader, pub, plain, nil)
}

// authEd25519 does ed25519 authentication used by MariaDB.
func authEd25519(scramble []byte, password string) ([]byte, error) {
	// Derived from https://github.com/MariaDB/server/blob/d8e6bb00888b1f82c031938f4c8ac5d97f6874c3/plugin/auth_ed25519/ref10/sign.c
	// Code style is from https://cs.opensource.google/go/go/+/refs/tags/go1.21.5:src/crypto/ed25519/ed25519.go;l=207
	h := sha512.Sum512([]byte(password))

	s, err := edwards25519.NewScalar().SetBytesWithClamping(h[:32])
	if err != nil {
		return nil, err
	}
	A := (&edwards25519.Point{}).ScalarBaseMult(s)

	mh := sha512.New()
	mh.Write(h[32:])
	mh.Write(scramble)
	messageDigest := mh.Sum(nil)
	r, err := edwards25519.NewScalar().SetUniformBytes(messageDigest)
	if err != nil {
		return nil, err
	}

	R := (&edwards25519.Point{}).ScalarBaseMult(r)

	kh := sha512.New()
	kh.Write(R.Bytes())
	kh.Write(A.Bytes())
	kh.Write(scramble)
	hramDigest := kh.Sum(nil)
	k, err := edwards25519.NewScalar().SetUniformBytes(hramDigest)
	if err != nil {
		return nil, err
	}

	S := k.MultiplyAdd(k, s, r)

	return append(R.Bytes(), S.Bytes()...), nil
}

func (mc *mysqlConn) auth(authData []byte, plugin string) ([]byte, error) {
	p := getAuthPlugin(plugin)
	if p == nil {
		mc.log("unknown auth plugin:", plugin)
		return nil, ErrUnknownPlugin
	}
	return p.InitAuth(authData, newAuthPluginConfig(mc.cfg))
}

func (mc *mysqlConn) handleAuthResult(oldAuthData []byte, plugin string) error {
	// Read Result Packet
	authData, newPlugin, err := mc.readAuthResult()
	if err != nil {
		return err
	}

	// handle auth plugin switch, if requested
	if newPlugin != "" {
		// If CLIENT_PLUGIN_AUTH capability is not supported, no new cipher is
		// sent and we have to keep using the cipher sent in the init packet.
		if authData == nil {
			authData = oldAuthData
		} else {
			// copy data from read buffer to owned slice
			copy(oldAuthData, authData)
		}

		plugin = newPlugin

		authResp, err := mc.auth(authData, plugin)
		if err != nil {
			return err
		}
		if err = mc.writeAuthSwitchPacket(authResp); err != nil {
			return err
		}

		// Read Result Packet
		authData, newPlugin, err = mc.readAuthResult()
		if err != nil {
			return err
		}

		// Do not allow to change the auth plugin more than once
		if newPlugin != "" {
			return ErrMalformPkt
		}
	}

	// Delegate continuation to the plugin's ContinuationAuth method.
	p := getAuthPlugin(plugin)
	if p == nil {
		// Unknown plugin, but if authData is empty the server accepted us.
		if len(authData) == 0 {
			return nil
		}
		return ErrUnknownPlugin
	}

	cfg := newAuthPluginConfig(mc.cfg)

	// Loop to handle multi-round authentication exchanges.
	for {
		if len(authData) == 0 {
			return nil // auth successful, no more data from server
		}

		nextPacket, done, err := p.ContinuationAuth(authData, oldAuthData, cfg)
		if err != nil {
			return err
		}

		if nextPacket != nil {
			if err = mc.writeAuthSwitchPacket(nextPacket); err != nil {
				return err
			}
		}

		if done {
			// Plugin says we're done. Read the final OK from the server.
			return mc.resultUnchanged().readResultOK()
		}

		// Plugin needs another round. Read the next server packet.
		data, err := mc.readPacket()
		if err != nil {
			return err
		}

		switch data[0] {
		case iOK:
			// resultUnchanged, since auth happens before any queries or
			// commands have been executed.
			return mc.resultUnchanged().handleOkPacket(data)
		case iAuthMoreData:
			authData = data[1:]
		case iERR:
			return mc.handleErrorPacket(data)
		default:
			// Some plugins (e.g. PAM/dialog) receive raw prompt packets
			// without the iAuthMoreData prefix.
			authData = data
		}
	}
}
