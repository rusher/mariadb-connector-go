// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package auth

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
)

// encryptPassword XORs the password with the seed then RSA-OAEP encrypts it.
// Used by caching_sha2_password and sha256_password.
func encryptPassword(password string, seed []byte, pub *rsa.PublicKey) ([]byte, error) {
	plain := make([]byte, len(password)+1)
	copy(plain, password)
	for i := range plain {
		plain[i] ^= seed[i%len(seed)]
	}
	return rsa.EncryptOAEP(sha256.New(), rand.Reader, pub, plain, nil)
}
