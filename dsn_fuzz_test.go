// MariaDB Connector/Go - A MariaDB/MySQL-Driver for Go's database/sql package
//
// Copyright 2016 The Go-MySQL-Driver Authors. All rights reserved.
// Copyright 2026 MariaDB Corporation Ab. All rights reserved.
//
// SPDX-License-Identifier: MPL-2.0
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at http://mozilla.org/MPL/2.0/.

//go:build go1.18
// +build go1.18

package mysql

import (
	"net"
	"testing"
)

func FuzzFormatDSN(f *testing.F) {
	for _, test := range testDSNs { // See dsn_test.go
		f.Add(test.in)
	}

	f.Fuzz(func(t *testing.T, dsn1 string) {
		// Do not waste resources
		if len(dsn1) > 1000 {
			t.Skip("ignore: too long")
		}

		cfg1, err := ParseDSN(dsn1)
		if err != nil {
			t.Skipf("invalid DSN: %v", err)
		}

		dsn2 := cfg1.FormatDSN()
		if dsn2 == dsn1 {
			return
		}

		// Skip known cases of bad config that are not strictly checked by ParseDSN
		if _, _, err := net.SplitHostPort(cfg1.Addr); err != nil {
			t.Skipf("invalid addr %q: %v", cfg1.Addr, err)
		}

		cfg2, err := ParseDSN(dsn2)
		if err != nil {
			t.Fatalf("%q rewritten as %q: %v", dsn1, dsn2, err)
		}

		dsn3 := cfg2.FormatDSN()
		if dsn3 != dsn2 {
			t.Errorf("%q rewritten as %q", dsn2, dsn3)
		}
	})
}
