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
	"context"
	"net"
	"testing"
	"time"
)

func TestConnectorReturnsTimeout(t *testing.T) {
	connector := newConnector(&Config{
		Net:     "tcp",
		Addr:    "1.1.1.1:1234",
		Timeout: 10 * time.Millisecond,
	})

	_, err := connector.Connect(context.Background())
	if err == nil {
		t.Fatal("error expected")
	}

	if nerr, ok := err.(*net.OpError); ok {
		expected := "dial tcp 1.1.1.1:1234: i/o timeout"
		if nerr.Error() != expected {
			t.Fatalf("expected %q, got %q", expected, nerr.Error())
		}
	} else {
		t.Fatalf("expected %T, got %T", nerr, err)
	}
}
