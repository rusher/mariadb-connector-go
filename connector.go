// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql/driver"

	"github.com/mariadb-connector-go/mariadb/internal/client"
)

// Connector implements driver.Connector interface
type Connector struct {
	driver *Driver
	config *client.Config
}

// Connect returns a connection to the database
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := newConn(c.config.Clone())
	if err != nil {
		return nil, err
	}

	if err := conn.connect(ctx); err != nil {
		return nil, err
	}

	return conn, nil
}

// Driver returns the underlying Driver of the Connector
func (c *Connector) Driver() driver.Driver {
	return c.driver
}
