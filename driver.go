// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"sync"

	"github.com/mariadb-connector-go/mariadb/internal/client"
)

const driverName = "mariadb"

func init() {
	sql.Register(driverName, &Driver{})
}

// Driver implements database/sql/driver.Driver interface
type Driver struct {
	mu         sync.Mutex
	connectors map[string]*Connector
}

// Open returns a new connection to the database.
// The name is a string in a driver-specific format.
func (d *Driver) Open(dsn string) (driver.Conn, error) {
	connector, err := d.OpenConnector(dsn)
	if err != nil {
		return nil, err
	}
	return connector.Connect(context.Background())
}

// OpenConnector implements driver.DriverContext interface
func (d *Driver) OpenConnector(dsn string) (driver.Connector, error) {
	cfg, err := client.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}

	return &Connector{
		driver: d,
		config: cfg,
	}, nil
}
