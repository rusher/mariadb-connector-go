// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import (
	"github.com/mariadb-connector-go/mariadb/internal/client"
)

// AuthPlugin defines the interface for authentication plugins
// This is exported for backward compatibility but delegates to internal client
type AuthPlugin = client.AuthPlugin

// GetAuthPlugin retrieves an authentication plugin by name
func GetAuthPlugin(name string) (AuthPlugin, bool) {
	return client.GetAuthPlugin(name)
}

// Config is exported for backward compatibility
// It's actually defined in internal/client but we re-export it here
type Config = client.Config

// ParseDSN parses a DSN string into a Config
func ParseDSN(dsn string) (*Config, error) {
	return client.ParseDSN(dsn)
}

// FormatDSN formats a Config into a DSN string
func FormatDSN(cfg *Config) string {
	return client.FormatDSN(cfg)
}
