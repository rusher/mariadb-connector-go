// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"os"
	"testing"
)

// TestConfig holds common configuration for integration tests
type TestConfig struct {
	DSN   string
	Debug bool
}

// GetTestConfig returns the test configuration
func GetTestConfig() *TestConfig {
	dsn := os.Getenv("MARIADB_TEST_DSN")
	if dsn == "" {
		dsn = "root@tcp(localhost:3306)/testgo"
	}

	// Enable debug logging if MARIADB_TEST_DEBUG is set
	debug := os.Getenv("MARIADB_TEST_DEBUG") == "true" || os.Getenv("MARIADB_TEST_DEBUG") == "1"
	if debug && dsn != "" {
		// Add debug parameter to DSN if not already present
		if !containsDebug(dsn) {
			if containsParams(dsn) {
				dsn += "&debug=true"
			} else {
				dsn += "?debug=true"
			}
		}
	}

	return &TestConfig{
		DSN:   dsn,
		Debug: debug,
	}
}

// OpenTestDB opens a database connection for testing
func OpenTestDB(t *testing.T) *sql.DB {
	cfg := GetTestConfig()
	db, err := sql.Open("mariadb", cfg.DSN)
	if err != nil {
		t.Fatalf("Failed to open connection: %v", err)
	}
	return db
}

// SetupTestDB creates a fresh test database and returns a connection
func SetupTestDB(t *testing.T) *sql.DB {
	db := OpenTestDB(t)

	// Clean up any existing test tables
	cleanupTestTables(t, db)

	return db
}

// cleanupTestTables drops common test tables
func cleanupTestTables(t *testing.T, db *sql.DB) {
	tables := []string{
		"test_users",
		"test_data",
		"test_types",
		"debug_test",
	}

	for _, table := range tables {
		_, err := db.Exec("DROP TABLE IF EXISTS " + table)
		if err != nil {
			t.Logf("Warning: Failed to drop table %s: %v", table, err)
		}
	}
}

// containsDebug checks if DSN already has debug parameter
func containsDebug(dsn string) bool {
	return len(dsn) > 6 && (dsn[len(dsn)-10:] == "debug=true" ||
		len(dsn) > 11 && dsn[len(dsn)-11:len(dsn)-5] == "debug=")
}

// containsParams checks if DSN already has parameters
func containsParams(dsn string) bool {
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '?' {
			return true
		}
		if dsn[i] == '/' {
			return false
		}
	}
	return false
}
