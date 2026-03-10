// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"os"
	"testing"

	_ "github.com/mariadb-connector-go/mariadb"
)

// TestDebugLogging demonstrates debug logging functionality
// Run with: MARIADB_TEST_DEBUG=true go test -tags=integration -v -run TestDebugLogging
func TestDebugLogging(t *testing.T) {
	// Skip if debug is not enabled
	if os.Getenv("MARIADB_TEST_DEBUG") != "true" && os.Getenv("MARIADB_TEST_DEBUG") != "1" {
		t.Skip("Skipping debug test - set MARIADB_TEST_DEBUG=true to run")
	}

	t.Log("=== MariaDB Debug Logging Test ===")
	t.Log("Debug logging is enabled - check output for packet exchanges")

	db := OpenTestDB(t)
	defer db.Close()

	t.Log("\n--- Executing simple query ---")
	var result int
	err := db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	t.Logf("Query result: %d", result)

	t.Log("\n--- Executing INSERT ---")
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS debug_test (id INT, name VARCHAR(50))")
	if err != nil {
		t.Errorf("Create table failed: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS debug_test")

	_, err = db.Exec("INSERT INTO debug_test VALUES (1, 'test')")
	if err != nil {
		t.Errorf("Insert failed: %v", err)
	}

	t.Log("\n--- Executing SELECT ---")
	rows, err := db.Query("SELECT id, name FROM debug_test LIMIT 1")
	if err != nil {
		t.Errorf("Select failed: %v", err)
	} else {
		defer rows.Close()
		for rows.Next() {
			var id int
			var name string
			rows.Scan(&id, &name)
			t.Logf("Row: id=%d, name=%s", id, name)
		}
	}

	t.Log("\n=== Debug logging test complete ===")
}
