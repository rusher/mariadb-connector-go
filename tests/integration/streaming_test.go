// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"testing"

	_ "github.com/mariadb-connector-go/mariadb"
)

// TestStreamingWithBuffering tests that streaming result sets are automatically
// buffered when a new command is issued before the previous result set is consumed
func TestStreamingWithBuffering(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	t.Log("=== Test: Streaming with automatic buffering ===")

	// Start a query but don't consume all rows
	rows, err := db.Query("SELECT 1 UNION SELECT 2 UNION SELECT 3")
	if err != nil {
		t.Fatalf("Query failed: %v", err)
	}
	defer rows.Close()

	// Read only first row
	if rows.Next() {
		var val int
		rows.Scan(&val)
		t.Logf("Read first row: %d", val)
		if val != 1 {
			t.Errorf("Expected first row to be 1, got %d", val)
		}
	}

	// Issue a new query WITHOUT closing the first one
	// This should trigger automatic buffering of remaining rows
	t.Log("Issuing new query while first query is still active...")

	var result int
	err = db.QueryRow("SELECT 42").Scan(&result)
	if err != nil {
		t.Fatalf("Second query failed: %v", err)
	}
	t.Logf("Second query result: %d", result)
	if result != 42 {
		t.Errorf("Expected 42, got %d", result)
	}

	// Now continue reading from first query (should read from buffer)
	t.Log("Continuing to read from first query (from buffer)...")
	count := 0
	expectedValues := []int{2, 3}
	for rows.Next() {
		var val int
		rows.Scan(&val)
		t.Logf("Read row %d: %d (from buffer)", count+2, val)
		if count < len(expectedValues) && val != expectedValues[count] {
			t.Errorf("Expected row %d to be %d, got %d", count+2, expectedValues[count], val)
		}
		count++
	}

	if count != 2 {
		t.Errorf("Expected to read 2 more rows from buffer, got %d", count)
	}

	t.Log("✓ Streaming with automatic buffering works correctly!")
}
