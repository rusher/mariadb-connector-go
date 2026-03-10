// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"encoding/json"
	"testing"
)

func TestJSONFields(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Create table with JSON column
	_, err := db.Exec(`
		CREATE TEMPORARY TABLE test_json (
			id INT PRIMARY KEY,
			data JSON,
			metadata JSON
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("InsertAndSelectJSON", func(t *testing.T) {
		// Test data
		jsonData := map[string]interface{}{
			"name":  "John Doe",
			"age":   30,
			"email": "john@example.com",
			"tags":  []string{"developer", "golang"},
		}
		jsonBytes, _ := json.Marshal(jsonData)

		metadata := map[string]interface{}{
			"created": "2024-01-01",
			"version": 1,
		}
		metadataBytes, _ := json.Marshal(metadata)

		// Insert JSON data
		_, err := db.Exec(
			"INSERT INTO test_json (id, data, metadata) VALUES (?, ?, ?)",
			1, string(jsonBytes), string(metadataBytes),
		)
		if err != nil {
			t.Fatalf("Failed to insert JSON data: %v", err)
		}

		// Select and verify JSON data
		var id int
		var dataStr, metadataStr string
		err = db.QueryRow("SELECT id, data, metadata FROM test_json WHERE id = ?", 1).
			Scan(&id, &dataStr, &metadataStr)
		if err != nil {
			t.Fatalf("Failed to query JSON data: %v", err)
		}

		if id != 1 {
			t.Errorf("Expected id=1, got %d", id)
		}

		// Verify JSON data
		var retrievedData map[string]interface{}
		err = json.Unmarshal([]byte(dataStr), &retrievedData)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON data: %v", err)
		}

		if retrievedData["name"] != "John Doe" {
			t.Errorf("Expected name='John Doe', got %v", retrievedData["name"])
		}
		if retrievedData["age"].(float64) != 30 {
			t.Errorf("Expected age=30, got %v", retrievedData["age"])
		}

		t.Log("✓ JSON insert and select works correctly")
	})

	t.Run("JSONNullValues", func(t *testing.T) {
		// Insert NULL JSON
		_, err := db.Exec(
			"INSERT INTO test_json (id, data, metadata) VALUES (?, NULL, NULL)",
			2,
		)
		if err != nil {
			t.Fatalf("Failed to insert NULL JSON: %v", err)
		}

		// Select and verify NULL
		var id int
		var dataStr, metadataStr sql.NullString
		err = db.QueryRow("SELECT id, data, metadata FROM test_json WHERE id = ?", 2).
			Scan(&id, &dataStr, &metadataStr)
		if err != nil {
			t.Fatalf("Failed to query NULL JSON: %v", err)
		}

		if dataStr.Valid {
			t.Error("Expected data to be NULL")
		}
		if metadataStr.Valid {
			t.Error("Expected metadata to be NULL")
		}

		t.Log("✓ JSON NULL values handled correctly")
	})

	t.Run("JSONArrays", func(t *testing.T) {
		// Test JSON array
		jsonArray := []interface{}{
			map[string]interface{}{"id": 1, "value": "first"},
			map[string]interface{}{"id": 2, "value": "second"},
			map[string]interface{}{"id": 3, "value": "third"},
		}
		jsonBytes, _ := json.Marshal(jsonArray)

		_, err := db.Exec(
			"INSERT INTO test_json (id, data) VALUES (?, ?)",
			3, string(jsonBytes),
		)
		if err != nil {
			t.Fatalf("Failed to insert JSON array: %v", err)
		}

		// Select and verify
		var dataStr string
		err = db.QueryRow("SELECT data FROM test_json WHERE id = ?", 3).Scan(&dataStr)
		if err != nil {
			t.Fatalf("Failed to query JSON array: %v", err)
		}

		var retrievedArray []interface{}
		err = json.Unmarshal([]byte(dataStr), &retrievedArray)
		if err != nil {
			t.Fatalf("Failed to unmarshal JSON array: %v", err)
		}

		if len(retrievedArray) != 3 {
			t.Errorf("Expected array length 3, got %d", len(retrievedArray))
		}

		t.Log("✓ JSON arrays handled correctly")
	})

	t.Run("JSONNestedObjects", func(t *testing.T) {
		// Test nested JSON objects
		nestedJSON := map[string]interface{}{
			"user": map[string]interface{}{
				"name": "Alice",
				"address": map[string]interface{}{
					"street": "123 Main St",
					"city":   "Springfield",
					"zip":    "12345",
				},
				"phones": []string{"+1-555-1234", "+1-555-5678"},
			},
			"preferences": map[string]interface{}{
				"theme":    "dark",
				"language": "en",
			},
		}
		jsonBytes, _ := json.Marshal(nestedJSON)

		_, err := db.Exec(
			"INSERT INTO test_json (id, data) VALUES (?, ?)",
			4, string(jsonBytes),
		)
		if err != nil {
			t.Fatalf("Failed to insert nested JSON: %v", err)
		}

		// Select and verify
		var dataStr string
		err = db.QueryRow("SELECT data FROM test_json WHERE id = ?", 4).Scan(&dataStr)
		if err != nil {
			t.Fatalf("Failed to query nested JSON: %v", err)
		}

		var retrieved map[string]interface{}
		err = json.Unmarshal([]byte(dataStr), &retrieved)
		if err != nil {
			t.Fatalf("Failed to unmarshal nested JSON: %v", err)
		}

		user := retrieved["user"].(map[string]interface{})
		address := user["address"].(map[string]interface{})
		if address["city"] != "Springfield" {
			t.Errorf("Expected city='Springfield', got %v", address["city"])
		}

		t.Log("✓ Nested JSON objects handled correctly")
	})

	t.Run("JSONWithPreparedStatements", func(t *testing.T) {
		// Test JSON with prepared statements
		stmt, err := db.Prepare("INSERT INTO test_json (id, data) VALUES (?, ?)")
		if err != nil {
			t.Fatalf("Failed to prepare statement: %v", err)
		}
		defer stmt.Close()

		jsonData := map[string]interface{}{
			"test": "prepared statement",
			"id":   5,
		}
		jsonBytes, _ := json.Marshal(jsonData)

		_, err = stmt.Exec(5, string(jsonBytes))
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}

		// Verify with prepared select
		selectStmt, err := db.Prepare("SELECT data FROM test_json WHERE id = ?")
		if err != nil {
			t.Fatalf("Failed to prepare select: %v", err)
		}
		defer selectStmt.Close()

		var dataStr string
		err = selectStmt.QueryRow(5).Scan(&dataStr)
		if err != nil {
			t.Fatalf("Failed to query with prepared statement: %v", err)
		}

		var retrieved map[string]interface{}
		json.Unmarshal([]byte(dataStr), &retrieved)
		if retrieved["test"] != "prepared statement" {
			t.Errorf("Expected test='prepared statement', got %v", retrieved["test"])
		}

		t.Log("✓ JSON with prepared statements works correctly")
	})
}

func TestExtendedMetadata(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Create table with various types that have extended metadata
	_, err := db.Exec(`
		CREATE TEMPORARY TABLE test_extended_meta (
			id INT PRIMARY KEY,
			json_col JSON,
			geom_col GEOMETRY,
			point_col POINT,
			enum_col ENUM('small', 'medium', 'large'),
			set_col SET('read', 'write', 'execute')
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("ExtendedMetadataTypes", func(t *testing.T) {
		// Insert test data
		_, err := db.Exec(`
			INSERT INTO test_extended_meta (id, json_col, enum_col, set_col)
			VALUES (?, ?, ?, ?)
		`, 1, `{"key": "value"}`, "medium", "read,write")
		if err != nil {
			t.Fatalf("Failed to insert data: %v", err)
		}

		// Query and check column metadata
		rows, err := db.Query("SELECT * FROM test_extended_meta WHERE id = ?", 1)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columns, err := rows.Columns()
		if err != nil {
			t.Fatalf("Failed to get columns: %v", err)
		}

		expectedColumns := []string{"id", "json_col", "geom_col", "point_col", "enum_col", "set_col"}
		if len(columns) != len(expectedColumns) {
			t.Errorf("Expected %d columns, got %d", len(expectedColumns), len(columns))
		}

		for i, col := range columns {
			if col != expectedColumns[i] {
				t.Errorf("Column %d: expected %s, got %s", i, expectedColumns[i], col)
			}
		}

		// Verify data can be scanned
		if rows.Next() {
			var id int
			var jsonCol sql.NullString
			var geomCol, pointCol sql.NullString
			var enumCol, setCol sql.NullString

			err = rows.Scan(&id, &jsonCol, &geomCol, &pointCol, &enumCol, &setCol)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			if id != 1 {
				t.Errorf("Expected id=1, got %d", id)
			}
			if !jsonCol.Valid || jsonCol.String != `{"key": "value"}` {
				t.Errorf("JSON column mismatch: %v", jsonCol)
			}
			if !enumCol.Valid || enumCol.String != "medium" {
				t.Errorf("ENUM column mismatch: %v", enumCol)
			}
			if !setCol.Valid || setCol.String != "read,write" {
				t.Errorf("SET column mismatch: %v", setCol)
			}
		}

		t.Log("✓ Extended metadata types handled correctly")
	})
}
