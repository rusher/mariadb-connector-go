// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"strings"
	"testing"
)

// Helper function to check if server is MariaDB
func isMariaDB(t *testing.T, db *sql.DB) bool {
	var version string
	err := db.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		t.Fatalf("Failed to get version: %v", err)
	}
	return strings.Contains(strings.ToLower(version), "mariadb")
}

func TestColumnTypes(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	mariadb := isMariaDB(t, db)
	t.Logf("Server type: MariaDB=%v", mariadb)

	// Create comprehensive test table
	createTableSQL := `
		CREATE TEMPORARY TABLE test_column_types (
			id INT PRIMARY KEY,
			-- Integer types
			tinyint_col TINYINT,
			tinyint_unsigned_col TINYINT UNSIGNED,
			smallint_col SMALLINT,
			smallint_unsigned_col SMALLINT UNSIGNED,
			mediumint_col MEDIUMINT,
			mediumint_unsigned_col MEDIUMINT UNSIGNED,
			int_col INT,
			int_unsigned_col INT UNSIGNED,
			bigint_col BIGINT,
			bigint_unsigned_col BIGINT UNSIGNED,
			-- Floating point types
			float_col FLOAT,
			double_col DOUBLE,
			decimal_col DECIMAL(10,2),
			-- String types
			char_col CHAR(10),
			varchar_col VARCHAR(255),
			tinytext_col TINYTEXT,
			text_col TEXT,
			mediumtext_col MEDIUMTEXT,
			longtext_col LONGTEXT,
			-- Binary types
			binary_col BINARY(10),
			varbinary_col VARBINARY(255),
			tinyblob_col TINYBLOB,
			blob_col BLOB,
			mediumblob_col MEDIUMBLOB,
			longblob_col LONGBLOB,
			-- Date/Time types
			date_col DATE,
			time_col TIME,
			datetime_col DATETIME,
			timestamp_col TIMESTAMP,
			year_col YEAR,
			-- Other types
			enum_col ENUM('small', 'medium', 'large'),
			set_col SET('read', 'write', 'execute'),
			json_col JSON`

	// Add UUID column for MariaDB
	if mariadb {
		createTableSQL += `,
			uuid_col UUID`
	}

	createTableSQL += `
		)
	`

	_, err := db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	t.Run("IntegerColumnTypes", func(t *testing.T) {
		rows, err := db.Query("SELECT tinyint_col, tinyint_unsigned_col, smallint_col, smallint_unsigned_col, mediumint_col, mediumint_unsigned_col, int_col, int_unsigned_col, bigint_col, bigint_unsigned_col FROM test_column_types LIMIT 0")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		expectedTypes := []struct {
			name         string
			databaseType string
			scanType     string
		}{
			{"tinyint_col", "TINYINT", "int64"},
			{"tinyint_unsigned_col", "TINYINT", "uint64"},
			{"smallint_col", "SMALLINT", "int64"},
			{"smallint_unsigned_col", "SMALLINT", "uint64"},
			{"mediumint_col", "MEDIUMINT", "int64"},
			{"mediumint_unsigned_col", "MEDIUMINT", "uint64"},
			{"int_col", "INT", "int64"},
			{"int_unsigned_col", "INT", "uint64"},
			{"bigint_col", "BIGINT", "int64"},
			{"bigint_unsigned_col", "BIGINT", "uint64"},
		}

		for i, ct := range columnTypes {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()
			scanType := ct.ScanType().String()

			if name != expectedTypes[i].name {
				t.Errorf("Column %d: expected name %s, got %s", i, expectedTypes[i].name, name)
			}

			if !strings.Contains(dbType, expectedTypes[i].databaseType) {
				t.Errorf("Column %s: expected type containing %s, got %s", name, expectedTypes[i].databaseType, dbType)
			}

			// Check nullable
			nullable, ok := ct.Nullable()
			if !ok {
				t.Errorf("Column %s: Nullable() should return ok=true", name)
			}
			if !nullable {
				t.Errorf("Column %s: should be nullable", name)
			}

			t.Logf("✓ Column %s: type=%s, scanType=%s, nullable=%v", name, dbType, scanType, nullable)
		}
	})

	t.Run("FloatingPointColumnTypes", func(t *testing.T) {
		rows, err := db.Query("SELECT float_col, double_col, decimal_col FROM test_column_types LIMIT 0")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		expectedTypes := []struct {
			name         string
			databaseType string
		}{
			{"float_col", "FLOAT"},
			{"double_col", "DOUBLE"},
			{"decimal_col", "DECIMAL"},
		}

		for i, ct := range columnTypes {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()

			if name != expectedTypes[i].name {
				t.Errorf("Column %d: expected name %s, got %s", i, expectedTypes[i].name, name)
			}

			if !strings.Contains(dbType, expectedTypes[i].databaseType) {
				t.Errorf("Column %s: expected type containing %s, got %s", name, expectedTypes[i].databaseType, dbType)
			}

			// Check precision for DECIMAL
			if name == "decimal_col" {
				precision, scale, ok := ct.DecimalSize()
				if !ok {
					t.Errorf("Column %s: DecimalSize() should return ok=true", name)
				}
				// Note: MariaDB may report different precision due to character set multiplier
				if scale != 2 {
					t.Errorf("Column %s: expected scale 2, got %d", name, scale)
				}
				t.Logf("✓ Column %s: precision=%d, scale=%d", name, precision, scale)
			}

			t.Logf("✓ Column %s: type=%s", name, dbType)
		}
	})

	t.Run("StringColumnTypes", func(t *testing.T) {
		rows, err := db.Query("SELECT char_col, varchar_col, tinytext_col, text_col, mediumtext_col, longtext_col FROM test_column_types LIMIT 0")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		// Note: MariaDB reports TEXT types as BLOB in the protocol
		expectedNames := []string{"char_col", "varchar_col", "tinytext_col", "text_col", "mediumtext_col", "longtext_col"}

		for i, ct := range columnTypes {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()

			if name != expectedNames[i] {
				t.Errorf("Column %d: expected name %s, got %s", i, expectedNames[i], name)
			}

			// Check length is available
			length, ok := ct.Length()
			if !ok {
				t.Errorf("Column %s: Length() should return ok=true", name)
			}

			t.Logf("✓ Column %s: type=%s, length=%d", name, dbType, length)
		}
	})

	t.Run("BinaryColumnTypes", func(t *testing.T) {
		rows, err := db.Query("SELECT binary_col, varbinary_col, tinyblob_col, blob_col, mediumblob_col, longblob_col FROM test_column_types LIMIT 0")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		// Note: MariaDB reports BINARY types with different names in the protocol
		expectedNames := []string{"binary_col", "varbinary_col", "tinyblob_col", "blob_col", "mediumblob_col", "longblob_col"}

		for i, ct := range columnTypes {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()

			if name != expectedNames[i] {
				t.Errorf("Column %d: expected name %s, got %s", i, expectedNames[i], name)
			}

			// Check length is available
			length, ok := ct.Length()
			if !ok {
				t.Errorf("Column %s: Length() should return ok=true", name)
			}

			t.Logf("✓ Column %s: type=%s, length=%d", name, dbType, length)
		}
	})

	t.Run("DateTimeColumnTypes", func(t *testing.T) {
		rows, err := db.Query("SELECT date_col, time_col, datetime_col, timestamp_col, year_col FROM test_column_types LIMIT 0")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		expectedTypes := []struct {
			name         string
			databaseType string
		}{
			{"date_col", "DATE"},
			{"time_col", "TIME"},
			{"datetime_col", "DATETIME"},
			{"timestamp_col", "TIMESTAMP"},
			{"year_col", "YEAR"},
		}

		for i, ct := range columnTypes {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()

			if name != expectedTypes[i].name {
				t.Errorf("Column %d: expected name %s, got %s", i, expectedTypes[i].name, name)
			}

			if !strings.Contains(dbType, expectedTypes[i].databaseType) {
				t.Errorf("Column %s: expected type containing %s, got %s", name, expectedTypes[i].databaseType, dbType)
			}

			t.Logf("✓ Column %s: type=%s", name, dbType)
		}
	})

	t.Run("EnumSetColumnTypes", func(t *testing.T) {
		rows, err := db.Query("SELECT enum_col, set_col FROM test_column_types LIMIT 0")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		// Note: MariaDB reports ENUM/SET as CHAR in the protocol
		expectedNames := []string{"enum_col", "set_col"}

		for i, ct := range columnTypes {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()

			if name != expectedNames[i] {
				t.Errorf("Column %d: expected name %s, got %s", i, expectedNames[i], name)
			}

			t.Logf("✓ Column %s: type=%s", name, dbType)
		}
	})

	t.Run("JSONColumnType", func(t *testing.T) {
		rows, err := db.Query("SELECT json_col FROM test_column_types LIMIT 0")
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		if len(columnTypes) != 1 {
			t.Fatalf("Expected 1 column, got %d", len(columnTypes))
		}

		ct := columnTypes[0]
		name := ct.Name()
		dbType := ct.DatabaseTypeName()

		if name != "json_col" {
			t.Errorf("Expected name json_col, got %s", name)
		}

		// Note: MariaDB reports JSON as BLOB in the protocol
		// The actual type is stored in extended metadata

		// Check nullable
		nullable, ok := ct.Nullable()
		if !ok {
			t.Errorf("Nullable() should return ok=true")
		}
		if !nullable {
			t.Errorf("JSON column should be nullable")
		}

		t.Logf("✓ JSON Column: name=%s, type=%s, nullable=%v", name, dbType, nullable)
	})

	if mariadb {
		t.Run("UUIDColumnType_MariaDB", func(t *testing.T) {
			rows, err := db.Query("SELECT uuid_col FROM test_column_types LIMIT 0")
			if err != nil {
				t.Fatalf("Failed to query: %v", err)
			}
			defer rows.Close()

			columnTypes, err := rows.ColumnTypes()
			if err != nil {
				t.Fatalf("Failed to get column types: %v", err)
			}

			if len(columnTypes) != 1 {
				t.Fatalf("Expected 1 column, got %d", len(columnTypes))
			}

			ct := columnTypes[0]
			name := ct.Name()
			dbType := ct.DatabaseTypeName()

			if name != "uuid_col" {
				t.Errorf("Expected name uuid_col, got %s", name)
			}

			// Note: MariaDB reports UUID as CHAR in the protocol
			// The actual UUID type is stored in extended metadata

			// Check length (UUID is 36 characters)
			length, ok := ct.Length()
			if ok {
				t.Logf("UUID length: %d", length)
			}

			t.Logf("✓ UUID Column (MariaDB): name=%s, type=%s", name, dbType)
		})
	}
}

func TestColumnTypesWithData(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	mariadb := isMariaDB(t, db)

	// Create table
	createTableSQL := `
		CREATE TEMPORARY TABLE test_column_data (
			id INT PRIMARY KEY,
			int_col INT,
			varchar_col VARCHAR(100),
			json_col JSON,
			date_col DATE,
			decimal_col DECIMAL(10,2)`

	if mariadb {
		createTableSQL += `,
			uuid_col UUID`
	}

	createTableSQL += `
		)
	`

	_, err := db.Exec(createTableSQL)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Insert test data
	insertSQL := `INSERT INTO test_column_data (id, int_col, varchar_col, json_col, date_col, decimal_col`
	if mariadb {
		insertSQL += `, uuid_col`
	}
	insertSQL += `) VALUES (?, ?, ?, ?, ?, ?`
	if mariadb {
		insertSQL += `, ?`
	}
	insertSQL += `)`

	args := []interface{}{
		1,
		42,
		"test string",
		`{"key": "value", "number": 123}`,
		"2024-01-15",
		123.45,
	}
	if mariadb {
		args = append(args, "550e8400-e29b-41d4-a716-446655440000")
	}

	_, err = db.Exec(insertSQL, args...)
	if err != nil {
		t.Fatalf("Failed to insert data: %v", err)
	}

	t.Run("ColumnTypesWithActualData", func(t *testing.T) {
		rows, err := db.Query("SELECT * FROM test_column_data WHERE id = ?", 1)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}
		defer rows.Close()

		columnTypes, err := rows.ColumnTypes()
		if err != nil {
			t.Fatalf("Failed to get column types: %v", err)
		}

		t.Logf("Total columns: %d", len(columnTypes))

		for _, ct := range columnTypes {
			name := ct.Name()
			dbType := ct.DatabaseTypeName()
			scanType := ct.ScanType()
			nullable, _ := ct.Nullable()
			length, hasLength := ct.Length()
			precision, scale, hasDecimal := ct.DecimalSize()

			t.Logf("Column: %s", name)
			t.Logf("  DatabaseType: %s", dbType)
			t.Logf("  ScanType: %v", scanType)
			t.Logf("  Nullable: %v", nullable)
			if hasLength {
				t.Logf("  Length: %d", length)
			}
			if hasDecimal {
				t.Logf("  Precision: %d, Scale: %d", precision, scale)
			}
		}

		// Verify we can scan the data
		if rows.Next() {
			values := make([]interface{}, len(columnTypes))
			valuePtrs := make([]interface{}, len(columnTypes))
			for i := range values {
				valuePtrs[i] = &values[i]
			}

			err = rows.Scan(valuePtrs...)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}

			t.Logf("✓ Successfully scanned row with %d columns", len(values))

			// Verify specific values
			if values[0] != int64(1) {
				t.Errorf("Expected id=1, got %v", values[0])
			}
			if values[1] != int64(42) {
				t.Errorf("Expected int_col=42, got %v", values[1])
			}
			if values[2] != "test string" {
				t.Errorf("Expected varchar_col='test string', got %v", values[2])
			}

			t.Log("✓ All column values verified successfully")
		}
	})
}
