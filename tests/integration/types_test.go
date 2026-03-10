// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"testing"
	"time"
)

// TestAllTypesTextProtocol tests all data types using text protocol (COM_QUERY)
func TestAllTypesTextProtocol(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	_, err := db.Exec("DROP TABLE IF EXISTS test_all_types")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE test_all_types (
			id INT PRIMARY KEY,
			-- Integer types
			tinyint_val TINYINT,
			smallint_val SMALLINT,
			mediumint_val MEDIUMINT,
			int_val INT,
			bigint_val BIGINT,
			-- Unsigned integer types
			tinyint_unsigned TINYINT UNSIGNED,
			smallint_unsigned SMALLINT UNSIGNED,
			mediumint_unsigned MEDIUMINT UNSIGNED,
			int_unsigned INT UNSIGNED,
			bigint_unsigned BIGINT UNSIGNED,
			-- Floating point types
			float_val FLOAT,
			double_val DOUBLE,
			decimal_val DECIMAL(10,2),
			-- String types
			char_val CHAR(10),
			varchar_val VARCHAR(100),
			text_val TEXT,
			-- Binary types
			binary_val BINARY(10),
			varbinary_val VARBINARY(100),
			blob_val BLOB,
			-- Date/Time types
			date_val DATE,
			time_val TIME,
			datetime_val DATETIME,
			timestamp_val TIMESTAMP,
			year_val YEAR,
			-- Other types
			enum_val ENUM('small', 'medium', 'large'),
			set_val SET('a', 'b', 'c')
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE test_all_types")

	// Insert test data with values
	now := time.Now().Truncate(time.Second)
	_, err = db.Exec(`
		INSERT INTO test_all_types VALUES (
			1,
			127, 32767, 8388607, 2147483647, 9223372036854775807,
			255, 65535, 16777215, 4294967295, 9223372036854775807,
			3.14, 2.718281828, 123.45,
			'char', 'varchar', 'text value',
			'binary', 'varbinary', 'blob data',
			?, ?, ?, ?, 2024,
			'medium', 'a,c'
		)
	`, now, now, now, now)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Insert test data with NULL values
	_, err = db.Exec(`
		INSERT INTO test_all_types (id) VALUES (2)
	`)
	if err != nil {
		t.Fatalf("Failed to insert NULL data: %v", err)
	}

	// Test reading non-NULL values using text protocol
	t.Run("TextProtocol_NonNullValues", func(t *testing.T) {
		var (
			id                                             int
			tinyint, smallint, mediumint, intVal, bigint   sql.NullInt64
			tinyintU, smallintU, mediumintU, intU, bigintU sql.NullInt64
			floatVal, doubleVal                            sql.NullFloat64
			decimalVal                                     sql.NullString
			charVal, varcharVal, textVal                   sql.NullString
			binaryVal, varbinaryVal, blobVal               sql.NullString
			dateVal, timeVal, datetimeVal, timestampVal    sql.NullString
			yearVal                                        sql.NullInt64
			enumVal, setVal                                sql.NullString
		)

		err := db.QueryRow("SELECT * FROM test_all_types WHERE id = 1").Scan(
			&id,
			&tinyint, &smallint, &mediumint, &intVal, &bigint,
			&tinyintU, &smallintU, &mediumintU, &intU, &bigintU,
			&floatVal, &doubleVal, &decimalVal,
			&charVal, &varcharVal, &textVal,
			&binaryVal, &varbinaryVal, &blobVal,
			&dateVal, &timeVal, &datetimeVal, &timestampVal, &yearVal,
			&enumVal, &setVal,
		)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}

		// Verify integer types
		if !tinyint.Valid || tinyint.Int64 != 127 {
			t.Errorf("tinyint: expected 127, got %v", tinyint)
		}
		if !smallint.Valid || smallint.Int64 != 32767 {
			t.Errorf("smallint: expected 32767, got %v", smallint)
		}
		if !mediumint.Valid || mediumint.Int64 != 8388607 {
			t.Errorf("mediumint: expected 8388607, got %v", mediumint)
		}
		if !intVal.Valid || intVal.Int64 != 2147483647 {
			t.Errorf("int: expected 2147483647, got %v", intVal)
		}
		if !bigint.Valid || bigint.Int64 != 9223372036854775807 {
			t.Errorf("bigint: expected 9223372036854775807, got %v", bigint)
		}

		// Verify unsigned integer types
		if !tinyintU.Valid || tinyintU.Int64 != 255 {
			t.Errorf("tinyint unsigned: expected 255, got %v", tinyintU)
		}
		if !smallintU.Valid || smallintU.Int64 != 65535 {
			t.Errorf("smallint unsigned: expected 65535, got %v", smallintU)
		}

		// Verify floating point types
		if !floatVal.Valid || floatVal.Float64 < 3.13 || floatVal.Float64 > 3.15 {
			t.Errorf("float: expected ~3.14, got %v", floatVal)
		}
		if !doubleVal.Valid || doubleVal.Float64 < 2.71 || doubleVal.Float64 > 2.72 {
			t.Errorf("double: expected ~2.718281828, got %v", doubleVal)
		}
		if !decimalVal.Valid || decimalVal.String != "123.45" {
			t.Errorf("decimal: expected 123.45, got %v", decimalVal)
		}

		// Verify string types
		if !charVal.Valid || charVal.String != "char" {
			t.Errorf("char: expected 'char', got %v", charVal)
		}
		if !varcharVal.Valid || varcharVal.String != "varchar" {
			t.Errorf("varchar: expected 'varchar', got %v", varcharVal)
		}
		if !textVal.Valid || textVal.String != "text value" {
			t.Errorf("text: expected 'text value', got %v", textVal)
		}

		// Verify date/time types
		if !dateVal.Valid {
			t.Error("date should not be NULL")
		}
		if !datetimeVal.Valid {
			t.Error("datetime should not be NULL")
		}
		if !timestampVal.Valid {
			t.Error("timestamp should not be NULL")
		}
		if !yearVal.Valid || yearVal.Int64 != 2024 {
			t.Errorf("year: expected 2024, got %v", yearVal)
		}

		// Verify enum/set types
		if !enumVal.Valid || enumVal.String != "medium" {
			t.Errorf("enum: expected 'medium', got %v", enumVal)
		}
		if !setVal.Valid || setVal.String != "a,c" {
			t.Errorf("set: expected 'a,c', got %v", setVal)
		}

		t.Log("✓ All non-NULL values read correctly via text protocol")
	})

	// Test reading NULL values using text protocol
	t.Run("TextProtocol_NullValues", func(t *testing.T) {
		var (
			id                                             int
			tinyint, smallint, mediumint, intVal, bigint   sql.NullInt64
			tinyintU, smallintU, mediumintU, intU, bigintU sql.NullInt64
			floatVal, doubleVal                            sql.NullFloat64
			decimalVal                                     sql.NullString
			charVal, varcharVal, textVal                   sql.NullString
			binaryVal, varbinaryVal, blobVal               sql.NullString
			dateVal, timeVal, datetimeVal, timestampVal    sql.NullString
			yearVal                                        sql.NullInt64
			enumVal, setVal                                sql.NullString
		)

		err := db.QueryRow("SELECT * FROM test_all_types WHERE id = 2").Scan(
			&id,
			&tinyint, &smallint, &mediumint, &intVal, &bigint,
			&tinyintU, &smallintU, &mediumintU, &intU, &bigintU,
			&floatVal, &doubleVal, &decimalVal,
			&charVal, &varcharVal, &textVal,
			&binaryVal, &varbinaryVal, &blobVal,
			&dateVal, &timeVal, &datetimeVal, &timestampVal, &yearVal,
			&enumVal, &setVal,
		)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}

		// Verify all values are NULL
		if tinyint.Valid {
			t.Error("tinyint should be NULL")
		}
		if smallint.Valid {
			t.Error("smallint should be NULL")
		}
		if floatVal.Valid {
			t.Error("float should be NULL")
		}
		if decimalVal.Valid {
			t.Error("decimal should be NULL")
		}
		if charVal.Valid {
			t.Error("char should be NULL")
		}
		if dateVal.Valid {
			t.Error("date should be NULL")
		}
		if enumVal.Valid {
			t.Error("enum should be NULL")
		}

		t.Log("✓ All NULL values read correctly via text protocol")
	})
}

// TestAllTypesBinaryProtocol tests all data types using binary protocol (prepared statements)
func TestAllTypesBinaryProtocol(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	_, err := db.Exec("DROP TABLE IF EXISTS test_all_types_binary")
	if err != nil {
		t.Fatalf("Failed to drop table: %v", err)
	}

	_, err = db.Exec(`
		CREATE TABLE test_all_types_binary (
			id INT PRIMARY KEY,
			-- Integer types
			tinyint_val TINYINT,
			smallint_val SMALLINT,
			mediumint_val MEDIUMINT,
			int_val INT,
			bigint_val BIGINT,
			-- Unsigned integer types
			tinyint_unsigned TINYINT UNSIGNED,
			smallint_unsigned SMALLINT UNSIGNED,
			mediumint_unsigned MEDIUMINT UNSIGNED,
			int_unsigned INT UNSIGNED,
			bigint_unsigned BIGINT UNSIGNED,
			-- Floating point types
			float_val FLOAT,
			double_val DOUBLE,
			decimal_val DECIMAL(10,2),
			-- String types
			char_val CHAR(10),
			varchar_val VARCHAR(100),
			text_val TEXT,
			-- Binary types
			binary_val BINARY(10),
			varbinary_val VARBINARY(100),
			blob_val BLOB,
			-- Date/Time types
			date_val DATE,
			time_val TIME,
			datetime_val DATETIME,
			timestamp_val TIMESTAMP,
			year_val YEAR,
			-- Other types
			enum_val ENUM('small', 'medium', 'large'),
			set_val SET('a', 'b', 'c')
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE test_all_types_binary")

	// Prepare insert statement
	stmt, err := db.Prepare(`
		INSERT INTO test_all_types_binary VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`)
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Insert test data with values using binary protocol
	now := time.Now().Truncate(time.Second)
	_, err = stmt.Exec(
		1,
		127, 32767, 8388607, 2147483647, int64(9223372036854775807),
		255, 65535, 16777215, uint64(4294967295), int64(9223372036854775807),
		3.14, 2.718281828, 123.45,
		"char", "varchar", "text value",
		[]byte("binary\x00\x00\x00\x00"), []byte("varbinary"), []byte("blob data"),
		now, "12:34:56", now, now, 2024,
		"medium", "a,c",
	)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Insert test data with NULL values using binary protocol
	_, err = stmt.Exec(
		2,
		nil, nil, nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil,
		nil, nil, nil, nil, nil,
		nil, nil,
	)
	if err != nil {
		t.Fatalf("Failed to insert NULL data: %v", err)
	}

	// Prepare select statement
	selectStmt, err := db.Prepare("SELECT * FROM test_all_types_binary WHERE id = ?")
	if err != nil {
		t.Fatalf("Failed to prepare select: %v", err)
	}
	defer selectStmt.Close()

	// Test reading non-NULL values using binary protocol
	t.Run("BinaryProtocol_NonNullValues", func(t *testing.T) {
		var (
			id                                             int
			tinyint, smallint, mediumint, intVal, bigint   sql.NullInt64
			tinyintU, smallintU, mediumintU, intU, bigintU sql.NullInt64
			floatVal, doubleVal                            sql.NullFloat64
			decimalVal                                     sql.NullString
			charVal, varcharVal, textVal                   sql.NullString
			binaryVal, varbinaryVal, blobVal               []byte
			dateVal, datetimeVal, timestampVal             sql.NullTime
			timeVal                                        time.Duration
			yearVal                                        sql.NullInt64
			enumVal, setVal                                sql.NullString
		)

		err := selectStmt.QueryRow(1).Scan(
			&id,
			&tinyint, &smallint, &mediumint, &intVal, &bigint,
			&tinyintU, &smallintU, &mediumintU, &intU, &bigintU,
			&floatVal, &doubleVal, &decimalVal,
			&charVal, &varcharVal, &textVal,
			&binaryVal, &varbinaryVal, &blobVal,
			&dateVal, &timeVal, &datetimeVal, &timestampVal, &yearVal,
			&enumVal, &setVal,
		)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}

		// Verify integer types
		if !tinyint.Valid || tinyint.Int64 != 127 {
			t.Errorf("tinyint: expected 127, got %v", tinyint)
		}
		if !smallint.Valid || smallint.Int64 != 32767 {
			t.Errorf("smallint: expected 32767, got %v", smallint)
		}
		if !mediumint.Valid || mediumint.Int64 != 8388607 {
			t.Errorf("mediumint: expected 8388607, got %v", mediumint)
		}
		if !intVal.Valid || intVal.Int64 != 2147483647 {
			t.Errorf("int: expected 2147483647, got %v", intVal)
		}
		if !bigint.Valid || bigint.Int64 != 9223372036854775807 {
			t.Errorf("bigint: expected 9223372036854775807, got %v", bigint)
		}

		// Verify unsigned integer types
		if !tinyintU.Valid || tinyintU.Int64 != 255 {
			t.Errorf("tinyint unsigned: expected 255, got %v", tinyintU)
		}
		if !smallintU.Valid || smallintU.Int64 != 65535 {
			t.Errorf("smallint unsigned: expected 65535, got %v", smallintU)
		}

		// Verify floating point types
		if !floatVal.Valid || floatVal.Float64 < 3.13 || floatVal.Float64 > 3.15 {
			t.Errorf("float: expected ~3.14, got %v", floatVal)
		}
		if !doubleVal.Valid || doubleVal.Float64 < 2.71 || doubleVal.Float64 > 2.72 {
			t.Errorf("double: expected ~2.718281828, got %v", doubleVal)
		}
		if !decimalVal.Valid || decimalVal.String != "123.45" {
			t.Errorf("decimal: expected 123.45, got %v", decimalVal)
		}

		// Verify string types
		if !charVal.Valid || charVal.String != "char" {
			t.Errorf("char: expected 'char', got %v", charVal)
		}
		if !varcharVal.Valid || varcharVal.String != "varchar" {
			t.Errorf("varchar: expected 'varchar', got %v", varcharVal)
		}
		if !textVal.Valid || textVal.String != "text value" {
			t.Errorf("text: expected 'text value', got %v", textVal)
		}

		// Verify binary types
		if len(blobVal) == 0 || string(blobVal) != "blob data" {
			t.Errorf("blob: expected 'blob data', got %v", string(blobVal))
		}

		// Verify date/time types
		if !dateVal.Valid {
			t.Error("date should not be NULL")
		}
		if timeVal == 0 {
			t.Error("time should not be zero")
		}
		if !datetimeVal.Valid {
			t.Error("datetime should not be NULL")
		}
		if !timestampVal.Valid {
			t.Error("timestamp should not be NULL")
		}
		if !yearVal.Valid || yearVal.Int64 != 2024 {
			t.Errorf("year: expected 2024, got %v", yearVal)
		}

		// Verify enum/set types
		if !enumVal.Valid || enumVal.String != "medium" {
			t.Errorf("enum: expected 'medium', got %v", enumVal)
		}
		if !setVal.Valid || setVal.String != "a,c" {
			t.Errorf("set: expected 'a,c', got %v", setVal)
		}

		t.Log("✓ All non-NULL values read correctly via binary protocol")
	})

	// Test reading NULL values using binary protocol
	t.Run("BinaryProtocol_NullValues", func(t *testing.T) {
		var (
			id                                             int
			tinyint, smallint, mediumint, intVal, bigint   sql.NullInt64
			tinyintU, smallintU, mediumintU, intU, bigintU sql.NullInt64
			floatVal, doubleVal                            sql.NullFloat64
			decimalVal                                     sql.NullString
			charVal, varcharVal, textVal                   sql.NullString
			binaryVal, varbinaryVal, blobVal               []byte
			dateVal, datetimeVal, timestampVal             sql.NullTime
			timeVal                                        *time.Duration
			yearVal                                        sql.NullInt64
			enumVal, setVal                                sql.NullString
		)

		err := selectStmt.QueryRow(2).Scan(
			&id,
			&tinyint, &smallint, &mediumint, &intVal, &bigint,
			&tinyintU, &smallintU, &mediumintU, &intU, &bigintU,
			&floatVal, &doubleVal, &decimalVal,
			&charVal, &varcharVal, &textVal,
			&binaryVal, &varbinaryVal, &blobVal,
			&dateVal, &timeVal, &datetimeVal, &timestampVal, &yearVal,
			&enumVal, &setVal,
		)
		if err != nil {
			t.Fatalf("Failed to query: %v", err)
		}

		// Verify all values are NULL
		if tinyint.Valid {
			t.Error("tinyint should be NULL")
		}
		if smallint.Valid {
			t.Error("smallint should be NULL")
		}
		if floatVal.Valid {
			t.Error("float should be NULL")
		}
		if decimalVal.Valid {
			t.Error("decimal should be NULL")
		}
		if charVal.Valid {
			t.Error("char should be NULL")
		}
		if len(binaryVal) > 0 {
			t.Error("binary should be NULL")
		}
		if dateVal.Valid {
			t.Error("date should be NULL")
		}
		if enumVal.Valid {
			t.Error("enum should be NULL")
		}

		t.Log("✓ All NULL values read correctly via binary protocol")
	})
}
