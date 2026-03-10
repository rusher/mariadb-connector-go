// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	_ "github.com/mariadb-connector-go/mariadb"
)

func TestConnection(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()
}

func TestSimpleQuery(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	var result int
	err := db.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatalf("Failed to execute query: %v", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}
}

func TestCreateTable(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Drop table if exists
	db.Exec("DROP TABLE IF EXISTS test_users")

	// Create table
	_, err := db.Exec(`
		CREATE TABLE test_users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) NOT NULL,
			age INT,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	// Cleanup
	defer db.Exec("DROP TABLE test_users")
}

func TestInsertAndSelect(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	db.Exec("DROP TABLE IF EXISTS test_users")
	_, err := db.Exec(`
		CREATE TABLE test_users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			email VARCHAR(100) NOT NULL,
			age INT
		)
	`)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}
	defer db.Exec("DROP TABLE test_users")

	// Insert
	result, err := db.Exec("INSERT INTO test_users (name, email, age) VALUES (?, ?, ?)",
		"John Doe", "john@example.com", 30)
	if err != nil {
		t.Fatalf("Failed to insert: %v", err)
	}

	id, err := result.LastInsertId()
	if err != nil {
		t.Fatalf("Failed to get last insert ID: %v", err)
	}

	affected, err := result.RowsAffected()
	if err != nil {
		t.Fatalf("Failed to get rows affected: %v", err)
	}

	if affected != 1 {
		t.Errorf("Expected 1 row affected, got %d", affected)
	}

	// Select
	var name, email string
	var age int
	err = db.QueryRow("SELECT name, email, age FROM test_users WHERE id = ?", id).
		Scan(&name, &email, &age)
	if err != nil {
		t.Fatalf("Failed to select: %v", err)
	}

	if name != "John Doe" || email != "john@example.com" || age != 30 {
		t.Errorf("Data mismatch: got (%s, %s, %d), want (John Doe, john@example.com, 30)",
			name, email, age)
	}
}

func TestPreparedStatement(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	db.Exec("DROP TABLE IF EXISTS test_users")
	db.Exec(`
		CREATE TABLE test_users (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			age INT
		)
	`)
	defer db.Exec("DROP TABLE test_users")

	// Prepare statement
	stmt, err := db.Prepare("INSERT INTO test_users (name, age) VALUES (?, ?)")
	if err != nil {
		t.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	// Execute multiple times
	users := []struct {
		name string
		age  int
	}{
		{"Alice", 25},
		{"Bob", 30},
		{"Carol", 35},
	}

	for _, user := range users {
		_, err := stmt.Exec(user.name, user.age)
		if err != nil {
			t.Fatalf("Failed to execute prepared statement: %v", err)
		}
	}

	// Verify
	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM test_users").Scan(&count)
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != len(users) {
		t.Errorf("Expected %d rows, got %d", len(users), count)
	}
}

func TestTransaction(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	db.Exec("DROP TABLE IF EXISTS test_accounts")
	db.Exec(`
		CREATE TABLE test_accounts (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			balance DECIMAL(10, 2) NOT NULL
		)
	`)
	defer db.Exec("DROP TABLE test_accounts")

	db.Exec("INSERT INTO test_accounts (name, balance) VALUES (?, ?)", "Alice", 1000.00)
	db.Exec("INSERT INTO test_accounts (name, balance) VALUES (?, ?)", "Bob", 500.00)

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Transfer money
	_, err = tx.Exec("UPDATE test_accounts SET balance = balance - ? WHERE name = ?", 200.00, "Alice")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to deduct from Alice: %v", err)
	}

	_, err = tx.Exec("UPDATE test_accounts SET balance = balance + ? WHERE name = ?", 200.00, "Bob")
	if err != nil {
		tx.Rollback()
		t.Fatalf("Failed to add to Bob: %v", err)
	}

	// Commit
	if err := tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	// Verify
	var aliceBalance, bobBalance float64
	db.QueryRow("SELECT balance FROM test_accounts WHERE name = ?", "Alice").Scan(&aliceBalance)
	db.QueryRow("SELECT balance FROM test_accounts WHERE name = ?", "Bob").Scan(&bobBalance)

	if aliceBalance != 800.00 {
		t.Errorf("Alice's balance: expected 800.00, got %.2f", aliceBalance)
	}
	if bobBalance != 700.00 {
		t.Errorf("Bob's balance: expected 700.00, got %.2f", bobBalance)
	}
}

func TestTransactionRollback(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	db.Exec("DROP TABLE IF EXISTS test_accounts")
	db.Exec(`
		CREATE TABLE test_accounts (
			id INT AUTO_INCREMENT PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			balance DECIMAL(10, 2) NOT NULL
		)
	`)
	defer db.Exec("DROP TABLE test_accounts")

	db.Exec("INSERT INTO test_accounts (name, balance) VALUES (?, ?)", "Alice", 1000.00)

	// Start transaction
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Update balance
	tx.Exec("UPDATE test_accounts SET balance = balance - ? WHERE name = ?", 500.00, "Alice")

	// Rollback
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}

	// Verify balance unchanged
	var balance float64
	db.QueryRow("SELECT balance FROM test_accounts WHERE name = ?", "Alice").Scan(&balance)

	if balance != 1000.00 {
		t.Errorf("Balance should be unchanged: expected 1000.00, got %.2f", balance)
	}
}

func TestContextTimeout(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	// This should timeout
	time.Sleep(10 * time.Millisecond)
	err := db.PingContext(ctx)
	if err == nil {
		t.Error("Expected timeout error but got none")
	}
}

func TestMultipleRows(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	db.Exec("DROP TABLE IF EXISTS test_data")
	db.Exec("CREATE TABLE test_data (id INT, value VARCHAR(50))")
	defer db.Exec("DROP TABLE test_data")

	// Insert multiple rows
	for i := 1; i <= 10; i++ {
		db.Exec("INSERT INTO test_data (id, value) VALUES (?, ?)", i, fmt.Sprintf("value%d", i))
	}

	// Query multiple rows
	rows, err := db.Query("SELECT id, value FROM test_data ORDER BY id")
	if err != nil {
		t.Fatalf("Failed to query: %v", err)
	}
	defer rows.Close()

	count := 0
	for rows.Next() {
		var id int
		var value string
		if err := rows.Scan(&id, &value); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}
		count++

		expectedValue := fmt.Sprintf("value%d", id)
		if value != expectedValue {
			t.Errorf("Row %d: expected %s, got %s", id, expectedValue, value)
		}
	}

	if err := rows.Err(); err != nil {
		t.Fatalf("Rows error: %v", err)
	}

	if count != 10 {
		t.Errorf("Expected 10 rows, got %d", count)
	}
}

func TestNullValues(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	db.Exec("DROP TABLE IF EXISTS test_nulls")
	db.Exec("CREATE TABLE test_nulls (id INT, nullable_value VARCHAR(50))")
	defer db.Exec("DROP TABLE test_nulls")

	// Insert NULL value
	_, err := db.Exec("INSERT INTO test_nulls (id, nullable_value) VALUES (?, ?)", 1, nil)
	if err != nil {
		t.Fatalf("Failed to insert NULL: %v", err)
	}

	// Query NULL value
	var id int
	var value sql.NullString
	err = db.QueryRow("SELECT id, nullable_value FROM test_nulls WHERE id = ?", 1).Scan(&id, &value)
	if err != nil {
		t.Fatalf("Failed to query NULL: %v", err)
	}

	if value.Valid {
		t.Error("Expected NULL value but got valid value")
	}
}

func TestDataTypes(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Setup
	db.Exec("DROP TABLE IF EXISTS test_types")
	db.Exec(`
		CREATE TABLE test_types (
			id INT,
			int_val INT,
			bigint_val BIGINT,
			float_val FLOAT,
			double_val DOUBLE,
			decimal_val DECIMAL(10,2),
			varchar_val VARCHAR(100),
			text_val TEXT,
			blob_val BLOB,
			date_val DATE,
			datetime_val DATETIME,
			timestamp_val TIMESTAMP
		)
	`)
	defer db.Exec("DROP TABLE test_types")

	// Insert test data
	now := time.Now()
	_, err := db.Exec(`
		INSERT INTO test_types 
		(id, int_val, bigint_val, float_val, double_val, decimal_val, varchar_val, text_val, blob_val, date_val, datetime_val, timestamp_val)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		1, 42, 9223372036854775807, 3.14, 2.718281828, 123.45, "test string", "long text", []byte("binary data"),
		now, now, now,
	)
	if err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Query and verify
	var id, intVal int
	var bigintVal int64
	var floatVal float32
	var doubleVal float64
	var decimalVal string
	var varcharVal, textVal string
	var blobVal []byte
	var dateVal, datetimeVal, timestampVal time.Time

	err = db.QueryRow("SELECT * FROM test_types WHERE id = ?", 1).Scan(
		&id, &intVal, &bigintVal, &floatVal, &doubleVal, &decimalVal,
		&varcharVal, &textVal, &blobVal, &dateVal, &datetimeVal, &timestampVal,
	)
	if err != nil {
		t.Fatalf("Failed to query test data: %v", err)
	}

	if intVal != 42 {
		t.Errorf("int_val: expected 42, got %d", intVal)
	}
	if varcharVal != "test string" {
		t.Errorf("varchar_val: expected 'test string', got %s", varcharVal)
	}
}

func BenchmarkSimpleQuery(b *testing.B) {
	cfg := GetTestConfig()
	db, err := sql.Open("mariadb", cfg.DSN)
	if err != nil {
		b.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result int
		db.QueryRow("SELECT 1").Scan(&result)
	}
}

func BenchmarkPreparedStatement(b *testing.B) {
	cfg := GetTestConfig()
	db, err := sql.Open("mariadb", cfg.DSN)
	if err != nil {
		b.Fatalf("Failed to open connection: %v", err)
	}
	defer db.Close()

	stmt, err := db.Prepare("SELECT ?")
	if err != nil {
		b.Fatalf("Failed to prepare: %v", err)
	}
	defer stmt.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result int
		stmt.QueryRow(1).Scan(&result)
	}
}
