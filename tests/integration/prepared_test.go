// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"testing"
)

// TestPreparedQuerySingleRow verifies that a prepared SELECT returning one row
// works correctly via the binary protocol. On the second and subsequent calls
// the server may skip column metadata (CACHE_METADATA).
func TestPreparedQuerySingleRow(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	stmt, err := db.Prepare("SELECT 42, 'hello'")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	for i := 0; i < 3; i++ {
		var n int
		var s string
		if err := stmt.QueryRow().Scan(&n, &s); err != nil {
			t.Fatalf("iteration %d: QueryRow.Scan: %v", i, err)
		}
		if n != 42 {
			t.Errorf("iteration %d: col1: got %d, want 42", i, n)
		}
		if s != "hello" {
			t.Errorf("iteration %d: col2: got %q, want \"hello\"", i, s)
		}
	}
}

// TestPreparedQueryMultiRow verifies that streaming a prepared result set
// works on repeated executions (CACHE_METADATA path).
func TestPreparedQueryMultiRow(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS prep_multi") //nolint:errcheck
	_, err := db.Exec("CREATE TABLE prep_multi (id INT, val VARCHAR(50))")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS prep_multi") //nolint:errcheck

	for i := 1; i <= 5; i++ {
		if _, err := db.Exec("INSERT INTO prep_multi VALUES (?, ?)", i, "v"); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	stmt, err := db.Prepare("SELECT id, val FROM prep_multi ORDER BY id")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	for iter := 0; iter < 3; iter++ {
		rows, err := stmt.Query()
		if err != nil {
			t.Fatalf("iter %d: Query: %v", iter, err)
		}
		n := 0
		for rows.Next() {
			var id int
			var val string
			if err := rows.Scan(&id, &val); err != nil {
				rows.Close()
				t.Fatalf("iter %d: Scan: %v", iter, err)
			}
			n++
			if id != n {
				t.Errorf("iter %d: row %d: id got %d, want %d", iter, n, id, n)
			}
			if val != "v" {
				t.Errorf("iter %d: row %d: val got %q, want \"v\"", iter, n, val)
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("iter %d: rows.Err: %v", iter, err)
		}
		rows.Close()
		if n != 5 {
			t.Errorf("iter %d: got %d rows, want 5", iter, n)
		}
	}
}

// TestPreparedQueryWithParams verifies parameter binding works with the
// binary protocol across multiple executions.
func TestPreparedQueryWithParams(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS prep_params") //nolint:errcheck
	_, err := db.Exec("CREATE TABLE prep_params (id INT, name VARCHAR(100))")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS prep_params") //nolint:errcheck

	names := []string{"Alice", "Bob", "Carol"}
	for i, name := range names {
		if _, err := db.Exec("INSERT INTO prep_params VALUES (?, ?)", i+1, name); err != nil {
			t.Fatalf("INSERT: %v", err)
		}
	}

	stmt, err := db.Prepare("SELECT name FROM prep_params WHERE id = ?")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	for i, want := range names {
		var got string
		if err := stmt.QueryRow(i + 1).Scan(&got); err != nil {
			t.Fatalf("id=%d: QueryRow.Scan: %v", i+1, err)
		}
		if got != want {
			t.Errorf("id=%d: got %q, want %q", i+1, got, want)
		}
	}
}

// TestPreparedExecReturnsResult verifies that prepared DML returns correct
// LastInsertId and RowsAffected.
func TestPreparedExecReturnsResult(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	db.Exec("DROP TABLE IF EXISTS prep_exec") //nolint:errcheck
	_, err := db.Exec("CREATE TABLE prep_exec (id INT AUTO_INCREMENT PRIMARY KEY, v INT)")
	if err != nil {
		t.Fatalf("CREATE TABLE: %v", err)
	}
	defer db.Exec("DROP TABLE IF EXISTS prep_exec") //nolint:errcheck

	stmt, err := db.Prepare("INSERT INTO prep_exec (v) VALUES (?)")
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	defer stmt.Close()

	for i := 1; i <= 3; i++ {
		res, err := stmt.Exec(i * 10)
		if err != nil {
			t.Fatalf("Exec %d: %v", i, err)
		}
		ra, _ := res.RowsAffected()
		if ra != 1 {
			t.Errorf("Exec %d: RowsAffected got %d, want 1", i, ra)
		}
		id, _ := res.LastInsertId()
		if id != int64(i) {
			t.Errorf("Exec %d: LastInsertId got %d, want %d", i, id, i)
		}
	}
}
