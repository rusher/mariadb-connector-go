// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"fmt"
	"strings"
	"testing"

	_ "github.com/mariadb-connector-go/mariadb"
)

// requireMariaDB skips the test if the connected server is not MariaDB.
func requireMariaDB(t *testing.T, db *sql.DB) {
	t.Helper()
	var version string
	if err := db.QueryRow("SELECT @@version_comment").Scan(&version); err != nil {
		t.Skipf("could not determine server type: %v", err)
	}
	if !strings.Contains(strings.ToLower(version), "mariadb") {
		t.Skipf("test requires MariaDB, got: %s", version)
	}
}

// requireSequenceEngine skips the test if the MariaDB SEQUENCE engine is unavailable.
func requireSequenceEngine(t *testing.T, db *sql.DB) {
	t.Helper()
	var n int
	if err := db.QueryRow("SELECT 1 FROM seq_1_to_1 LIMIT 1").Scan(&n); err != nil {
		t.Skipf("SEQUENCE engine not available: %v", err)
	}
}

// dsnWithParams appends DSN parameters to a base DSN string.
func dsnWithParams(base string, params ...string) string {
	for _, p := range params {
		if containsParams(base) {
			base += "&" + p
		} else {
			base += "?" + p
		}
	}
	return base
}

// TestMultiResultSetSequence runs a three-statement query against MariaDB:
//
//	SELECT * FROM seq_1_to_9   → 9 rows (values 1-9)
//	DO 1                        → no result set
//	SELECT * FROM seq_10_to_19 → 10 rows (values 10-19)
//
// The test is repeated for each fetchSize value to exercise the streaming /
// batch-loading code paths. HasNextResultSet is exercised through the bool
// return value of sql.Rows.NextResultSet().
func TestMultiResultSetSequence(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	requireMariaDB(t, db)
	requireSequenceEngine(t, db)

	fetchSizes := []int{9, 10, 11}
	baseDSN := GetTestConfig().DSN

	for _, fetchSize := range fetchSizes {
		fetchSize := fetchSize
		t.Run(fmt.Sprintf("fetchSize=%d", fetchSize), func(t *testing.T) {
			dsn := dsnWithParams(baseDSN,
				"multiStatements=true",
				fmt.Sprintf("fetchSize=%d", fetchSize),
			)
			mdb, err := sql.Open("mariadb", dsn)
			if err != nil {
				t.Fatalf("sql.Open: %v", err)
			}
			defer mdb.Close()

			const query = "SELECT * FROM seq_1_to_9; DO 1; SELECT * FROM seq_10_to_19"
			rows, err := mdb.Query(query)
			if err != nil {
				t.Fatalf("Query: %v", err)
			}
			defer rows.Close()

			// ── First result set: seq_1_to_9 ────────────────────────────────
			var got []int
			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					t.Fatalf("Scan (first set): %v", err)
				}
				got = append(got, v)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("Err after first set: %v", err)
			}
			if len(got) != 9 {
				t.Errorf("first result set: got %d rows, want 9", len(got))
			}
			for i, v := range got {
				if want := i + 1; v != want {
					t.Errorf("first set row[%d]: got %d, want %d", i, v, want)
				}
			}

			// HasNextResultSet should be true — DO 1 has no rows but
			// seq_10_to_19 follows it and does. NextResultSet skips DO 1
			// automatically and lands on seq_10_to_19.
			if !rows.NextResultSet() {
				t.Fatalf("NextResultSet after first set: expected true (seq_10_to_19 follows), err=%v", rows.Err())
			}

			// ── Second result set: seq_10_to_19 ─────────────────────────────
			got = got[:0]
			for rows.Next() {
				var v int
				if err := rows.Scan(&v); err != nil {
					t.Fatalf("Scan (second set): %v", err)
				}
				got = append(got, v)
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("Err after second set: %v", err)
			}
			if len(got) != 10 {
				t.Errorf("second result set: got %d rows, want 10", len(got))
			}
			for i, v := range got {
				if want := i + 10; v != want {
					t.Errorf("second set row[%d]: got %d, want %d", i, v, want)
				}
			}

			// HasNextResultSet should now be false — no more result sets.
			if rows.NextResultSet() {
				t.Fatal("NextResultSet after second set: expected false (no more result sets)")
			}
			if err := rows.Err(); err != nil {
				t.Fatalf("unexpected error after last NextResultSet: %v", err)
			}
		})
	}
}
