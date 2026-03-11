// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build benchmark

// Package benchmark compares mariadb-connector-go with go-sql-driver/mysql.
//
// Run with:
//
//	go test -tags benchmark -bench=. -benchmem ./tests/benchmark/
//
// Environment variables (same convention as the Python benchmark suite):
//
//	TEST_DB_HOST      default: 127.0.0.1
//	TEST_DB_PORT      default: 3306
//	TEST_DB_USER      default: root
//	TEST_DB_PASSWORD  default: (empty)
//	TEST_DB_DATABASE  default: test
package benchmark

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/mariadb-connector-go/mariadb"
)

// ── DSN helpers ──────────────────────────────────────────────────────────────

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func dsn() string {
	host := env("TEST_DB_HOST", "127.0.0.1")
	port := env("TEST_DB_PORT", "3306")
	user := env("TEST_DB_USER", "root")
	pass := env("TEST_DB_PASSWORD", "")
	db := env("TEST_DB_DATABASE", "testgo")
	if pass != "" {
		return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, pass, host, port, db)
	}
	return fmt.Sprintf("%s@tcp(%s:%s)/%s", user, host, port, db)
}

// ── fixtures ─────────────────────────────────────────────────────────────────

type driverFixture struct {
	name string
	db   *sql.DB
}

var drivers []driverFixture

func TestMain(m *testing.M) {
	d := dsn()

	mariaDB, err := sql.Open("mariadb", d)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mariadb open: %v\n", err)
		os.Exit(1)
	}
	mariaDB.SetMaxOpenConns(1)
	mariaDB.SetMaxIdleConns(1)

	mysqlDB, err := sql.Open("mysql", d)
	if err != nil {
		fmt.Fprintf(os.Stderr, "mysql open: %v\n", err)
		os.Exit(1)
	}
	mysqlDB.SetMaxOpenConns(1)
	mysqlDB.SetMaxIdleConns(1)

	drivers = []driverFixture{
		{name: "mariadb", db: mariaDB},
		{name: "mysql", db: mysqlDB},
	}

	setupSchema(mariaDB)

	code := m.Run()

	teardownSchema(mariaDB)
	mariaDB.Close()
	mysqlDB.Close()
	os.Exit(code)
}

func setupSchema(db *sql.DB) {
	mustExec(db, "DROP TABLE IF EXISTS bench_100cols")
	cols := make([]string, 100)
	for i := range cols {
		cols[i] = fmt.Sprintf("i%d INT NOT NULL DEFAULT %d", i+1, i+1)
	}
	mustExec(db, "CREATE TABLE bench_100cols ("+strings.Join(cols, ",")+")")
	vals := make([]string, 100)
	for i := range vals {
		vals[i] = fmt.Sprintf("%d", i+1)
	}
	mustExec(db, "INSERT INTO bench_100cols VALUES ("+strings.Join(vals, ",")+")")

	mustExec(db, "DROP TABLE IF EXISTS bench_insert")
	mustExec(db, `CREATE TABLE bench_insert (
		id INT NOT NULL AUTO_INCREMENT,
		t0  TEXT,
		PRIMARY KEY (id)
	) COLLATE='utf8mb4_unicode_ci'`)
}

func teardownSchema(db *sql.DB) {
	db.Exec("DROP TABLE IF EXISTS bench_100cols")  //nolint:errcheck
	db.Exec("DROP TABLE IF EXISTS bench_insert")   //nolint:errcheck
}

func mustExec(db *sql.DB, query string) {
	if _, err := db.Exec(query); err != nil {
		fmt.Fprintf(os.Stderr, "setup: %s\n  error: %v\n", query, err)
		os.Exit(1)
	}
}

// hasSeqEngine returns true when the MariaDB SEQUENCE engine is available.
func hasSeqEngine(db *sql.DB) bool {
	var n int
	return db.QueryRow("SELECT 1 FROM seq_1_to_1 LIMIT 1").Scan(&n) == nil
}

// ── helpers inside benchmarks ─────────────────────────────────────────────────

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// ── benchmarks ────────────────────────────────────────────────────────────────

// BenchmarkDoOne mirrors test_bench_do_1.py — plain command execution.
func BenchmarkDoOne(b *testing.B) {
	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := d.db.Exec("DO 1"); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSelectOne mirrors test_bench_select_1.py — single scalar SELECT.
func BenchmarkSelectOne(b *testing.B) {
	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				var v int
				if err := d.db.QueryRow("SELECT 1").Scan(&v); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSelect1000RowsText mirrors test_bench_select_1000_rows.py (text protocol).
// Requires the MariaDB SEQUENCE engine; skipped otherwise.
func BenchmarkSelect1000RowsText(b *testing.B) {
	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			if !hasSeqEngine(d.db) {
				b.Skip("SEQUENCE engine not available")
			}
			const query = "SELECT seq, 'abcdefghijabcdefghijabcdefghijaa' FROM seq_1_to_1000 WHERE 1 = ?"
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := d.db.Query(query, 1)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var seq int
					var s string
					if err := rows.Scan(&seq, &s); err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}

// BenchmarkSelect1000RowsBinary mirrors test_bench_select_1000_rows.py (binary / prepared protocol).
func BenchmarkSelect1000RowsBinary(b *testing.B) {
	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			if !hasSeqEngine(d.db) {
				b.Skip("SEQUENCE engine not available")
			}
			const query = "SELECT seq, 'abcdefghijabcdefghijabcdefghijaa' FROM seq_1_to_1000 WHERE 1 = ?"
			stmt, err := d.db.Prepare(query)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				rows, err := stmt.Query(1)
				if err != nil {
					b.Fatal(err)
				}
				for rows.Next() {
					var seq int
					var s string
					if err := rows.Scan(&seq, &s); err != nil {
						b.Fatal(err)
					}
				}
				rows.Close()
			}
		})
	}
}

// BenchmarkSelect100ColsText mirrors test_bench_select_100_cols.py (text protocol).
func BenchmarkSelect100ColsText(b *testing.B) {
	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			const query = "SELECT * FROM bench_100cols WHERE 1 = ?"
			dest := make([]any, 100)
			ptrs := make([]any, 100)
			for i := range dest {
				ptrs[i] = &dest[i]
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row := d.db.QueryRow(query, 1)
				if err := row.Scan(ptrs...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkSelect100ColsBinary mirrors test_bench_select_100_cols.py (binary / prepared protocol).
func BenchmarkSelect100ColsBinary(b *testing.B) {
	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			const query = "SELECT * FROM bench_100cols WHERE 1 = ?"
			stmt, err := d.db.Prepare(query)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			dest := make([]any, 100)
			ptrs := make([]any, 100)
			for i := range dest {
				ptrs[i] = &dest[i]
			}
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				row := stmt.QueryRow(1)
				if err := row.Scan(ptrs...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkInsertBatch100 mirrors test_bench_insert_batch.py —
// inserts 100 rows in a single prepared-statement loop then rolls back.
func BenchmarkInsertBatch100(b *testing.B) {
	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			stmt, err := d.db.Prepare("INSERT INTO bench_insert(t0) VALUES (?)")
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				tx, err := d.db.Begin()
				if err != nil {
					b.Fatal(err)
				}
				txStmt := tx.Stmt(stmt)
				s := randString(100)
				for j := 0; j < 100; j++ {
					if _, err := txStmt.Exec(s); err != nil {
						tx.Rollback() //nolint:errcheck
						b.Fatal(err)
					}
				}
				tx.Rollback() //nolint:errcheck
			}
		})
	}
}

// BenchmarkDo1000ParamsBinary mirrors test_bench_do_1000_params.py (binary protocol).
func BenchmarkDo1000ParamsBinary(b *testing.B) {
	placeholders := strings.Repeat("?,", 1000)
	placeholders = placeholders[:len(placeholders)-1]
	query := "DO " + placeholders

	args := make([]any, 1000)
	for i := range args {
		args[i] = i + 1
	}

	for _, d := range drivers {
		d := d
		b.Run(d.name, func(b *testing.B) {
			stmt, err := d.db.Prepare(query)
			if err != nil {
				b.Fatal(err)
			}
			defer stmt.Close()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := stmt.Exec(args...); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
