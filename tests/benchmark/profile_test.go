// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build benchmark

package benchmark

import (
	"os"
	"runtime/pprof"
	"testing"
)

// TestProfileSelect1000RowsBinary profiles the binary protocol query to analyze allocations.
// Run with: go test -tags benchmark -run=TestProfileSelect1000RowsBinary ./tests/benchmark/
// Then analyze with: go tool pprof -alloc_space mem.prof
func TestProfileSelect1000RowsBinary(t *testing.T) {
	if !hasSeqEngine(drivers[0].db) {
		t.Skip("SEQUENCE engine not available")
	}

	d := drivers[0] // mariadb driver
	const query = "SELECT seq, 'abcdefghijabcdefghijabcdefghijaa' FROM seq_1_to_1000 WHERE 1 = ?"

	stmt, err := d.db.Prepare(query)
	if err != nil {
		t.Fatal(err)
	}
	defer stmt.Close()

	// Warm up
	rows, err := stmt.Query(1)
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var seq int
		var s string
		if err := rows.Scan(&seq, &s); err != nil {
			t.Fatal(err)
		}
	}
	rows.Close()

	// Start memory profiling
	f, err := os.Create("mem.prof")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Run profiled iterations
	for i := 0; i < 100; i++ {
		rows, err := stmt.Query(1)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var seq int
			var s string
			if err := rows.Scan(&seq, &s); err != nil {
				t.Fatal(err)
			}
		}
		rows.Close()
	}

	if err := pprof.WriteHeapProfile(f); err != nil {
		t.Fatal(err)
	}

	t.Logf("Memory profile written to mem.prof")
	t.Logf("Analyze with: go tool pprof -alloc_space mem.prof")
}
