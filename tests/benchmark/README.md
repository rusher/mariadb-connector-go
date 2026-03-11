# Benchmarks

Compares **mariadb-connector-go** against **go-sql-driver/mysql** using Go's standard `testing.B` infrastructure.

## Requirements

- MariaDB or MySQL server reachable from the test host
- A database that the user can create/drop tables in (default: `testgo`)
- For the `Select1000Rows` benchmarks: [MariaDB SEQUENCE engine](https://mariadb.com/kb/en/sequence-storage-engine/) (auto-skipped when absent)

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `TEST_DB_HOST` | `127.0.0.1` | Server hostname or IP |
| `TEST_DB_PORT` | `3306` | Server port |
| `TEST_DB_USER` | `root` | Database user |
| `TEST_DB_PASSWORD` | *(empty)* | Database password |
| `TEST_DB_DATABASE` | `testgo` | Target database |

## Running

```bash
# All benchmarks, both drivers
go test -tags benchmark -bench=. -benchmem ./tests/benchmark/

# Single scenario
go test -tags benchmark -bench=BenchmarkSelectOne -benchmem ./tests/benchmark/

# Longer run for more stable numbers (e.g. 5 s per benchmark)
go test -tags benchmark -bench=. -benchmem -benchtime=5s ./tests/benchmark/

# With non-default credentials
TEST_DB_USER=app TEST_DB_PASSWORD=secret TEST_DB_DATABASE=mydb \
  go test -tags benchmark -bench=. -benchmem ./tests/benchmark/
```

## Scenarios

| Benchmark | Description | Protocol |
|---|---|---|
| `BenchmarkDoOne` | `DO 1` — minimal round-trip | text |
| `BenchmarkSelectOne` | `SELECT 1` — single scalar | text |
| `BenchmarkSelect1000RowsText` | Fetch 1 000 rows via `seq_1_to_1000` | text |
| `BenchmarkSelect1000RowsBinary` | Same with a prepared statement | binary |
| `BenchmarkSelect100ColsText` | Fetch one row with 100 INT columns | text |
| `BenchmarkSelect100ColsBinary` | Same with a prepared statement | binary |
| `BenchmarkInsertBatch100` | 100 prepared INSERTs inside a rolled-back transaction | binary |
| `BenchmarkDo1000ParamsBinary` | `DO ?,?,…` with 1 000 bound parameters | binary |

Each benchmark runs both drivers as sub-benchmarks (`/mariadb` and `/mysql`).

## Example output

```
BenchmarkDoOne/mariadb-8          	   30000	     45231 ns/op	     312 B/op	       6 allocs/op
BenchmarkDoOne/mysql-8            	   25000	     52418 ns/op	     480 B/op	       9 allocs/op
BenchmarkSelect1000RowsBinary/mariadb-8    500	   2841022 ns/op	  102400 B/op	    4012 allocs/op
BenchmarkSelect1000RowsBinary/mysql-8      400	   3102345 ns/op	  115200 B/op	    4521 allocs/op
```
