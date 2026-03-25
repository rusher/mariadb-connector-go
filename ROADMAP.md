# MariaDB Connector/Go — Roadmap

This document describes the plan to create a MariaDB-native Go database driver
by forking [go-sql-driver/mysql](https://github.com/go-sql-driver/mysql).

---

## 1. Fork & Licensing

### 1.1 Fork origin

The upstream repository is **go-sql-driver/mysql** (commit `master` as of
March 2026).

### 1.2 License

The upstream code is licensed under the **Mozilla Public License 2.0 (MPL-2.0)**.

Key implications for a fork:

| MPL-2.0 rule | What it means for us |
|---|---|
| **File-level copyleft** | Any *modified* file must remain MPL-2.0. New files we add from scratch can use any license, but keeping everything MPL-2.0 is simplest. |
| **Larger Work** (§3.3) | The driver can be combined into proprietary applications without viral effect — MPL-2.0 only requires source availability for the driver files themselves. |
| **Notice preservation** (§3.4) | We **must not** remove or alter existing copyright/license headers. We **may** add our own copyright lines (e.g. `Copyright 2026 MariaDB Corporation Ab`). |
| **No trademark rights** (§2.3) | We have no right to use "MySQL" or "Go-MySQL-Driver" trademarks. Renaming the driver is not only allowed, it is advisable. |
| **Patent grant** (§2.1b) | Contributors grant a patent license for their contributions. |

**Recommendation:** keep MPL-2.0 for all files (existing and new). Add a
`Copyright 2026 MariaDB Corporation Ab` line to newly created files and to
headers of substantially modified files, alongside the original notice.

### 1.3 Repository

- **Target:** `github.com/mariadb-corporation/mariadb-connector-go`
- **Go module:** `github.com/mariadb-corporation/mariadb-connector-go`
- **Driver name** registered with `database/sql`: `mariadb`
- **Go package name:** `mysql` (kept for now to minimize churn; can be renamed
  to `mariadb` in a v2 major version bump if desired)

### 1.4 Renaming checklist

- [x] `go.mod` module path → `github.com/mariadb-corporation/mariadb-connector-go`
- [x] All import paths updated across `.go` files
- [x] `driverName` constant → `"mariadb"`
- [x] `MariaDBDriver` struct (with `MySQLDriver` as a type alias for compat)
- [x] README / doc comments updated
- [ ] AUTHORS file updated
- [ ] Create GitHub repo and push

---

## 2. Authentication

Based on PR [#1696](https://github.com/go-sql-driver/mysql/pull/1696).

### 2.1 Plugin architecture (done)

```go
AuthPlugin interface {
    Name() string
    InitAuth(authData []byte, cfg authPluginConfig) ([]byte, error)
    ContinuationAuth(packet, seed []byte, cfg authPluginConfig) ([]byte, done bool, err error)
}
```

- `mc.auth()` delegates to `plugin.InitAuth()`
- `mc.handleAuthResult()` runs a generic continuation loop calling
  `plugin.ContinuationAuth()` for multi-round exchanges
- Plugins are decoupled from `*mysqlConn` via `authPluginConfig`

### 2.2 Implemented plugins

| Plugin | Status | Notes |
|---|---|---|
| `mysql_native_password` | ✅ Done | |
| `caching_sha2_password` | ✅ Done | Multi-round (pub key retrieval) |
| `sha256_password` | ✅ Done | Multi-round (pub key retrieval) |
| `mysql_clear_password` | ✅ Done | |
| `mysql_old_password` | ✅ Done | |
| `client_ed25519` | ✅ Done | MariaDB ed25519 |
| `parsec` | ✅ Done | MariaDB 11.6+, PBKDF2 + ed25519 |
| `dialog` (PAM) | ✅ Done | Multi-round prompt/response |
| `auth_gssapi_client` | ⬜ Stub | Returns "not supported" error |

### 2.3 MariaDB multi-authentication

MariaDB supports multiple authentication plugins per user account
([CREATE USER ... IDENTIFIED VIA plugin1 OR plugin2](https://mariadb.com/kb/en/create-user/#identified-viawith-authentication_plugin)).
The server tries each plugin in order and sends auth-switch packets between
attempts.

What needs to be done:

- [ ] Handle multiple sequential auth-switch packets in `handleAuthResult()`
  without hitting the max-switch limit prematurely
- [ ] Each switch resets the auth plugin and seed; the loop already supports
  this but needs testing with real multi-auth users
- [ ] Integration tests: create user with `IDENTIFIED VIA ed25519 OR
  mysql_native_password` and verify both paths work

### 2.4 MySQL multi-factor authentication (MFA)

MySQL 8.0.27+ supports multi-factor authentication where a user must
authenticate with up to 3 plugins sequentially
([docs](https://dev.mysql.com/doc/refman/8.0/en/multifactor-authentication.html)).
The server sends `AuthNextFactor` packets (0x02) after each factor succeeds.

What needs to be done:

- [ ] Detect `AuthNextFactor` packet in the continuation loop
- [ ] Chain to the next plugin specified in the packet
- [ ] Support `authentication_fido` / `authentication_webauthn` plugins
  (likely stub — these require hardware interaction)
- [ ] Integration tests with MySQL 8.0.27+ MFA-enabled users

### 2.5 Remaining plugin work

- [ ] Full GSSAPI implementation (requires OS-level Kerberos; likely cgo or
  external library — may stay as stub with clear error message)
- [ ] Integration tests against real MariaDB server with parsec/PAM enabled
- [ ] `AllowPublicKeyRetrieval` defaults to `true` (done, for compat)

---

## 3. Session Tracking

MariaDB (and MySQL 5.7+) can return session state changes in OK packets when
the `CLIENT_SESSION_TRACK` capability is set and
`session_track_system_variables` is configured.

### 3.1 Protocol

When the OK packet has `SERVER_SESSION_STATE_CHANGED` (bit 14) set in the
status flags, it contains a session state info section with typed entries:

| Type | Value | Description |
|---|---|---|
| `SESSION_TRACK_SYSTEM_VARIABLES` | 0 | System variable changed (name + value) |
| `SESSION_TRACK_SCHEMA` | 1 | Default schema changed |
| `SESSION_TRACK_STATE_CHANGE` | 2 | Session state changed (generic flag) |
| `SESSION_TRACK_GTIDS` | 3 | GTID information |
| `SESSION_TRACK_TRANSACTION_CHARACTERISTICS` | 4 | Transaction characteristics |
| `SESSION_TRACK_TRANSACTION_STATE` | 5 | Transaction state |

### 3.2 What needs to be done

- [ ] Set `CLIENT_SESSION_TRACK` capability flag in handshake
- [ ] Parse session state info section in `readResultOK()` / `handleOkPacket()`
- [ ] Store tracked state on `mysqlConn`:
  - Last GTID (for read-your-writes consistency)
  - Schema changes (auto-update `cfg.DBName`)
  - System variable changes (e.g. `redirect_url` — see §7 Redirection)
  - Transaction state
- [ ] Expose tracked state via public API:
  - `conn.LastGTID() string`
  - `conn.SessionVar(name string) string`
  - For `database/sql`: expose via `driver.SessionResetter` or connection
    attributes
- [ ] Tests with `SET session_track_system_variables = '*'`

### 3.3 Use cases

- **Read-your-writes consistency**: use `SESSION_TRACK_GTIDS` to route reads
  to replicas that have caught up to the last write's GTID
- **Connection redirection**: the `redirect_url` system variable is delivered
  via `SESSION_TRACK_SYSTEM_VARIABLES` (see §7)
- **Schema tracking**: detect when the default schema changes (e.g. `USE db2`)
  to keep the driver's internal state consistent

---

## 4. High Availability (Multi-Host)

### 4.1 Goal

Support multiple hosts in the connection string with failover and load
balancing modes, matching MariaDB Connector/J's capabilities.

### 4.2 Connection string format

```
mariadb://user:password@host1:3306,host2:3307,host3:3308/dbname?param=value
```

Or with explicit roles:

```
mariadb:sequential://host1:3306,host2:3306,host3:3306/dbname
mariadb:loadbalance://host1:3306,host2:3306,host3:3306/dbname
mariadb:replication://primary:3306,replica1:3306,replica2:3306/dbname
```

### 4.3 Failover and load balancing modes

| Mode | Description |
|---|---|
| **`sequential`** | Try hosts in declared order. First available host used for all queries. First host = primary, rest = replicas (unless explicit). |
| **`loadbalance`** | Random host selection per connection. For multi-primary (e.g. Galera). |
| **`replication`** | Primary/replica awareness. Writes go to primary, reads load-balanced across replicas when connection is read-only. |

### 4.4 What needs to be done

- [ ] Parse multi-host DSN (comma-separated hosts)
- [ ] `Config.Hosts []HostConfig` with per-host address/port/role
- [ ] Implement `sequential` mode: ordered failover
- [ ] Implement `loadbalance` mode: random host per connection
- [ ] Implement `replication` mode: primary/replica split
  - Respect `readOnly` flag to route to replicas
  - Random replica selection for load balancing
- [ ] Failover on connection error: retry next host
- [ ] Failover during query: detect connection loss, reconnect (configurable)
- [ ] Health check / blacklisting: temporarily skip hosts that fail
  (with configurable recovery timeout)
- [ ] Integration with `database/sql` connection pool (connector returns
  connections to different hosts transparently)
- [ ] Tests with simulated multi-host topologies

---

## 5. Prepared Statement Client-Side Caching

### 5.1 Problem

Every call to `db.Query("SELECT ...", args)` through `database/sql` with
prepared statements sends:
1. `COM_STMT_PREPARE` → server parses and returns statement ID + metadata
2. `COM_STMT_EXECUTE` → server executes with bound parameters
3. `COM_STMT_CLOSE` → server deallocates

For repeated queries, the prepare/close round-trips are wasted network and
server CPU. MariaDB Connector/J caches `COM_STMT_PREPARE` results client-side
(keyed by SQL text) and reuses statement IDs, sending only `DEALLOCATE PREPARE`
when the cache evicts an entry.

### 5.2 Design

- **LRU cache** keyed by SQL string, per connection
- Cache stores: statement ID, parameter count, column count, column definitions
- On cache hit: skip `COM_STMT_PREPARE`, go directly to `COM_STMT_EXECUTE`
- On eviction: send `COM_STMT_CLOSE` for the evicted statement
- On connection close: close all cached statements
- Configurable cache size (default: 256, 0 = disabled)

### 5.3 What needs to be done

- [ ] Implement `stmtCache` struct with LRU eviction
- [ ] Intercept `Prepare()` / `Query()` to check cache before wire call
- [ ] Send `COM_STMT_CLOSE` only on eviction or connection close
- [ ] DSN parameter: `prepStmtCacheSize=256` (0 to disable)
- [ ] Handle `SERVER_STATUS_METADATA_CHANGED` flag (server signals that a
  cached prepare is stale due to DDL — must re-prepare)
- [ ] Benchmarks: measure round-trip savings with cache enabled vs disabled
- [ ] Tests: verify cache hit/miss/eviction/stale behavior

---

## 6. Standard Connection String Format

### 6.1 Goal

Support the standard URI-style connection string used by MariaDB Connector/J,
Connector/Node.js, and Connector/C:

```
mariadb://user:password@localhost:3306/dbname?param1=value1&param2=value2
```

### 6.2 Current state

The current driver uses the go-sql-driver format:

```
user:password@tcp(localhost:3306)/dbname?param=value
```

### 6.3 What needs to be done

- [ ] Parse `mariadb://` and `mysql://` URI schemes in `ParseDSN()`
- [ ] Map URI components to `Config` fields:
  - `scheme` → ignored (just a marker)
  - `userinfo` → `User`, `Passwd`
  - `host:port` → `Addr` (default port 3306)
  - `host1,host2,...` → multi-host (see §4)
  - `path` → `DBName`
  - `query` → `Params` + config booleans
- [ ] Support Unix socket via `mariadb://user:pass@unix(/path/to/socket)/db`
  or `mariadb://user:pass@/db?socket=/path/to/socket`
- [ ] Support HA modes via scheme prefix:
  `mariadb:sequential://`, `mariadb:replication://`, `mariadb:loadbalance://`
- [ ] Keep backward compatibility with the old `user:pass@tcp(host)/db` format
- [ ] Update `FormatDSN()` to optionally output the standard format
- [ ] Tests for both formats, round-trip parsing

---

## 7. Connection Redirection

### 7.1 Background

MariaDB 11.3+ supports connection redirection
([MDEV-15935](https://jira.mariadb.org/browse/MDEV-15935)). A proxy sets
`redirect_url` as a system variable, which the server delivers to the client
via session tracking in the OK packet of the initial handshake. The client
then transparently reconnects directly to the target server, bypassing the
proxy for subsequent queries.

This eliminates proxy latency for the lifetime of the connection — similar to
HTTP redirects.

### 7.2 Protocol flow

```
Client → Proxy    : connect + authenticate
Proxy  → Server   : forward handshake
Server → Proxy    : OK with session_track: redirect_url = "mariadb://host2:3306"
Proxy  → Client   : forward OK
Client            : parse redirect_url from session tracking
Client → Server   : new direct connection to host2:3306
Client            : re-authenticate, restore session state
Client → Proxy    : close original connection
```

### 7.3 What needs to be done

- [ ] Requires session tracking (§3) to be implemented first
- [ ] After successful handshake, check for `redirect_url` in tracked system
  variables
- [ ] If present, establish new connection to the redirect target
- [ ] Re-authenticate on the new connection with same credentials
- [ ] Restore session state (schema, variables, transaction state) on new
  connection
- [ ] Close the original (proxy) connection
- [ ] DSN parameter: `permitRedirect=true` (default: true)
- [ ] Limit redirect depth to prevent loops (max 1 redirect)
- [ ] Tests with simulated redirect flow

---

## 8. Zero-Configuration SSL/TLS

### 8.1 Goal

Enable TLS by default when the server supports it, without requiring the user
to configure certificates — matching the behavior of MariaDB Connector/J's
`sslMode=trust` and the `mariadb` CLI client's `--ssl` default.

### 8.2 Current state

The current driver requires explicit TLS configuration:
- `tls=true` — requires valid server certificate chain
- `tls=skip-verify` — encrypted but no server verification
- `tls=custom` — user-provided `tls.Config` registered via `RegisterTLSConfig`
- Default: **no TLS**

### 8.3 SSL modes (matching Connector/J)

| Mode | Encrypted | Server cert verified | Client cert sent | Description |
|---|---|---|---|---|
| `disable` | No | — | — | No TLS at all |
| `trust` | Yes | No | No | Encrypted, but accepts any server cert (like `skip-verify`) |
| `verify-ca` | Yes | CA only | Optional | Verifies server cert is signed by a known CA |
| `verify-full` | Yes | CA + hostname | Optional | Verifies CA chain AND hostname match (most secure) |

### 8.4 What needs to be done

- [ ] Add `sslMode` DSN parameter: `disable`, `trust`, `verify-ca`,
  `verify-full` (deprecate raw `tls=true/false/skip-verify`)
- [ ] **Default `sslMode=trust`**: encrypt by default if the server advertises
  `CLIENT_SSL` capability, without requiring certificate configuration.
  This is "zero-config" — just connecting to a TLS-enabled server works.
- [ ] For `verify-ca` and `verify-full`: use system CA bundle by default
  (Go's `crypto/tls` does this automatically), with optional `sslCert`,
  `sslKey`, `sslCA` DSN parameters for custom certs.
- [ ] Fallback behavior: if `sslMode=trust` and server doesn't support TLS,
  fall back to unencrypted (equivalent to current `allowFallbackToPlaintext`)
- [ ] Client certificate support via `sslCert` + `sslKey` DSN parameters
- [ ] Tests with TLS-enabled server in various modes

---

## 9. Remove `interpolateParams` Option

### 9.1 Rationale

Client-side parameter interpolation (`interpolateParams=true`) is inherently
fragile and security-sensitive:

- The driver must fully parse SQL syntax to safely locate parameter
  placeholders — duplicating server-side work
- Even with the PR [#1732](https://github.com/go-sql-driver/mysql/pull/1732)
  fix (state-machine parser for strings/comments/backticks), edge cases remain
  risky
- It bypasses the prepared statement protocol, losing type safety
- It is a foot-gun for users who enable it without understanding the risks

### 9.2 Plan

- [ ] Remove `InterpolateParams` from `Config` struct
- [ ] Remove `interpolateParams` DSN parameter parsing
- [ ] Remove `interpolateParams()` method and `errInvalidDSNUnsafeCollation`
- [ ] Remove related code paths in `query()` and `exec()`
- [ ] Update README / documentation
- [ ] **Breaking change**: document in CHANGELOG with migration guide

---

## 10. Dual API: `database/sql` Compatible + Proprietary

### 10.1 Motivation

The standard `database/sql` package introduces overhead:

- **`lastcols` allocation**: `make([]driver.Value, N)` per row — unavoidable
  heap allocation
- **Type conversion**: `convertAssign()` uses reflection to convert
  `driver.Value` (only `int64/float64/bool/[]byte/string/time.Time`) to user
  target types. Driver knows TINYINT→int8 but must widen to int64 first.
- **Connection pooling**: mutex-heavy pool with no MariaDB-specific awareness
- **Missing features**: no session tracking exposure, no `COM_STMT_FETCH`, no
  zero-copy `[]byte`

### 10.2 Proposed architecture

```
github.com/mariadb-corporation/mariadb-connector-go/
├── internal/proto/      # Core protocol, packets, auth plugins
├── std/                 # database/sql compatible wrapper
└── mariadb/             # Proprietary high-performance API
```

### 10.3 Implementation plan

1. **Profile first** — benchmark `database/sql` overhead to quantify cost
2. **Extract core** — move protocol code to `internal/proto/`
3. **Wrap for database/sql** — `std/` implements `driver.Connector` etc.
4. **Build proprietary API** — zero-alloc rows, native types, MariaDB features
5. **Benchmark & document** — comparative results, migration guide

### 10.4 Proprietary API highlights

| Feature | database/sql | Proprietary API |
|---|---|---|
| Row values | `[]driver.Value` (alloc/row) | Reusable typed buffer |
| Integer types | Widened to `int64` | Native `int8/uint8/int16/uint16/int32/uint32/int64/uint64` |
| Float types | Widened to `float64` | Native `float32/float64` |
| UUID | `string` or `[]byte` | `uuid.UUID` |
| `[]byte` | Copied per Scan | Zero-copy via buffer pinning |
| MariaDB metadata | Not exposed | Session tracking, warnings, RETURNING |
| Streaming cursor | Not supported | `COM_STMT_FETCH` with prefetch |

---

## 11. Priority Order

| Pri | Feature | Section | Effort | Dependencies |
|---|---|---|---|---|
| **P0** | Create repo, push fork | §1 | 1 day | — |
| **P0** | Auth plugin system | §2.1–2.2 | ✅ Done | — |
| **P1** | Remove `interpolateParams` | §9 | 1–2 days | — |
| **P1** | Standard connection string (`mariadb://`) | §6 | 3–5 days | — |
| **P1** | Session tracking | §3 | 1 week | — |
| **P1** | MariaDB multi-auth / MySQL MFA | §2.3–2.4 | 3–5 days | §2.1 |
| **P2** | Zero-config SSL (`sslMode`) | §8 | 3–5 days | — |
| **P2** | Prepared statement client caching | §5 | 1 week | — |
| **P2** | Connection redirection | §7 | 3–5 days | §3 |
| **P2** | High availability (multi-host) | §4 | 2–3 weeks | §6 |
| **P3** | Profile `database/sql` overhead | §10.3 | 2–3 days | — |
| **P3** | Dual API (proprietary + database/sql) | §10 | 4–8 weeks | §3, §5 |
| **P4** | Full GSSAPI auth | §2.5 | TBD | cgo |
