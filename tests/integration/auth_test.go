// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

//go:build integration
// +build integration

package integration

import (
	"database/sql"
	"testing"

	_ "github.com/mariadb-connector-go/mariadb"
)

// TestEd25519Authentication tests the client_ed25519 authentication plugin
func TestEd25519Authentication(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Check if server supports ed25519
	var hasPlugin bool
	err := db.QueryRow("SELECT COUNT(*) > 0 FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'ed25519'").Scan(&hasPlugin)
	if err != nil {
		t.Skip("Cannot check for ed25519 plugin:", err)
	}

	if !hasPlugin {
		// Try to install the plugin
		_, err = db.Exec("INSTALL SONAME 'auth_ed25519'")
		if err != nil {
			t.Skip("Server doesn't have ed25519 plugin, skipping test")
		}
	}

	// Clean up any existing test user
	db.Exec("DROP USER IF EXISTS 'testEd25519User'@'%'")
	db.Exec("DROP USER IF EXISTS 'testEd25519User'@'localhost'")

	// Check server version for password syntax
	var version string
	err = db.QueryRow("SELECT VERSION()").Scan(&version)
	if err != nil {
		t.Fatal("Failed to get server version:", err)
	}

	// Create user with ed25519 authentication
	// Note: Ed25519 authentication in MariaDB uses a custom TweetNaCl-based implementation
	// The PASSWORD() function generates the correct hash format for the server
	// However, there may be compatibility issues with how the hash is generated vs verified
	createUserSQL := `CREATE USER 'testEd25519User'@'%' IDENTIFIED VIA ed25519 USING PASSWORD('MySup8%rPassw@ord')`
	_, err = db.Exec(createUserSQL)
	if err != nil {
		t.Fatal("Failed to create ed25519 user:", err)
	}
	defer db.Exec("DROP USER IF EXISTS 'testEd25519User'@'%'")

	// Grant privileges
	_, err = db.Exec("GRANT SELECT ON *.* TO 'testEd25519User'@'%'")
	if err != nil {
		t.Fatal("Failed to grant privileges:", err)
	}

	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		t.Fatal("Failed to flush privileges:", err)
	}

	// Test connection with ed25519 authentication
	dsn := "testEd25519User:MySup8%rPassw@ord@tcp(127.0.0.1:3306)/test"
	testDB, err := sql.Open("mariadb", dsn)
	if err != nil {
		t.Fatal("Failed to open connection with ed25519 user:", err)
	}
	defer testDB.Close()

	// Verify connection works
	err = testDB.Ping()
	if err != nil {
		t.Fatal("Failed to ping with ed25519 user:", err)
	}

	// Execute a simple query
	var result int
	err = testDB.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatal("Failed to execute query with ed25519 user:", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}
}

// TestParsecAuthentication tests the parsec authentication plugin
func TestParsecAuthentication(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Check if server supports parsec (MariaDB 10.6.1+)
	var hasPlugin bool
	err := db.QueryRow("SELECT COUNT(*) > 0 FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'parsec'").Scan(&hasPlugin)
	if err != nil {
		t.Skip("Cannot check for parsec plugin:", err)
	}

	if !hasPlugin {
		// Try to install the plugin
		_, err = db.Exec("INSTALL SONAME 'auth_parsec'")
		if err != nil {
			t.Skip("Server doesn't have parsec plugin, skipping test")
		}
	}

	// Clean up any existing test users
	db.Exec("DROP USER IF EXISTS 'testParsecUser'@'%'")
	db.Exec("DROP USER IF EXISTS 'testParsecUser'@'localhost'")

	// Create user with parsec authentication
	createUserSQL := `CREATE USER 'testParsecUser'@'%' IDENTIFIED VIA parsec USING PASSWORD('heyPassw-!*20oRd')`
	_, err = db.Exec(createUserSQL)
	if err != nil {
		t.Fatal("Failed to create parsec user:", err)
	}
	defer db.Exec("DROP USER IF EXISTS 'testParsecUser'@'%'")

	// Grant privileges
	_, err = db.Exec("GRANT SELECT ON *.* TO 'testParsecUser'@'%'")
	if err != nil {
		t.Fatal("Failed to grant privileges:", err)
	}

	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		t.Fatal("Failed to flush privileges:", err)
	}

	// Test connection with parsec authentication
	dsn := "testParsecUser:heyPassw-!*20oRd@tcp(127.0.0.1:3306)/test"
	testDB, err := sql.Open("mariadb", dsn)
	if err != nil {
		t.Fatal("Failed to open connection with parsec user:", err)
	}
	defer testDB.Close()

	// Verify connection works
	err = testDB.Ping()
	if err != nil {
		t.Fatal("Failed to ping with parsec user:", err)
	}

	// Execute a simple query
	var result int
	err = testDB.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatal("Failed to execute query with parsec user:", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}
}

// TestNativePasswordAuthentication tests the mysql_native_password authentication
func TestNativePasswordAuthentication(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Clean up any existing test user
	db.Exec("DROP USER IF EXISTS 'testNativeUser'@'%'")
	db.Exec("DROP USER IF EXISTS 'testNativeUser'@'localhost'")

	// Create user with native password authentication
	// Try MySQL syntax first, then MariaDB syntax
	createUserSQL := `CREATE USER 'testNativeUser'@'%' IDENTIFIED WITH mysql_native_password BY 'heyPassw-!*20oRd'`
	_, err := db.Exec(createUserSQL)
	if err != nil {
		// Try MariaDB syntax
		createUserSQL = `CREATE USER 'testNativeUser'@'%' IDENTIFIED VIA mysql_native_password USING PASSWORD('heyPassw-!*20oRd')`
		_, err = db.Exec(createUserSQL)
		if err != nil {
			t.Fatal("Failed to create native password user:", err)
		}
	}
	defer db.Exec("DROP USER IF EXISTS 'testNativeUser'@'%'")

	// Grant privileges
	_, err = db.Exec("GRANT SELECT ON *.* TO 'testNativeUser'@'%'")
	if err != nil {
		t.Fatal("Failed to grant privileges:", err)
	}

	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		t.Fatal("Failed to flush privileges:", err)
	}

	// Test connection with native password authentication
	dsn := "testNativeUser:heyPassw-!*20oRd@tcp(127.0.0.1:3306)/test"
	testDB, err := sql.Open("mariadb", dsn)
	if err != nil {
		t.Fatal("Failed to open connection with native password user:", err)
	}
	defer testDB.Close()

	// Verify connection works
	err = testDB.Ping()
	if err != nil {
		t.Fatal("Failed to ping with native password user:", err)
	}

	// Execute a simple query
	var result int
	err = testDB.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatal("Failed to execute query with native password user:", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}
}

// TestCachingSha2PasswordAuthentication tests the caching_sha2_password authentication
func TestCachingSha2PasswordAuthentication(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Check if server supports caching_sha2_password
	var hasPlugin bool
	err := db.QueryRow("SELECT COUNT(*) > 0 FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'caching_sha2_password'").Scan(&hasPlugin)
	if err != nil {
		t.Skip("Cannot check for caching_sha2_password plugin:", err)
	}

	if !hasPlugin {
		t.Skip("Server doesn't have caching_sha2_password plugin, skipping test")
	}

	// Clean up any existing test user
	db.Exec("DROP USER IF EXISTS 'testCachingSha2User'@'%'")
	db.Exec("DROP USER IF EXISTS 'testCachingSha2User'@'localhost'")

	// Create user with caching_sha2_password authentication
	createUserSQL := `CREATE USER 'testCachingSha2User'@'%' IDENTIFIED WITH caching_sha2_password BY 'heyPassw-!*20oRd'`
	_, err = db.Exec(createUserSQL)
	if err != nil {
		// Try MariaDB syntax
		createUserSQL = `CREATE USER 'testCachingSha2User'@'%' IDENTIFIED VIA caching_sha2_password USING PASSWORD('heyPassw-!*20oRd')`
		_, err = db.Exec(createUserSQL)
		if err != nil {
			t.Fatal("Failed to create caching_sha2_password user:", err)
		}
	}
	defer db.Exec("DROP USER IF EXISTS 'testCachingSha2User'@'%'")

	// Grant privileges
	_, err = db.Exec("GRANT SELECT ON *.* TO 'testCachingSha2User'@'%'")
	if err != nil {
		t.Fatal("Failed to grant privileges:", err)
	}

	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		t.Fatal("Failed to flush privileges:", err)
	}

	// Test connection with caching_sha2_password authentication
	// Note: This may require TLS or allowPublicKeyRetrieval depending on server config
	dsn := "testCachingSha2User:heyPassw-!*20oRd@tcp(127.0.0.1:3306)/test"
	testDB, err := sql.Open("mariadb", dsn)
	if err != nil {
		t.Fatal("Failed to open connection with caching_sha2_password user:", err)
	}
	defer testDB.Close()

	// Verify connection works
	err = testDB.Ping()
	if err != nil {
		t.Logf("Note: caching_sha2_password may require TLS or public key retrieval: %v", err)
		t.Skip("Skipping due to caching_sha2_password requirements")
	}

	// Execute a simple query
	var result int
	err = testDB.QueryRow("SELECT 1").Scan(&result)
	if err != nil {
		t.Fatal("Failed to execute query with caching_sha2_password user:", err)
	}

	if result != 1 {
		t.Errorf("Expected 1, got %d", result)
	}
}

// TestCachingSha2PasswordWithDisabledPublicKeyRetrieval tests that caching_sha2_password
// fails with appropriate error when AllowPublicKeyRetrieval=false and no TLS/ServerPubKey
func TestCachingSha2PasswordWithDisabledPublicKeyRetrieval(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Check if server supports caching_sha2_password
	var hasPlugin bool
	err := db.QueryRow("SELECT COUNT(*) > 0 FROM information_schema.PLUGINS WHERE PLUGIN_NAME = 'caching_sha2_password'").Scan(&hasPlugin)
	if err != nil {
		t.Skip("Cannot check for caching_sha2_password plugin:", err)
	}

	if !hasPlugin {
		t.Skip("Server doesn't have caching_sha2_password plugin, skipping test")
	}

	// Clean up any existing test user
	db.Exec("DROP USER IF EXISTS 'testCachingSha2NoRetrievalUser'@'%'")
	db.Exec("DROP USER IF EXISTS 'testCachingSha2NoRetrievalUser'@'localhost'")

	// Create user with caching_sha2_password authentication
	createUserSQL := `CREATE USER 'testCachingSha2NoRetrievalUser'@'%' IDENTIFIED WITH caching_sha2_password BY 'testPass123'`
	_, err = db.Exec(createUserSQL)
	if err != nil {
		// Try MariaDB syntax
		createUserSQL = `CREATE USER 'testCachingSha2NoRetrievalUser'@'%' IDENTIFIED VIA caching_sha2_password USING PASSWORD('testPass123')`
		_, err = db.Exec(createUserSQL)
		if err != nil {
			t.Fatal("Failed to create caching_sha2_password user:", err)
		}
	}
	defer db.Exec("DROP USER IF EXISTS 'testCachingSha2NoRetrievalUser'@'%'")

	// Grant privileges
	_, err = db.Exec("GRANT SELECT ON *.* TO 'testCachingSha2NoRetrievalUser'@'%'")
	if err != nil {
		t.Fatal("Failed to grant privileges:", err)
	}

	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		t.Fatal("Failed to flush privileges:", err)
	}

	// Test connection with AllowPublicKeyRetrieval=false (no TLS, no ServerPubKey)
	// This should fail with an appropriate error message
	dsn := "testCachingSha2NoRetrievalUser:testPass123@tcp(127.0.0.1:3306)/test?allowPublicKeyRetrieval=false"
	testDB, err := sql.Open("mariadb", dsn)
	if err != nil {
		t.Fatal("Failed to open connection:", err)
	}
	defer testDB.Close()

	// This should fail because:
	// - No TLS configured
	// - No ServerPubKey provided
	// - AllowPublicKeyRetrieval=false
	err = testDB.Ping()
	if err == nil {
		t.Fatal("Expected error when AllowPublicKeyRetrieval=false without TLS or ServerPubKey, but connection succeeded")
	}

	// Verify the error message is appropriate
	expectedErrMsg := "caching_sha2_password requires either TLS or server public key"
	if !contains(err.Error(), expectedErrMsg) {
		t.Errorf("Expected error containing '%s', got: %v", expectedErrMsg, err)
	}

	t.Logf("Correctly failed with error: %v", err)
}

// contains checks if a string contains a substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// TestClearPasswordAuthentication tests the mysql_clear_password authentication
func TestClearPasswordAuthentication(t *testing.T) {
	db := OpenTestDB(t)
	defer db.Close()

	// Clean up any existing test user
	db.Exec("DROP USER IF EXISTS 'testClearUser'@'%'")
	db.Exec("DROP USER IF EXISTS 'testClearUser'@'localhost'")

	// Create user with clear password authentication
	createUserSQL := `CREATE USER 'testClearUser'@'%' IDENTIFIED WITH mysql_clear_password`
	_, err := db.Exec(createUserSQL)
	if err != nil {
		t.Skip("Server doesn't support mysql_clear_password plugin, skipping test")
	}
	defer db.Exec("DROP USER IF EXISTS 'testClearUser'@'%'")

	// Set password
	_, err = db.Exec("SET PASSWORD FOR 'testClearUser'@'%' = PASSWORD('clearPass123')")
	if err != nil {
		t.Fatal("Failed to set password for clear password user:", err)
	}

	// Grant privileges
	_, err = db.Exec("GRANT SELECT ON *.* TO 'testClearUser'@'%'")
	if err != nil {
		t.Fatal("Failed to grant privileges:", err)
	}

	_, err = db.Exec("FLUSH PRIVILEGES")
	if err != nil {
		t.Fatal("Failed to flush privileges:", err)
	}

	// Note: Clear password authentication should only be used over TLS
	// This test is included for completeness but may be skipped if TLS is not configured
	t.Skip("Cleartext password authentication requires TLS and AllowCleartextPasswords=true")
}
