// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package unit

import (
	"testing"
	"time"

	"github.com/mariadb-connector-go/mariadb"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name     string
		dsn      string
		wantErr  bool
		validate func(*testing.T, *mariadb.Config)
	}{
		{
			name: "Simple TCP connection",
			dsn:  "user:password@tcp(localhost:3306)/dbname",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.User != "user" {
					t.Errorf("User mismatch: got %q, want %q", cfg.User, "user")
				}
				if cfg.Passwd != "password" {
					t.Errorf("Password mismatch: got %q, want %q", cfg.Passwd, "password")
				}
				if cfg.Net != "tcp" {
					t.Errorf("Protocol mismatch: got %q, want %q", cfg.Net, "tcp")
				}
				if cfg.Host != "localhost" {
					t.Errorf("Host mismatch: got %q, want %q", cfg.Host, "localhost")
				}
				if cfg.Port != 3306 {
					t.Errorf("Port mismatch: got %d, want %d", cfg.Port, 3306)
				}
				if cfg.DBName != "dbname" {
					t.Errorf("Database mismatch: got %q, want %q", cfg.DBName, "dbname")
				}
			},
		},
		{
			name: "TCP with custom port",
			dsn:  "root@tcp(127.0.0.1:3307)/test",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.User != "root" {
					t.Errorf("User mismatch: got %q, want %q", cfg.User, "root")
				}
				if cfg.Host != "127.0.0.1" {
					t.Errorf("Host mismatch: got %q, want %q", cfg.Host, "127.0.0.1")
				}
				if cfg.Port != 3307 {
					t.Errorf("Port mismatch: got %d, want %d", cfg.Port, 3307)
				}
			},
		},
		{
			name: "Unix socket",
			dsn:  "user:password@unix(/var/run/mysqld/mysqld.sock)/dbname",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.Net != "unix" {
					t.Errorf("Protocol mismatch: got %q, want %q", cfg.Net, "unix")
				}
				if cfg.Socket != "/var/run/mysqld/mysqld.sock" {
					t.Errorf("Socket mismatch: got %q, want %q", cfg.Socket, "/var/run/mysqld/mysqld.sock")
				}
			},
		},
		{
			name: "With timeout parameter",
			dsn:  "user@tcp(localhost:3306)/db?timeout=5s",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.Timeout != 5*time.Second {
					t.Errorf("Timeout mismatch: got %v, want %v", cfg.Timeout, 5*time.Second)
				}
			},
		},
		{
			name: "With charset parameter",
			dsn:  "user@tcp(localhost:3306)/db?charset=utf8",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.Charset != "utf8" {
					t.Errorf("Charset mismatch: got %q, want %q", cfg.Charset, "utf8")
				}
			},
		},
		{
			name: "With TLS parameter",
			dsn:  "user@tcp(localhost:3306)/db?tls=true",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.TLS == nil {
					t.Error("Expected TLS config but got nil")
				}
				if cfg.TLSConfig != "true" {
					t.Errorf("TLSConfig mismatch: got %q, want %q", cfg.TLSConfig, "true")
				}
			},
		},
		{
			name: "Multiple parameters",
			dsn:  "user:pass@tcp(localhost:3306)/db?timeout=10s&charset=utf8mb4&readTimeout=5s",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.Timeout != 10*time.Second {
					t.Errorf("Timeout mismatch: got %v, want %v", cfg.Timeout, 10*time.Second)
				}
				if cfg.Charset != "utf8mb4" {
					t.Errorf("Charset mismatch: got %q, want %q", cfg.Charset, "utf8mb4")
				}
				if cfg.ReadTimeout != 5*time.Second {
					t.Errorf("ReadTimeout mismatch: got %v, want %v", cfg.ReadTimeout, 5*time.Second)
				}
			},
		},
		{
			name:    "Empty DSN",
			dsn:     "",
			wantErr: true,
		},
		{
			name: "No database",
			dsn:  "user:password@tcp(localhost:3306)/",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.DBName != "" {
					t.Errorf("Expected empty database, got %q", cfg.DBName)
				}
			},
		},
		{
			name: "IPv6 address",
			dsn:  "user@tcp([::1]:3306)/db",
			validate: func(t *testing.T, cfg *mariadb.Config) {
				if cfg.Host != "::1" {
					t.Errorf("Host mismatch: got %q, want %q", cfg.Host, "::1")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := mariadb.ParseDSN(tt.dsn)

			if tt.wantErr {
				if err == nil {
					t.Error("Expected error but got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("ParseDSN failed: %v", err)
			}

			if tt.validate != nil {
				tt.validate(t, cfg)
			}
		})
	}
}

func TestFormatDSN(t *testing.T) {
	tests := []struct {
		name   string
		config *mariadb.Config
		want   string
	}{
		{
			name: "Simple TCP connection",
			config: &mariadb.Config{
				Net:    "tcp",
				Host:   "localhost",
				Port:   3306,
				User:   "user",
				Passwd: "password",
				DBName: "dbname",
			},
			want: "user:password@tcp(localhost)/dbname",
		},
		{
			name: "Unix socket",
			config: &mariadb.Config{
				Net:    "unix",
				Socket: "/tmp/mysql.sock",
				User:   "user",
				DBName: "dbname",
			},
			want: "user@unix(/tmp/mysql.sock)/dbname",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mariadb.FormatDSN(tt.config)

			// Parse both to compare normalized forms
			gotCfg, err := mariadb.ParseDSN(got)
			if err != nil {
				t.Fatalf("Failed to parse formatted DSN: %v", err)
			}

			wantCfg, err := mariadb.ParseDSN(tt.want)
			if err != nil {
				t.Fatalf("Failed to parse expected DSN: %v", err)
			}

			// Compare key fields
			if gotCfg.User != wantCfg.User {
				t.Errorf("User mismatch: got %q, want %q", gotCfg.User, wantCfg.User)
			}
			if gotCfg.Host != wantCfg.Host {
				t.Errorf("Host mismatch: got %q, want %q", gotCfg.Host, wantCfg.Host)
			}
			if gotCfg.DBName != wantCfg.DBName {
				t.Errorf("Database mismatch: got %q, want %q", gotCfg.DBName, wantCfg.DBName)
			}
		})
	}
}

func TestConfigClone(t *testing.T) {
	original := &mariadb.Config{
		Net:     "tcp",
		Host:    "localhost",
		Port:    3306,
		User:    "user",
		Passwd:  "password",
		DBName:  "dbname",
		Timeout: 10 * time.Second,
		Charset: "utf8mb4",
		Params:  map[string]string{"key": "value"},
	}

	clone := original.Clone()

	// Verify clone has same values
	if clone.User != original.User {
		t.Errorf("User mismatch: got %q, want %q", clone.User, original.User)
	}
	if clone.Host != original.Host {
		t.Errorf("Host mismatch: got %q, want %q", clone.Host, original.Host)
	}

	// Verify it's a deep copy by modifying clone
	clone.User = "different"
	clone.Params["key"] = "different"

	if original.User == clone.User {
		t.Error("Clone is not independent - modifying clone affected original")
	}
	if original.Params["key"] == clone.Params["key"] {
		t.Error("Clone is not deep - modifying clone params affected original")
	}
}

func BenchmarkParseDSN(b *testing.B) {
	dsn := "user:password@tcp(localhost:3306)/dbname?timeout=10s&charset=utf8mb4"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mariadb.ParseDSN(dsn)
	}
}

func BenchmarkFormatDSN(b *testing.B) {
	cfg := &mariadb.Config{
		Net:     "tcp",
		Host:    "localhost",
		Port:    3306,
		User:    "user",
		Passwd:  "password",
		DBName:  "dbname",
		Timeout: 10 * time.Second,
		Charset: "utf8mb4",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		mariadb.FormatDSN(cfg)
	}
}
