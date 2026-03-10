// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"context"
	"crypto/rsa"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"time"
)

// Config holds the configuration for a MariaDB/MySQL connection
type Config struct {
	// Network and address
	Protocol string // "tcp", "tcp6", "unix" (default: "tcp")
	Host     string // Hostname (default: "127.0.0.1")
	Port     int    // Port (default: 3306)
	Socket   string // Unix socket path (default: "/tmp/mysql.sock" for unix)
	Addr     string // Network address (computed from Host:Port or Socket)

	// Authentication
	User     string // Username
	Password string // Password (requires User)
	Database string // Database name

	// Timeouts
	Timeout      time.Duration // Dial timeout (default: 10s)
	ReadTimeout  time.Duration // I/O read timeout
	WriteTimeout time.Duration // I/O write timeout

	// TLS configuration
	TLS             *tls.Config    // TLS configuration, its priority is higher than TLSConfig
	TLSConfig       string         // TLS configuration name: "true", "false", "skip-verify", "preferred", or custom name
	ServerPubKey    string         // Server public key name for sha256_password authentication
	ServerPublicKey *rsa.PublicKey // Parsed server public key for RSA encryption

	// Character set and collation
	Charset   string         // Connection charset (can be comma-separated list)
	Charsets  []string       // Connection charsets (internal)
	Collation string         // Connection collation
	Loc       *time.Location // Location for time.Time values (default: UTC)

	// Connection parameters
	MaxAllowedPacket     int               // Max packet size allowed (default: 64MB)
	ConnectionAttributes string            // Connection attributes, comma-delimited "key:value" pairs
	Params               map[string]string // Additional connection parameters

	// Boolean options
	AllowAllFiles            bool // Allow all files to be used with LOAD DATA LOCAL INFILE
	AllowCleartextPasswords  bool // Allows the cleartext client side plugin
	AllowFallbackToPlaintext bool // Allows fallback to unencrypted connection if server does not support TLS
	AllowNativePasswords     bool // Allows the native password authentication method (default: true)
	AllowOldPasswords        bool // Allows the old insecure password method
	AllowPublicKeyRetrieval  bool // Allow retrieval of server public key for caching_sha2_password (default: true)
	CheckConnLiveness        bool // Check connections for liveness before using them (default: true)
	ClientFoundRows          bool // Return number of matching rows instead of rows changed
	ColumnsWithAlias         bool // Prepend table alias to column names
	InterpolateParams        bool // Interpolate placeholders into query string
	MultiStatements          bool // Allow multiple statements in one query
	ParseTime                bool // Parse time values to time.Time
	RejectReadOnly           bool // Reject read-only connections

	// Result set options
	FetchSize        int // Number of rows to fetch at once for streaming results (0 = complete fetch, >0 = streaming)
	DefaultFetchSize int // Default fetch size if not specified (default: 0 = complete)

	// Debug options
	Debug bool // Enable debug logging of all protocol exchanges

	// Advanced options (unexported in go-sql-driver/mysql)
	Compress      bool                                                              // Enable zlib compression
	TimeTruncate  time.Duration                                                     // Truncate time.Time values to the specified duration
	BeforeConnect func(context.Context, *Config) error                              // Invoked before a connection is established
	DialFunc      func(ctx context.Context, network, addr string) (net.Conn, error) // Custom dial function

	// Internal fields
	pubKey *rsa.PublicKey // Server public key (parsed from ServerPubKey)
}

// GetUser returns the username for handshake
func (c *Config) GetUser() string {
	return c.User
}

// GetPassword returns the password for handshake
func (c *Config) GetPassword() string {
	return c.Password
}

// GetDatabase returns the database name for handshake
func (c *Config) GetDatabase() string {
	return c.Database
}

// AllowMultiQueries returns whether multi-queries are allowed
func (c *Config) AllowMultiQueries() bool {
	return c.MultiStatements
}

// UseAffectedRows returns whether to use affected rows instead of found rows
func (c *Config) UseAffectedRows() bool {
	return !c.ClientFoundRows
}

// AllowLocalInfile returns whether local infile is allowed
func (c *Config) AllowLocalInfile() bool {
	return c.AllowAllFiles
}

// UseBulkStmts returns whether bulk statements are used
func (c *Config) UseBulkStmts() bool {
	return false // Not implemented yet
}

// UseCompression returns whether compression is enabled
func (c *Config) UseCompression() bool {
	return c.Compress
}

// GetProtocol returns the protocol
func (c *Config) GetProtocol() string {
	return c.Protocol
}

// GetHost returns the host
func (c *Config) GetHost() string {
	return c.Host
}

// GetPort returns the port
func (c *Config) GetPort() int {
	return c.Port
}

// GetSocket returns the socket path
func (c *Config) GetSocket() string {
	return c.Socket
}

// GetTimeout returns the timeout
func (c *Config) GetTimeout() time.Duration {
	return c.Timeout
}

// GetReadTimeout returns the read timeout
func (c *Config) GetReadTimeout() time.Duration {
	return c.ReadTimeout
}

// GetWriteTimeout returns the write timeout
func (c *Config) GetWriteTimeout() time.Duration {
	return c.WriteTimeout
}

// GetCharset returns the charset
func (c *Config) GetCharset() string {
	return c.Charset
}

// GetCollation returns the collation
func (c *Config) GetCollation() string {
	return c.Collation
}

// GetTLSConfig returns the TLS config name
func (c *Config) GetTLSConfig() string {
	return c.TLSConfig
}

// GetDebug returns whether debug is enabled
func (c *Config) GetDebug() bool {
	return c.Debug
}

// GetAllowCleartextPasswords returns whether cleartext passwords are allowed
func (c *Config) GetAllowCleartextPasswords() bool {
	return c.AllowCleartextPasswords
}

// GetAllowNativePasswords returns whether native passwords are allowed
func (c *Config) GetAllowNativePasswords() bool {
	return c.AllowNativePasswords
}

// GetAllowPublicKeyRetrieval returns whether public key retrieval is allowed
func (c *Config) GetAllowPublicKeyRetrieval() bool {
	return c.AllowPublicKeyRetrieval
}

// GetAllowLocalInfile returns whether local infile is allowed
func (c *Config) GetAllowLocalInfile() bool {
	return c.AllowLocalInfile()
}

// GetAllowMultiQueries returns whether multi-queries are allowed
func (c *Config) GetAllowMultiQueries() bool {
	return c.AllowMultiQueries()
}

// GetUseAffectedRows returns whether to use affected rows
func (c *Config) GetUseAffectedRows() bool {
	return c.UseAffectedRows()
}

// GetUseBulkStmts returns whether to use bulk statements
func (c *Config) GetUseBulkStmts() bool {
	return c.UseBulkStmts()
}

// GetUseCompression returns whether to use compression
func (c *Config) GetUseCompression() bool {
	return c.UseCompression()
}

// GetServerPubKey returns the server public key name
func (c *Config) GetServerPubKey() string {
	return c.ServerPubKey
}

// GetConnectionAttributes returns the connection attributes
func (c *Config) GetConnectionAttributes() string {
	return c.ConnectionAttributes
}

// GetParams returns the parameters
func (c *Config) GetParams() map[string]string {
	return c.Params
}

// ValidateCharset validates that the charset is UTF-8 compatible
func (c *Config) ValidateCharset() error {
	if c.Charset == "" {
		c.Charset = "utf8mb4"
		return nil
	}

	charset := strings.ToLower(c.Charset)
	if charset != "utf8" && charset != "utf8mb3" && charset != "utf8mb4" {
		return fmt.Errorf("charset must be utf8, utf8mb3, or utf8mb4, got: %s", c.Charset)
	}

	return nil
}

// NewConfig creates a new Config with default values
func NewConfig() *Config {
	return &Config{
		Protocol:                "tcp",
		Host:                    "127.0.0.1",
		Port:                    3306,
		Timeout:                 10 * time.Second,
		Charset:                 "utf8mb4",
		Loc:                     time.UTC,
		MaxAllowedPacket:        64 * 1024 * 1024, // 64MB (matches go-sql-driver/mysql)
		AllowNativePasswords:    true,
		AllowPublicKeyRetrieval: true,
		CheckConnLiveness:       true,
		Params:                  make(map[string]string),
	}
}

// Clone creates a deep copy of the Config
func (c *Config) Clone() *Config {
	if c == nil {
		return nil
	}

	clone := &Config{
		Protocol:                 c.Protocol,
		Host:                     c.Host,
		Port:                     c.Port,
		Socket:                   c.Socket,
		Addr:                     c.Addr,
		User:                     c.User,
		Password:                 c.Password,
		Database:                 c.Database,
		Timeout:                  c.Timeout,
		ReadTimeout:              c.ReadTimeout,
		WriteTimeout:             c.WriteTimeout,
		TLSConfig:                c.TLSConfig,
		ServerPubKey:             c.ServerPubKey,
		Charset:                  c.Charset,
		Charsets:                 append([]string{}, c.Charsets...),
		Collation:                c.Collation,
		Loc:                      c.Loc,
		MaxAllowedPacket:         c.MaxAllowedPacket,
		ConnectionAttributes:     c.ConnectionAttributes,
		AllowAllFiles:            c.AllowAllFiles,
		AllowCleartextPasswords:  c.AllowCleartextPasswords,
		AllowFallbackToPlaintext: c.AllowFallbackToPlaintext,
		AllowNativePasswords:     c.AllowNativePasswords,
		AllowOldPasswords:        c.AllowOldPasswords,
		CheckConnLiveness:        c.CheckConnLiveness,
		ClientFoundRows:          c.ClientFoundRows,
		ColumnsWithAlias:         c.ColumnsWithAlias,
		InterpolateParams:        c.InterpolateParams,
		MultiStatements:          c.MultiStatements,
		ParseTime:                c.ParseTime,
		RejectReadOnly:           c.RejectReadOnly,
		FetchSize:                c.FetchSize,
		DefaultFetchSize:         c.DefaultFetchSize,
		Debug:                    c.Debug,
		Compress:                 c.Compress,
		TimeTruncate:             c.TimeTruncate,
		BeforeConnect:            c.BeforeConnect,
		DialFunc:                 c.DialFunc,
		Params:                   make(map[string]string),
	}

	// Deep copy TLS config
	if c.TLS != nil {
		clone.TLS = c.TLS.Clone()
	}

	// Deep copy params
	for k, v := range c.Params {
		clone.Params[k] = v
	}

	// Deep copy public key if present
	if c.pubKey != nil {
		clone.pubKey = &rsa.PublicKey{
			N: c.pubKey.N,
			E: c.pubKey.E,
		}
	}

	return clone
}
