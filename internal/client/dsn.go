// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package client

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	errInvalidDSNUnsafeCollation = errors.New("invalid DSN: interpolateParams can not be used with unsafe collations")
)

// ParseDSN parses a Data Source Name (DSN) string into a Config
// DSN format: [username[:password]@][protocol[(address)]]/[dbname][?param1=value1&...&paramN=valueN]
func ParseDSN(dsn string) (*Config, error) {
	cfg := NewConfig()

	// Handle empty DSN
	if dsn == "" {
		return nil, errors.New("empty DSN")
	}

	// Find the last '/' to separate database from parameters
	foundSlash := false
	for i := len(dsn) - 1; i >= 0; i-- {
		if dsn[i] == '/' {
			foundSlash = true
			break
		}
	}

	// Parse user:password@protocol(address)/dbname?params format
	var userInfo, netAddr, dbName, params string

	// Split by '@' to get user info
	// Use LastIndex to handle passwords containing '@' characters
	if idx := strings.LastIndex(dsn, "@"); idx != -1 {
		userInfo = dsn[:idx]
		dsn = dsn[idx+1:]
	}

	// Split by '/' to get database and params
	if foundSlash {
		if idx := strings.Index(dsn, "/"); idx != -1 {
			netAddr = dsn[:idx]
			remainder := dsn[idx+1:]

			// Split database and params by '?'
			if qIdx := strings.Index(remainder, "?"); qIdx != -1 {
				dbName = remainder[:qIdx]
				params = remainder[qIdx+1:]
			} else {
				dbName = remainder
			}
		}
	} else {
		netAddr = dsn
	}

	// Parse user info
	if userInfo != "" {
		if idx := strings.Index(userInfo, ":"); idx != -1 {
			cfg.User = userInfo[:idx]
			cfg.Password = userInfo[idx+1:]
		} else {
			cfg.User = userInfo
		}
	}

	// Parse network address: protocol(address)
	if netAddr != "" {
		if idx := strings.Index(netAddr, "("); idx != -1 {
			cfg.Protocol = netAddr[:idx]
			endIdx := strings.Index(netAddr, ")")
			if endIdx == -1 {
				return nil, errors.New("invalid DSN: missing closing parenthesis")
			}
			address := netAddr[idx+1 : endIdx]

			if cfg.Protocol == "unix" {
				cfg.Socket = address
			} else {
				// Parse host:port
				host, port, err := net.SplitHostPort(address)
				if err != nil {
					// No port specified, use default
					cfg.Host = address
				} else {
					cfg.Host = host
					p, err := strconv.Atoi(port)
					if err != nil {
						return nil, fmt.Errorf("invalid port: %s", port)
					}
					cfg.Port = p
				}
			}
		} else {
			// No protocol specified, assume tcp
			cfg.Protocol = "tcp"
			host, port, err := net.SplitHostPort(netAddr)
			if err != nil {
				cfg.Host = netAddr
			} else {
				cfg.Host = host
				p, err := strconv.Atoi(port)
				if err != nil {
					return nil, fmt.Errorf("invalid port: %s", port)
				}
				cfg.Port = p
			}
		}
	}

	// Set database name
	cfg.Database = dbName

	// Parse parameters
	if params != "" {
		values, err := url.ParseQuery(params)
		if err != nil {
			return nil, fmt.Errorf("invalid DSN parameters: %w", err)
		}

		for key, vals := range values {
			if len(vals) == 0 {
				continue
			}
			value := vals[0]

			switch key {
			// Timeouts
			case "timeout":
				d, err := time.ParseDuration(value)
				if err != nil {
					return nil, fmt.Errorf("invalid timeout: %w", err)
				}
				cfg.Timeout = d

			case "readTimeout":
				d, err := time.ParseDuration(value)
				if err != nil {
					return nil, fmt.Errorf("invalid readTimeout: %w", err)
				}
				cfg.ReadTimeout = d

			case "writeTimeout":
				d, err := time.ParseDuration(value)
				if err != nil {
					return nil, fmt.Errorf("invalid writeTimeout: %w", err)
				}
				cfg.WriteTimeout = d

			// TLS
			case "tls":
				cfg.TLSConfig = value
				if value == "true" || value == "skip-verify" || value == "preferred" {
					cfg.TLS = &tls.Config{
						InsecureSkipVerify: value == "skip-verify" || value == "preferred",
					}
					if value == "preferred" {
						cfg.AllowFallbackToPlaintext = true
					}
				}

			case "ca":
				if cfg.TLS == nil {
					cfg.TLS = &tls.Config{}
				}
				caCert, err := os.ReadFile(value)
				if err != nil {
					return nil, fmt.Errorf("failed to read CA certificate: %w", err)
				}
				caCertPool := x509.NewCertPool()
				if !caCertPool.AppendCertsFromPEM(caCert) {
					return nil, errors.New("failed to parse CA certificate")
				}
				cfg.TLS.RootCAs = caCertPool

			case "cert", "key":
				// Store for later loading with both cert and key
				cfg.Params[key] = value

			// Character set and collation
			case "charset":
				cfg.Charset = value
				cfg.Charsets = strings.Split(value, ",")

			case "collation":
				cfg.Collation = value

			// Location
			case "loc":
				if value, err := url.QueryUnescape(value); err == nil {
					cfg.Loc, err = time.LoadLocation(value)
					if err != nil {
						return nil, fmt.Errorf("invalid loc: %w", err)
					}
				}

			// Packet size
			case "maxAllowedPacket":
				size, err := strconv.Atoi(value)
				if err != nil {
					return nil, fmt.Errorf("invalid maxAllowedPacket: %w", err)
				}
				cfg.MaxAllowedPacket = size

			// Server public key
			case "serverPubKey":
				if name, err := url.QueryUnescape(value); err == nil {
					cfg.ServerPubKey = name
				}

			// Connection attributes
			case "connectionAttributes":
				if attrs, err := url.QueryUnescape(value); err == nil {
					cfg.ConnectionAttributes = attrs
				}

			// Time truncate
			case "timeTruncate":
				d, err := time.ParseDuration(value)
				if err != nil {
					return nil, fmt.Errorf("invalid timeTruncate: %w", err)
				}
				cfg.TimeTruncate = d

			// Boolean options
			case "allowAllFiles":
				cfg.AllowAllFiles = parseBool(value)

			case "allowCleartextPasswords":
				cfg.AllowCleartextPasswords = parseBool(value)

			case "allowFallbackToPlaintext":
				cfg.AllowFallbackToPlaintext = parseBool(value)

			case "allowNativePasswords":
				cfg.AllowNativePasswords = parseBool(value)

			case "allowOldPasswords":
				cfg.AllowOldPasswords = parseBool(value)

			case "checkConnLiveness":
				cfg.CheckConnLiveness = parseBool(value)

			case "clientFoundRows":
				cfg.ClientFoundRows = parseBool(value)

			case "columnsWithAlias":
				cfg.ColumnsWithAlias = parseBool(value)

			case "compress":
				cfg.Compress = parseBool(value)

			case "interpolateParams":
				cfg.InterpolateParams = parseBool(value)

			case "multiStatements":
				cfg.MultiStatements = parseBool(value)

			case "parseTime":
				cfg.ParseTime = parseBool(value)

			case "rejectReadOnly":
				cfg.RejectReadOnly = parseBool(value)

			case "debug":
				cfg.Debug = parseBool(value)

			default:
				if cfg.Params == nil {
					cfg.Params = make(map[string]string)
				}
				if unescaped, err := url.QueryUnescape(value); err == nil {
					cfg.Params[key] = unescaped
				} else {
					cfg.Params[key] = value
				}
			}
		}

		// Load client certificate if both cert and key are specified
		if certFile, hasCert := cfg.Params["cert"]; hasCert {
			if keyFile, hasKey := cfg.Params["key"]; hasKey {
				if cfg.TLS == nil {
					cfg.TLS = &tls.Config{}
				}
				cert, err := tls.LoadX509KeyPair(certFile, keyFile)
				if err != nil {
					return nil, fmt.Errorf("failed to load client certificate: %w", err)
				}
				cfg.TLS.Certificates = []tls.Certificate{cert}
				delete(cfg.Params, "cert")
				delete(cfg.Params, "key")
			}
		}
	}

	// Normalize configuration
	if err := normalizeConfig(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}

// parseBool parses a boolean value from string
func parseBool(value string) bool {
	switch strings.ToLower(value) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

// normalizeConfig normalizes and validates the configuration
func normalizeConfig(cfg *Config) error {
	// Set default network if empty
	if cfg.Protocol == "" {
		cfg.Protocol = "tcp"
	}

	// Compute Addr from Host/Port or Socket
	if cfg.Addr == "" {
		if cfg.Protocol == "unix" {
			if cfg.Socket != "" {
				cfg.Addr = cfg.Socket
			} else {
				cfg.Addr = "/tmp/mysql.sock"
				cfg.Socket = cfg.Addr
			}
		} else {
			if cfg.Host == "" {
				cfg.Host = "127.0.0.1"
			}
			if cfg.Port == 0 {
				cfg.Port = 3306
			}
			cfg.Addr = net.JoinHostPort(cfg.Host, strconv.Itoa(cfg.Port))
		}
	}

	// Set TLS ServerName if not set
	if cfg.TLS != nil && cfg.TLS.ServerName == "" && !cfg.TLS.InsecureSkipVerify {
		host, _, err := net.SplitHostPort(cfg.Addr)
		if err == nil {
			cfg.TLS.ServerName = host
		}
	}

	return nil
}

// FormatDSN formats a Config into a DSN string
func FormatDSN(cfg *Config) string {
	if cfg == nil {
		return ""
	}

	var buf strings.Builder

	// User info
	if cfg.User != "" {
		buf.WriteString(cfg.User)
		if cfg.Password != "" {
			buf.WriteByte(':')
			buf.WriteString(cfg.Password)
		}
		buf.WriteByte('@')
	}

	// Protocol and address
	buf.WriteString(cfg.Protocol)
	buf.WriteByte('(')
	if cfg.Protocol == "unix" {
		buf.WriteString(cfg.Socket)
	} else {
		buf.WriteString(cfg.Host)
		if cfg.Port != 3306 {
			buf.WriteByte(':')
			buf.WriteString(strconv.Itoa(cfg.Port))
		}
	}
	buf.WriteByte(')')

	// Database
	buf.WriteByte('/')
	buf.WriteString(cfg.Database)

	// Parameters
	hasParams := false
	addParam := func(key, value string) {
		if !hasParams {
			buf.WriteByte('?')
			hasParams = true
		} else {
			buf.WriteByte('&')
		}
		buf.WriteString(key)
		buf.WriteByte('=')
		buf.WriteString(url.QueryEscape(value))
	}

	// Timeouts
	if cfg.Timeout > 0 && cfg.Timeout != 10*time.Second {
		addParam("timeout", cfg.Timeout.String())
	}
	if cfg.ReadTimeout > 0 {
		addParam("readTimeout", cfg.ReadTimeout.String())
	}
	if cfg.WriteTimeout > 0 {
		addParam("writeTimeout", cfg.WriteTimeout.String())
	}

	// TLS
	if cfg.TLSConfig != "" {
		addParam("tls", cfg.TLSConfig)
	}

	// Character set and collation
	if cfg.Charset != "" && cfg.Charset != "utf8mb4" {
		addParam("charset", cfg.Charset)
	}
	if cfg.Collation != "" {
		addParam("collation", cfg.Collation)
	}

	// Location
	if cfg.Loc != nil && cfg.Loc != time.UTC {
		addParam("loc", cfg.Loc.String())
	}

	// Packet size
	if cfg.MaxAllowedPacket > 0 && cfg.MaxAllowedPacket != 64*1024*1024 {
		addParam("maxAllowedPacket", strconv.Itoa(cfg.MaxAllowedPacket))
	}

	// Server public key
	if cfg.ServerPubKey != "" {
		addParam("serverPubKey", cfg.ServerPubKey)
	}

	// Connection attributes
	if cfg.ConnectionAttributes != "" {
		addParam("connectionAttributes", cfg.ConnectionAttributes)
	}

	// Time truncate
	if cfg.TimeTruncate > 0 {
		addParam("timeTruncate", cfg.TimeTruncate.String())
	}

	// Boolean options (only add if non-default)
	if cfg.AllowAllFiles {
		addParam("allowAllFiles", "true")
	}
	if cfg.AllowCleartextPasswords {
		addParam("allowCleartextPasswords", "true")
	}
	if cfg.AllowFallbackToPlaintext {
		addParam("allowFallbackToPlaintext", "true")
	}
	if !cfg.AllowNativePasswords {
		addParam("allowNativePasswords", "false")
	}
	if cfg.AllowOldPasswords {
		addParam("allowOldPasswords", "true")
	}
	if !cfg.CheckConnLiveness {
		addParam("checkConnLiveness", "false")
	}
	if cfg.ClientFoundRows {
		addParam("clientFoundRows", "true")
	}
	if cfg.ColumnsWithAlias {
		addParam("columnsWithAlias", "true")
	}
	if cfg.Compress {
		addParam("compress", "true")
	}
	if cfg.InterpolateParams {
		addParam("interpolateParams", "true")
	}
	if cfg.MultiStatements {
		addParam("multiStatements", "true")
	}
	if cfg.ParseTime {
		addParam("parseTime", "true")
	}
	if cfg.RejectReadOnly {
		addParam("rejectReadOnly", "true")
	}

	// Custom parameters
	for key, value := range cfg.Params {
		addParam(key, value)
	}

	return buf.String()
}
