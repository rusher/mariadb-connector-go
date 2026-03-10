// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2026 MariaDB Corporation Ab

package mariadb

import "context"

// Context keys for query options
type contextKey int

const (
	// ContextKeyFetchSize is the context key for setting fetch size per query
	// Value should be int. 0 = complete fetch (all rows), >0 = streaming with specified batch size
	ContextKeyFetchSize contextKey = iota
)

// WithFetchSize returns a context with the specified fetch size
// fetchSize: 0 = fetch all rows immediately (CompleteRows)
//
//	>0 = streaming mode with specified batch size (StreamingRows)
func WithFetchSize(ctx context.Context, fetchSize int) context.Context {
	return context.WithValue(ctx, ContextKeyFetchSize, fetchSize)
}

// GetFetchSize extracts the fetch size from context, returns default if not set
func GetFetchSize(ctx context.Context, defaultSize int) int {
	if v := ctx.Value(ContextKeyFetchSize); v != nil {
		if size, ok := v.(int); ok {
			return size
		}
	}
	return defaultSize
}
