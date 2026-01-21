// Package tikv provides a TikV-based storage implementation for consensus and application state.
package tikv

import (
	"time"
)

// Config holds TikV connection configuration.
type Config struct {
	// PDAddrs is a list of PD (Placement Driver) addresses.
	// Example: ["127.0.0.1:2379"]
	PDAddrs []string

	// ConnectTimeout is the timeout for establishing connection.
	ConnectTimeout time.Duration

	// ReadTimeout is the timeout for read operations.
	ReadTimeout time.Duration

	// WriteTimeout is the timeout for write operations.
	WriteTimeout time.Duration

	// MaxRetries is the maximum number of retries on failure.
	MaxRetries int

	// KeyPrefix is an optional prefix for all keys (useful for namespacing).
	KeyPrefix string
}

// DefaultConfig returns a default TikV configuration.
func DefaultConfig() *Config {
	return &Config{
		PDAddrs:        []string{"127.0.0.1:2379"},
		ConnectTimeout: 10 * time.Second,
		ReadTimeout:    5 * time.Second,
		WriteTimeout:   5 * time.Second,
		MaxRetries:     3,
		KeyPrefix:      "kvstore:",
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if len(c.PDAddrs) == 0 {
		return ErrNoPDAddrs
	}
	return nil
}
