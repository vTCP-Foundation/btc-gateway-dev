package tikv

import "errors"

var (
	// ErrNoPDAddrs is returned when no PD addresses are configured.
	ErrNoPDAddrs = errors.New("no PD addresses configured")

	// ErrNotConnected is returned when operations are attempted before connecting.
	ErrNotConnected = errors.New("not connected to TikV")

	// ErrNotFound is returned when a key is not found.
	ErrNotFound = errors.New("key not found")

	// ErrSerializationFailed is returned when serialization fails.
	ErrSerializationFailed = errors.New("serialization failed")

	// ErrDeserializationFailed is returned when deserialization fails.
	ErrDeserializationFailed = errors.New("deserialization failed")
)
