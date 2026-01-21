package network

import (
	"errors"
	"fmt"
)

// Common network errors
var (
	ErrNotStarted       = errors.New("network manager is not started")
	ErrAlreadyStarted   = errors.New("network manager is already started")
	ErrInvalidAddress   = errors.New("invalid multiaddr format")
	ErrConnectionFailed = errors.New("connection failed")
	ErrProtocolNotFound = errors.New("protocol handler not found")
	ErrHandlerExists    = errors.New("protocol handler already exists")
	ErrInvalidConfig    = errors.New("invalid network configuration")
)

// NetworkError represents a network-specific error with additional context
type NetworkError struct {
	Operation string
	Cause     error
	Context   map[string]interface{}
}

// Error implements the error interface
func (e *NetworkError) Error() string {
	if e.Context != nil && len(e.Context) > 0 {
		return fmt.Sprintf("network error in %s: %v (context: %v)", e.Operation, e.Cause, e.Context)
	}
	return fmt.Sprintf("network error in %s: %v", e.Operation, e.Cause)
}

// Unwrap returns the underlying error
func (e *NetworkError) Unwrap() error {
	return e.Cause
}

// NewNetworkError creates a new network error with context
func NewNetworkError(operation string, cause error, context map[string]interface{}) *NetworkError {
	return &NetworkError{
		Operation: operation,
		Cause:     cause,
		Context:   context,
	}
}

// IsTemporary indicates if the error is temporary and the operation should be retried
func (e *NetworkError) IsTemporary() bool {
	// Define which errors are considered temporary
	switch e.Cause {
	case ErrConnectionFailed:
		return true
	default:
		return false
	}
}

// ConnectionError represents a connection-specific error
type ConnectionError struct {
	PeerID  string
	Address string
	Cause   error
}

// Error implements the error interface
func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection error with peer %s at %s: %v", e.PeerID, e.Address, e.Cause)
}

// Unwrap returns the underlying error
func (e *ConnectionError) Unwrap() error {
	return e.Cause
}

// ProtocolError represents a protocol-specific error
type ProtocolError struct {
	ProtocolID string
	Cause      error
}

// Error implements the error interface
func (e *ProtocolError) Error() string {
	return fmt.Sprintf("protocol error for %s: %v", e.ProtocolID, e.Cause)
}

// Unwrap returns the underlying error
func (e *ProtocolError) Unwrap() error {
	return e.Cause
}
