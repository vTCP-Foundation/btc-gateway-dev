// Package network defines error types for network interface operations.
package network

import "fmt"

// NetworkErrorType represents the category of network error.
type NetworkErrorType int

const (
	ErrorTypeConnection NetworkErrorType = iota
	ErrorTypeTimeout
	ErrorTypeMessageDelivery
	ErrorTypeNodeNotFound
)

// NetworkError represents an error that occurred during network operations.
type NetworkError struct {
	Type    NetworkErrorType
	Message string
	Cause   error
}

// NewNetworkError creates a new network error with the specified type and message.
func NewNetworkError(errorType NetworkErrorType, message string) *NetworkError {
	return &NetworkError{
		Type:    errorType,
		Message: message,
	}
}

// NewNetworkErrorWithCause creates a new network error with an underlying cause.
func NewNetworkErrorWithCause(errorType NetworkErrorType, message string, cause error) *NetworkError {
	return &NetworkError{
		Type:    errorType,
		Message: message,
		Cause:   cause,
	}
}

// Error returns the string representation of the network error.
func (e *NetworkError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("network error (%s): %s - caused by: %v", e.typeString(), e.Message, e.Cause)
	}
	return fmt.Sprintf("network error (%s): %s", e.typeString(), e.Message)
}

// Unwrap returns the underlying cause of the error for error unwrapping.
func (e *NetworkError) Unwrap() error {
	return e.Cause
}

// Sentinel errors for use with errors.Is
var (
	ErrConnection      = &NetworkError{Type: ErrorTypeConnection, Message: "connection error"}
	ErrTimeout         = &NetworkError{Type: ErrorTypeTimeout, Message: "timeout error"}
	ErrMessageDelivery = &NetworkError{Type: ErrorTypeMessageDelivery, Message: "message delivery error"}
	ErrNodeNotFound    = &NetworkError{Type: ErrorTypeNodeNotFound, Message: "node not found"}
)

// typeString returns a human-readable representation of the error type.
func (e *NetworkError) typeString() string {
	switch e.Type {
	case ErrorTypeConnection:
		return "connection"
	case ErrorTypeTimeout:
		return "timeout"
	case ErrorTypeMessageDelivery:
		return "message_delivery"
	case ErrorTypeNodeNotFound:
		return "node_not_found"
	default:
		return "unknown"
	}
}

// IsNetworkError checks if an error is a NetworkError of a specific type.
func IsNetworkError(err error, errorType NetworkErrorType) bool {
	if netErr, ok := err.(*NetworkError); ok {
		return netErr.Type == errorType
	}
	return false
}