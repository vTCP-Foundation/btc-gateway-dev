// Package storage defines error types for storage interface operations.
package storage

import "fmt"

// StorageErrorType represents the category of storage error.
type StorageErrorType int

const (
	ErrorTypeNotFound StorageErrorType = iota
	ErrorTypePersistence
	ErrorTypeRetrieval
	ErrorTypeCorruption
	ErrorTypeInvalidData
)

// StorageError represents an error that occurred during storage operations.
type StorageError struct {
	Type    StorageErrorType
	Message string
	Cause   error
}

// NewStorageError creates a new storage error with the specified type and message.
func NewStorageError(errorType StorageErrorType, message string) *StorageError {
	return &StorageError{
		Type:    errorType,
		Message: message,
	}
}

// NewStorageErrorWithCause creates a new storage error with an underlying cause.
func NewStorageErrorWithCause(errorType StorageErrorType, message string, cause error) *StorageError {
	return &StorageError{
		Type:    errorType,
		Message: message,
		Cause:   cause,
	}
}

// Error returns the string representation of the storage error.
func (e *StorageError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("storage error (%s): %s - caused by: %v", e.typeString(), e.Message, e.Cause)
	}
	return fmt.Sprintf("storage error (%s): %s", e.typeString(), e.Message)
}

// Unwrap returns the underlying cause of the error for error unwrapping.
func (e *StorageError) Unwrap() error {
	return e.Cause
}

// Sentinel errors for use with errors.Is
var (
	ErrNotFound     = &StorageError{Type: ErrorTypeNotFound, Message: "not found"}
	ErrPersistence  = &StorageError{Type: ErrorTypePersistence, Message: "persistence error"}
	ErrRetrieval    = &StorageError{Type: ErrorTypeRetrieval, Message: "retrieval error"}
	ErrCorruption   = &StorageError{Type: ErrorTypeCorruption, Message: "data corruption"}
	ErrInvalidData  = &StorageError{Type: ErrorTypeInvalidData, Message: "invalid data"}
)

// typeString returns a human-readable representation of the error type.
func (e *StorageError) typeString() string {
	switch e.Type {
	case ErrorTypeNotFound:
		return "not_found"
	case ErrorTypePersistence:
		return "persistence"
	case ErrorTypeRetrieval:
		return "retrieval"
	case ErrorTypeCorruption:
		return "corruption"
	case ErrorTypeInvalidData:
		return "invalid_data"
	default:
		return "unknown"
	}
}

// IsStorageError checks if an error is a StorageError of a specific type.
func IsStorageError(err error, errorType StorageErrorType) bool {
	if storageErr, ok := err.(*StorageError); ok {
		return storageErr.Type == errorType
	}
	return false
}