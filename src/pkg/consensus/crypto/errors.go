// Package crypto defines error types for cryptographic interface operations.
package crypto

import "fmt"

// CryptoErrorType represents the category of cryptographic error.
type CryptoErrorType int

const (
	ErrorTypeSignature CryptoErrorType = iota
	ErrorTypeVerification
	ErrorTypeKeyManagement
	ErrorTypeHashing
	ErrorTypeInvalidKey
)

// CryptoError represents an error that occurred during cryptographic operations.
type CryptoError struct {
	Type    CryptoErrorType
	Message string
	Cause   error
}

// NewCryptoError creates a new crypto error with the specified type and message.
func NewCryptoError(errorType CryptoErrorType, message string) *CryptoError {
	return &CryptoError{
		Type:    errorType,
		Message: message,
	}
}

// NewCryptoErrorWithCause creates a new crypto error with an underlying cause.
func NewCryptoErrorWithCause(errorType CryptoErrorType, message string, cause error) *CryptoError {
	return &CryptoError{
		Type:    errorType,
		Message: message,
		Cause:   cause,
	}
}

// Error returns the string representation of the crypto error.
func (e *CryptoError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("crypto error (%s): %s - caused by: %v", e.typeString(), e.Message, e.Cause)
	}
	return fmt.Sprintf("crypto error (%s): %s", e.typeString(), e.Message)
}

// Unwrap returns the underlying cause of the error for error unwrapping.
func (e *CryptoError) Unwrap() error {
	return e.Cause
}

// Sentinel errors for use with errors.Is
var (
	ErrSignature      = &CryptoError{Type: ErrorTypeSignature, Message: "signature error"}
	ErrVerification   = &CryptoError{Type: ErrorTypeVerification, Message: "verification failed"}
	ErrKeyManagement  = &CryptoError{Type: ErrorTypeKeyManagement, Message: "key management error"}
	ErrHashing        = &CryptoError{Type: ErrorTypeHashing, Message: "hashing error"}
	ErrInvalidKey     = &CryptoError{Type: ErrorTypeInvalidKey, Message: "invalid key"}
)

// typeString returns a human-readable representation of the error type.
func (e *CryptoError) typeString() string {
	switch e.Type {
	case ErrorTypeSignature:
		return "signature"
	case ErrorTypeVerification:
		return "verification"
	case ErrorTypeKeyManagement:
		return "key_management"
	case ErrorTypeHashing:
		return "hashing"
	case ErrorTypeInvalidKey:
		return "invalid_key"
	default:
		return "unknown"
	}
}

// IsCryptoError checks if an error is a CryptoError of a specific type.
func IsCryptoError(err error, errorType CryptoErrorType) bool {
	if cryptoErr, ok := err.(*CryptoError); ok {
		return cryptoErr.Type == errorType
	}
	return false
}