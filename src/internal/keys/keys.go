package keys

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"fmt"
)

// KeyManager handles cryptographic key operations
type KeyManager struct{}

// NewKeyManager creates a new KeyManager instance
func NewKeyManager() *KeyManager {
	return &KeyManager{}
}

// GeneratePrivateKey generates a new Ed25519 private key and returns it as base64
func (km *KeyManager) GeneratePrivateKey() (string, error) {
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return "", fmt.Errorf("failed to generate Ed25519 key: %w", err)
	}

	return base64.StdEncoding.EncodeToString(privateKey), nil
}

// ValidatePrivateKey validates that a private key string is valid base64 and correct length
func (km *KeyManager) ValidatePrivateKey(privateKeyBase64 string) error {
	if privateKeyBase64 == "" {
		return nil // Empty is valid - will be generated
	}

	keyBytes, err := base64.StdEncoding.DecodeString(privateKeyBase64)
	if err != nil {
		return fmt.Errorf("private key must be valid base64: %w", err)
	}

	if len(keyBytes) != ed25519.PrivateKeySize {
		return fmt.Errorf("private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(keyBytes))
	}

	return nil
}

// GetPublicKey derives the public key from a private key
func (km *KeyManager) GetPublicKey(privateKeyBase64 string) (string, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(privateKeyBase64)
	if err != nil {
		return "", fmt.Errorf("invalid private key base64: %w", err)
	}

	if len(keyBytes) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("invalid private key length")
	}

	privateKey := ed25519.PrivateKey(keyBytes)
	publicKey := privateKey.Public().(ed25519.PublicKey)

	return base64.StdEncoding.EncodeToString(publicKey), nil
}