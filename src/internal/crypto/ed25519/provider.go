// Package ed25519 provides an Ed25519-based implementation of the CryptoProvider interface.
package ed25519

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"btc-gateway/internal/crypto"
)

// Provider implements CryptoProvider using Ed25519 signatures.
type Provider struct{}

// NewProvider creates a new Ed25519 crypto provider.
func NewProvider() *Provider {
	return &Provider{}
}

// Sign signs the given data using the provided Ed25519 private key.
func (p *Provider) Sign(privateKey []byte, data []byte) ([]byte, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key size: expected %d, got %d", ed25519.PrivateKeySize, len(privateKey))
	}
	return ed25519.Sign(privateKey, data), nil
}

// Verify verifies that the signature is valid for the given data and public key.
func (p *Provider) Verify(publicKey []byte, data []byte, signature []byte) (bool, error) {
	if len(publicKey) != ed25519.PublicKeySize {
		return false, fmt.Errorf("invalid public key size: expected %d, got %d", ed25519.PublicKeySize, len(publicKey))
	}
	if len(signature) != ed25519.SignatureSize {
		return false, fmt.Errorf("invalid signature size: expected %d, got %d", ed25519.SignatureSize, len(signature))
	}
	return ed25519.Verify(publicKey, data, signature), nil
}

// ValidateAddress checks if the given address is a valid hex-encoded Ed25519 public key.
// Valid addresses are hex strings of 64 characters (32 bytes encoded).
func (p *Provider) ValidateAddress(address string) bool {
	if len(address) != 64 {
		return false
	}
	_, err := hex.DecodeString(address)
	return err == nil
}

// DeriveAddress derives an address from the given Ed25519 public key.
// The address is the hex-encoded public key.
func (p *Provider) DeriveAddress(publicKey []byte) string {
	return hex.EncodeToString(publicKey)
}

// Hash computes a SHA-256 hash of the given data.
func (p *Provider) Hash(data []byte) [32]byte {
	return sha256.Sum256(data)
}

// Ensure Provider implements CryptoProvider.
var _ crypto.CryptoProvider = (*Provider)(nil)
