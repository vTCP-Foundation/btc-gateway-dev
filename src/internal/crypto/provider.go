// Package crypto provides cryptographic interfaces and implementations for transaction signing and verification.
package crypto

// CryptoProvider defines the interface for cryptographic operations used in transaction processing.
type CryptoProvider interface {
	// Sign signs the given data using the provided private key.
	Sign(privateKey []byte, data []byte) ([]byte, error)

	// Verify verifies that the signature is valid for the given data and public key.
	Verify(publicKey []byte, data []byte, signature []byte) (bool, error)

	// ValidateAddress checks if the given address is valid for this crypto provider.
	ValidateAddress(address string) bool

	// DeriveAddress derives an address from the given public key.
	DeriveAddress(publicKey []byte) string

	// Hash computes a cryptographic hash of the given data.
	Hash(data []byte) [32]byte
}
