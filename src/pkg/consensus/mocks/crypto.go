package mocks

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"sync"

	"btc-gateway/pkg/consensus/crypto"
	"btc-gateway/pkg/consensus/types"
)

// CryptoConfig contains configuration parameters for MockCrypto behavior.
type CryptoConfig struct {
	// EnableDeterministicSignatures makes signatures predictable for testing
	EnableDeterministicSignatures bool
	// SignatureLength is the length of generated signatures in bytes
	SignatureLength int
}

// DefaultCryptoConfig returns a configuration suitable for most tests.
func DefaultCryptoConfig() CryptoConfig {
	return CryptoConfig{
		EnableDeterministicSignatures: true,
		SignatureLength:               64, // Simulate Ed25519 signatures
	}
}

// CryptoFailureConfig contains failure injection parameters for crypto operations.
type CryptoFailureConfig struct {
	// SignFailureRate is the probability of signing operations failing
	SignFailureRate float64
	// VerifyFailureRate is the probability of verification operations failing
	VerifyFailureRate float64
	// InvalidSignatureRate is the probability of generating invalid signatures
	InvalidSignatureRate float64
	// VerificationRejectRate is the probability of rejecting valid signatures
	VerificationRejectRate float64
}

// DefaultCryptoFailureConfig returns a failure configuration with no failures.
func DefaultCryptoFailureConfig() CryptoFailureConfig {
	return CryptoFailureConfig{
		SignFailureRate:        0.0,
		VerifyFailureRate:      0.0,
		InvalidSignatureRate:   0.0,
		VerificationRejectRate: 0.0,
	}
}

// MockCrypto implements CryptoInterface for testing with deterministic behavior.
type MockCrypto struct {
	nodeID     types.NodeID
	keys       map[types.NodeID][]byte
	signatures map[string][]byte // Cache for deterministic signatures
	config     CryptoConfig
	failures   CryptoFailureConfig
	mu         sync.RWMutex
	rand       *rand.Rand
	signCount  uint64
	verifyCount uint64
	hashCount  uint64
}

// NewMockCrypto creates a new MockCrypto instance.
func NewMockCrypto(nodeID types.NodeID, config CryptoConfig, failures CryptoFailureConfig) *MockCrypto {
	mc := &MockCrypto{
		nodeID:     nodeID,
		keys:       make(map[types.NodeID][]byte),
		signatures: make(map[string][]byte),
		config:     config,
		failures:   failures,
		rand:       rand.New(rand.NewSource(int64(nodeID) + 12345)),
	}
	
	// Generate a deterministic "public key" for this node
	mc.keys[nodeID] = mc.generateKey(nodeID)
	
	return mc
}

// AddNode adds a node to the known keys set.
func (mc *MockCrypto) AddNode(nodeID types.NodeID) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	if _, exists := mc.keys[nodeID]; !exists {
		mc.keys[nodeID] = mc.generateKey(nodeID)
	}
}

// Sign creates a signature for the given data.
func (mc *MockCrypto) Sign(data []byte) ([]byte, error) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.signCount++
	
	// Simulate sign failures
	if mc.rand.Float64() < mc.failures.SignFailureRate {
		return nil, crypto.NewCryptoError(crypto.ErrorTypeSignature, "simulated signing failure")
	}
	
	var signature []byte
	
	if mc.config.EnableDeterministicSignatures {
		// Generate deterministic signature based on data and node ID
		key := fmt.Sprintf("%d:%x", mc.nodeID, sha256.Sum256(data))
		if sig, exists := mc.signatures[key]; exists {
			signature = sig
		} else {
			signature = mc.generateDeterministicSignature(data)
			mc.signatures[key] = signature
		}
	} else {
		// Generate random signature
		signature = make([]byte, mc.config.SignatureLength)
		mc.rand.Read(signature)
	}
	
	// Simulate generating invalid signatures
	if mc.rand.Float64() < mc.failures.InvalidSignatureRate {
		// Corrupt the signature
		signature[0] ^= 0xFF
	}
	
	return signature, nil
}

// Verify checks if a signature is valid for the given data and node ID.
func (mc *MockCrypto) Verify(data []byte, signature []byte, nodeID types.NodeID) error {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	mc.verifyCount++
	
	// Simulate verify failures
	if mc.rand.Float64() < mc.failures.VerifyFailureRate {
		return crypto.NewCryptoError(crypto.ErrorTypeVerification, "simulated verification failure")
	}
	
	// Check if we know this node
	if _, exists := mc.keys[nodeID]; !exists {
		return crypto.NewCryptoError(crypto.ErrorTypeInvalidKey, fmt.Sprintf("unknown node ID: %d", nodeID))
	}
	
	// Basic signature validation
	if len(signature) != mc.config.SignatureLength {
		return crypto.NewCryptoError(crypto.ErrorTypeVerification, 
			fmt.Sprintf("invalid signature length: got %d, expected %d", len(signature), mc.config.SignatureLength))
	}
	
	if mc.config.EnableDeterministicSignatures {
		// For deterministic mode, recreate expected signature and compare
		expectedSig := mc.generateDeterministicSignatureForNode(data, nodeID)
		
		// Check if signatures match
		if len(expectedSig) != len(signature) {
			return crypto.NewCryptoError(crypto.ErrorTypeVerification, "signature length mismatch")
		}
		
		for i := range expectedSig {
			if expectedSig[i] != signature[i] {
				return crypto.NewCryptoError(crypto.ErrorTypeVerification, "signature verification failed")
			}
		}
	} else {
		// For non-deterministic mode, we accept any signature of correct length
		// This simulates a scenario where we can't recreate the signature but can validate its format
		// In real crypto, this would involve actual signature verification algorithms
	}
	
	// Simulate rejecting valid signatures
	if mc.rand.Float64() < mc.failures.VerificationRejectRate {
		return crypto.NewCryptoError(crypto.ErrorTypeVerification, "simulated verification rejection")
	}
	
	return nil
}

// Hash computes SHA-256 hash of the given data.
func (mc *MockCrypto) Hash(data []byte) types.BlockHash {
	mc.mu.Lock()
	mc.hashCount++
	mc.mu.Unlock()
	
	// Use real SHA-256 for consistency across the system
	return sha256.Sum256(data)
}

// GetStats returns crypto operation statistics.
func (mc *MockCrypto) GetStats() CryptoStats {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	
	return CryptoStats{
		NodeID:      mc.nodeID,
		KnownNodes:  len(mc.keys),
		SignCount:   mc.signCount,
		VerifyCount: mc.verifyCount,
		HashCount:   mc.hashCount,
	}
}

// UpdateFailures updates the failure configuration.
func (mc *MockCrypto) UpdateFailures(failures CryptoFailureConfig) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.failures = failures
}

// Clear resets all cached signatures (useful for test cleanup).
func (mc *MockCrypto) Clear() {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	
	mc.signatures = make(map[string][]byte)
	mc.signCount = 0
	mc.verifyCount = 0
	mc.hashCount = 0
}

// CryptoStats contains runtime statistics for MockCrypto.
type CryptoStats struct {
	NodeID      types.NodeID
	KnownNodes  int
	SignCount   uint64
	VerifyCount uint64
	HashCount   uint64
}

// generateKey generates a deterministic "public key" for a node ID.
func (mc *MockCrypto) generateKey(nodeID types.NodeID) []byte {
	key := make([]byte, 32) // Simulate 32-byte public key
	seed := sha256.Sum256([]byte(fmt.Sprintf("node-%d-key", nodeID)))
	copy(key, seed[:])
	return key
}

// generateDeterministicSignature creates a deterministic signature for this node.
func (mc *MockCrypto) generateDeterministicSignature(data []byte) []byte {
	return mc.generateDeterministicSignatureForNode(data, mc.nodeID)
}

// generateDeterministicSignatureForNode creates a deterministic signature for any node.
func (mc *MockCrypto) generateDeterministicSignatureForNode(data []byte, nodeID types.NodeID) []byte {
	// Combine node ID, data hash, and a salt for deterministic signature generation
	input := fmt.Sprintf("sig-%d-%x", nodeID, sha256.Sum256(data))
	hash := sha256.Sum256([]byte(input))
	
	signature := make([]byte, mc.config.SignatureLength)
	
	// Fill signature with repeated hash until we reach desired length
	for i := 0; i < mc.config.SignatureLength; i++ {
		signature[i] = hash[i%32]
	}
	
	return signature
}