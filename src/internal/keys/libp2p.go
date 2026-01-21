package keys

import (
	"crypto/ed25519"
	"encoding/base64"
	"fmt"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
)

// ToLibp2pPrivateKey converts a base64-encoded Ed25519 private key to a libp2p crypto.PrivKey
func (km *KeyManager) ToLibp2pPrivateKey(privateKeyBase64 string) (crypto.PrivKey, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(privateKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid private key base64: %w", err)
	}

	if len(keyBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key length: expected %d, got %d", ed25519.PrivateKeySize, len(keyBytes))
	}

	// libp2p Ed25519 keys use the same format as Go's crypto/ed25519
	privKey, err := crypto.UnmarshalEd25519PrivateKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal libp2p private key: %w", err)
	}

	return privKey, nil
}

// ToLibp2pPublicKey converts a base64-encoded Ed25519 public key to a libp2p crypto.PubKey
func (km *KeyManager) ToLibp2pPublicKey(publicKeyBase64 string) (crypto.PubKey, error) {
	keyBytes, err := base64.StdEncoding.DecodeString(publicKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid public key base64: %w", err)
	}

	if len(keyBytes) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid public key length: expected %d, got %d", ed25519.PublicKeySize, len(keyBytes))
	}

	pubKey, err := crypto.UnmarshalEd25519PublicKey(keyBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal libp2p public key: %w", err)
	}

	return pubKey, nil
}

// PublicKeyToPeerID converts a base64-encoded Ed25519 public key to a libp2p peer.ID
func (km *KeyManager) PublicKeyToPeerID(publicKeyBase64 string) (peer.ID, error) {
	pubKey, err := km.ToLibp2pPublicKey(publicKeyBase64)
	if err != nil {
		return "", err
	}

	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("failed to derive peer ID from public key: %w", err)
	}

	return peerID, nil
}

// PrivateKeyToPeerID derives the peer.ID from a base64-encoded private key
func (km *KeyManager) PrivateKeyToPeerID(privateKeyBase64 string) (peer.ID, error) {
	privKey, err := km.ToLibp2pPrivateKey(privateKeyBase64)
	if err != nil {
		return "", err
	}

	peerID, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return "", fmt.Errorf("failed to derive peer ID from private key: %w", err)
	}

	return peerID, nil
}
