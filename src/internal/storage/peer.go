package storage

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/multiformats/go-multiaddr"
	"gopkg.in/yaml.v3"
)

// Peer represents a network peer with validation capabilities
type Peer struct {
	PublicKey string   `yaml:"public_key" validate:"required,base64"`
	Addresses []string `yaml:"addresses" validate:"required,min=1,dive,required"`
}

// PeerStorage defines the interface for peer storage operations
type PeerStorage interface {
	GetPeers() ([]Peer, error)
	LoadPeers() error
	SavePeers(peers []Peer) error
}

// PeerFile represents the YAML structure for peers.yaml
type PeerFile struct {
	Peers []Peer `yaml:"peers"`
}

// FilePeerStorage implements PeerStorage using YAML file storage
type FilePeerStorage struct {
	filePath string
	mutex    sync.RWMutex
	peers    []Peer
}

// NewFilePeerStorage creates a new file-based peer storage
func NewFilePeerStorage(filePath string) *FilePeerStorage {
	return &FilePeerStorage{
		filePath: filePath,
		peers:    make([]Peer, 0),
	}
}

// GetPeers returns the current list of peers
func (s *FilePeerStorage) GetPeers() ([]Peer, error) {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	// Return a copy to prevent external modification
	peers := make([]Peer, len(s.peers))
	copy(peers, s.peers)
	return peers, nil
}

// LoadPeers loads peers from the YAML file
func (s *FilePeerStorage) LoadPeers() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	file, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			// File doesn't exist, start with empty peer list
			s.peers = make([]Peer, 0)
			return nil
		}
		return fmt.Errorf("failed to open peer file %s: %w", s.filePath, err)
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("failed to read peer file: %w", err)
	}

	var peerFile PeerFile
	if err := yaml.Unmarshal(data, &peerFile); err != nil {
		return fmt.Errorf("failed to parse peer file: %w", err)
	}

	// Validate and filter peers
	validPeers, err := s.validateAndFilterPeers(peerFile.Peers)
	if err != nil {
		return fmt.Errorf("peer validation failed: %w", err)
	}

	s.peers = validPeers
	return nil
}

// SavePeers saves peers to the YAML file using atomic operations
func (s *FilePeerStorage) SavePeers(peers []Peer) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Validate and filter peers before saving
	validPeers, err := s.validateAndFilterPeers(peers)
	if err != nil {
		return fmt.Errorf("peer validation failed: %w", err)
	}

	peerFile := PeerFile{
		Peers: validPeers,
	}

	data, err := yaml.Marshal(&peerFile)
	if err != nil {
		return fmt.Errorf("failed to marshal peers: %w", err)
	}

	// Atomic write: write to temporary file first, then rename
	dir := filepath.Dir(s.filePath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempFile := s.filePath + ".tmp"
	if err := os.WriteFile(tempFile, data, 0644); err != nil {
		return fmt.Errorf("failed to write temporary file: %w", err)
	}

	if err := os.Rename(tempFile, s.filePath); err != nil {
		// Clean up temp file on error
		os.Remove(tempFile)
		return fmt.Errorf("failed to atomic rename: %w", err)
	}

	s.peers = validPeers
	return nil
}

// validateAndFilterPeers validates peer data and removes duplicates
func (s *FilePeerStorage) validateAndFilterPeers(peers []Peer) ([]Peer, error) {
	seen := make(map[string]bool)
	validPeers := make([]Peer, 0, len(peers))

	for i, peer := range peers {
		// Validate public key format
		if err := s.validatePublicKey(peer.PublicKey); err != nil {
			return nil, fmt.Errorf("invalid public key at index %d: %w", i, err)
		}

		// Validate addresses
		if err := s.validateAddresses(peer.Addresses); err != nil {
			return nil, fmt.Errorf("invalid addresses at index %d: %w", i, err)
		}

		// Check for duplicates using public key
		if seen[peer.PublicKey] {
			continue // Skip duplicate
		}
		seen[peer.PublicKey] = true

		validPeers = append(validPeers, peer)
	}

	return validPeers, nil
}

// validatePublicKey validates the format of a base64-encoded public key
func (s *FilePeerStorage) validatePublicKey(publicKey string) error {
	if publicKey == "" {
		return fmt.Errorf("public key cannot be empty")
	}

	decoded, err := base64.StdEncoding.DecodeString(publicKey)
	if err != nil {
		return fmt.Errorf("public key is not valid base64: %w", err)
	}

	// Basic length check - libp2p ed25519 public keys are typically 32 bytes
	// We allow some flexibility for different key types
	if len(decoded) < 16 || len(decoded) > 128 {
		return fmt.Errorf("public key length %d is outside acceptable range (16-128 bytes)", len(decoded))
	}

	return nil
}

// validateAddresses validates that all addresses are valid multiaddresses
func (s *FilePeerStorage) validateAddresses(addresses []string) error {
	if len(addresses) == 0 {
		return fmt.Errorf("at least one address must be provided")
	}

	for i, addrStr := range addresses {
		if addrStr == "" {
			return fmt.Errorf("address at index %d cannot be empty", i)
		}

		_, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return fmt.Errorf("invalid multiaddress at index %d (%s): %w", i, addrStr, err)
		}
	}

	return nil
}

// CreateBackup creates a backup of the current peer file
func (s *FilePeerStorage) CreateBackup() error {
	s.mutex.RLock()
	defer s.mutex.RUnlock()

	if _, err := os.Stat(s.filePath); os.IsNotExist(err) {
		return nil // No file to backup
	}

	// Create backup with timestamp
	timestamp := time.Now().Format("20060102-150405")
	backupPath := s.filePath + ".backup." + timestamp

	source, err := os.Open(s.filePath)
	if err != nil {
		return fmt.Errorf("failed to open source file: %w", err)
	}
	defer source.Close()

	dest, err := os.Create(backupPath)
	if err != nil {
		return fmt.Errorf("failed to create backup file: %w", err)
	}
	defer dest.Close()

	if _, err := io.Copy(dest, source); err != nil {
		return fmt.Errorf("failed to copy to backup: %w", err)
	}

	return nil
}

// GetFileChecksum returns SHA256 checksum of the peer file for integrity checking
func (s *FilePeerStorage) GetFileChecksum() (string, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil // No file means no checksum
		}
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("failed to compute checksum: %w", err)
	}

	return hex.EncodeToString(hash.Sum(nil)), nil
}
