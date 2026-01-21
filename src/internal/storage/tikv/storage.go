package tikv

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"

	"btc-gateway/pkg/consensus/storage"
	"btc-gateway/pkg/consensus/types"
)

// Key prefixes for different data types
const (
	prefixBlock       = "block:"
	prefixQC          = "qc:"
	prefixHeight      = "height:"
	prefixMeta        = "meta:"
	prefixAppState    = "state:kv:"
	prefixLastApplied = "state:meta:last_applied"

	keyCurrentView = "meta:current_view"
	keyHighestQC   = "meta:highest_qc"
)

// Storage implements StorageInterface using TikV as the backend.
// It provides persistence for both consensus state and application state.
type Storage struct {
	client    *rawkv.Client
	config    *Config
	mu        sync.RWMutex
	connected bool
}

// NewStorage creates a new TikV storage instance.
func NewStorage(config *Config) *Storage {
	if config == nil {
		config = DefaultConfig()
	}
	return &Storage{
		config: config,
	}
}

// Connect establishes connection to TikV cluster.
func (s *Storage) Connect(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.connected {
		return nil
	}

	if err := s.config.Validate(); err != nil {
		return err
	}

	// Create client with empty security config (no TLS)
	client, err := rawkv.NewClient(ctx, s.config.PDAddrs, config.Security{})
	if err != nil {
		return fmt.Errorf("failed to connect to TikV: %w", err)
	}

	s.client = client
	s.connected = true
	return nil
}

// Close closes the TikV connection.
func (s *Storage) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.connected {
		return nil
	}

	err := s.client.Close()
	s.client = nil
	s.connected = false
	return err
}

// GetClient returns the underlying rawkv client.
// This is used by other components that need direct TiKV access.
func (s *Storage) GetClient() *rawkv.Client {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.client
}

// makeKey creates a full key with the configured prefix.
func (s *Storage) makeKey(parts ...string) []byte {
	key := s.config.KeyPrefix
	for _, part := range parts {
		key += part
	}
	return []byte(key)
}

// StoreBlock stores a block in TikV.
func (s *Storage) StoreBlock(block *types.Block) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return ErrNotConnected
	}

	// Serialize block
	data, err := json.Marshal(block)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSerializationFailed, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.WriteTimeout)
	defer cancel()

	// Store block by hash
	blockKey := s.makeKey(prefixBlock, fmt.Sprintf("%x", block.Hash))
	if err := s.client.Put(ctx, blockKey, data); err != nil {
		return fmt.Errorf("failed to store block: %w", err)
	}

	// Store height index
	heightKey := s.makeKey(prefixHeight, fmt.Sprintf("%d:%x", block.Height, block.Hash))
	if err := s.client.Put(ctx, heightKey, block.Hash[:]); err != nil {
		return fmt.Errorf("failed to store height index: %w", err)
	}

	return nil
}

// GetBlock retrieves a block by hash from TikV.
func (s *Storage) GetBlock(hash types.BlockHash) (*types.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return nil, ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadTimeout)
	defer cancel()

	key := s.makeKey(prefixBlock, fmt.Sprintf("%x", hash))
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get block: %w", err)
	}
	if data == nil {
		return nil, storage.NewStorageError(storage.ErrorTypeNotFound, fmt.Sprintf("block not found: %x", hash))
	}

	var block types.Block
	if err := json.Unmarshal(data, &block); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDeserializationFailed, err)
	}

	return &block, nil
}

// StoreQC stores a quorum certificate in TikV.
func (s *Storage) StoreQC(qc *types.QuorumCertificate) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return ErrNotConnected
	}

	// Serialize QC
	data, err := json.Marshal(qc)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSerializationFailed, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.WriteTimeout)
	defer cancel()

	// Store QC by block hash and phase
	key := s.makeKey(prefixQC, fmt.Sprintf("%x:%s", qc.BlockHash, qc.Phase.String()))
	if err := s.client.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to store QC: %w", err)
	}

	// Check if this is the highest QC and update if so
	highestQC, _ := s.getHighestQCUnsafe(ctx)
	if highestQC == nil || qc.View > highestQC.View {
		if err := s.storeHighestQCUnsafe(ctx, qc); err != nil {
			return fmt.Errorf("failed to update highest QC: %w", err)
		}
	}

	return nil
}

// GetQC retrieves a quorum certificate by block hash and phase.
func (s *Storage) GetQC(blockHash types.BlockHash, phase types.ConsensusPhase) (*types.QuorumCertificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return nil, ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadTimeout)
	defer cancel()

	key := s.makeKey(prefixQC, fmt.Sprintf("%x:%s", blockHash, phase.String()))
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get QC: %w", err)
	}
	if data == nil {
		return nil, storage.NewStorageError(storage.ErrorTypeNotFound,
			fmt.Sprintf("QC not found: block=%x, phase=%s", blockHash, phase.String()))
	}

	var qc types.QuorumCertificate
	if err := json.Unmarshal(data, &qc); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDeserializationFailed, err)
	}

	return &qc, nil
}

// StoreView stores the current view number.
func (s *Storage) StoreView(view types.ViewNumber) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.WriteTimeout)
	defer cancel()

	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(view))

	key := s.makeKey(keyCurrentView)
	if err := s.client.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to store view: %w", err)
	}

	return nil
}

// GetCurrentView retrieves the current view number.
func (s *Storage) GetCurrentView() (types.ViewNumber, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return 0, ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadTimeout)
	defer cancel()

	key := s.makeKey(keyCurrentView)
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return 0, fmt.Errorf("failed to get view: %w", err)
	}
	if data == nil {
		return 0, nil // Return 0 if not set
	}

	if len(data) < 8 {
		return 0, fmt.Errorf("%w: invalid view data length", ErrDeserializationFailed)
	}

	return types.ViewNumber(binary.BigEndian.Uint64(data)), nil
}

// GetBlocksByHeight retrieves all blocks at a given height.
func (s *Storage) GetBlocksByHeight(height types.Height) ([]*types.Block, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return nil, ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadTimeout)
	defer cancel()

	// Scan for all blocks at this height
	startKey := s.makeKey(prefixHeight, fmt.Sprintf("%d:", height))
	endKey := s.makeKey(prefixHeight, fmt.Sprintf("%d;", height)) // ';' is after ':' in ASCII

	keys, _, err := s.client.Scan(ctx, startKey, endKey, 1000)
	if err != nil {
		return nil, fmt.Errorf("failed to scan height index: %w", err)
	}

	var blocks []*types.Block
	for _, key := range keys {
		// Extract block hash from key
		keyStr := string(key)
		// Key format: prefix + "height:{height}:{hash}"
		// We need to extract the hash part
		var hash types.BlockHash
		n, err := fmt.Sscanf(keyStr[len(s.config.KeyPrefix)+len(prefixHeight):], "%d:%x", new(types.Height), &hash)
		if err != nil || n != 2 {
			continue
		}

		block, err := s.GetBlock(hash)
		if err != nil {
			continue
		}
		blocks = append(blocks, block)
	}

	return blocks, nil
}

// GetHighestQC retrieves the highest quorum certificate.
func (s *Storage) GetHighestQC() (*types.QuorumCertificate, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return nil, ErrNotConnected
	}

	ctx, cancel := context.WithTimeout(context.Background(), s.config.ReadTimeout)
	defer cancel()

	return s.getHighestQCUnsafe(ctx)
}

// getHighestQCUnsafe retrieves the highest QC without locking.
func (s *Storage) getHighestQCUnsafe(ctx context.Context) (*types.QuorumCertificate, error) {
	key := s.makeKey(keyHighestQC)
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get highest QC: %w", err)
	}
	if data == nil {
		return nil, storage.NewStorageError(storage.ErrorTypeNotFound, "no highest QC found")
	}

	var qc types.QuorumCertificate
	if err := json.Unmarshal(data, &qc); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrDeserializationFailed, err)
	}

	return &qc, nil
}

// storeHighestQCUnsafe stores the highest QC without locking.
func (s *Storage) storeHighestQCUnsafe(ctx context.Context, qc *types.QuorumCertificate) error {
	data, err := json.Marshal(qc)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSerializationFailed, err)
	}

	key := s.makeKey(keyHighestQC)
	return s.client.Put(ctx, key, data)
}

// Ensure Storage implements StorageInterface
var _ storage.StorageInterface = (*Storage)(nil)
