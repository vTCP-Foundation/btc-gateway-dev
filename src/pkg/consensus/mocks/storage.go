package mocks

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"btc-gateway/pkg/consensus/storage"
	"btc-gateway/pkg/consensus/types"
)

// StorageConfig contains configuration parameters for MockStorage behavior.
type StorageConfig struct {
	// PersistenceDelay simulates disk write delays
	PersistenceDelay time.Duration
	// DelayVariation is the random variation added to persistence delay
	DelayVariation time.Duration
	// EnableDurabilitySimulation simulates persistence across restarts
	EnableDurabilitySimulation bool
}

// DefaultStorageConfig returns a configuration suitable for most tests.
func DefaultStorageConfig() StorageConfig {
	return StorageConfig{
		PersistenceDelay:           1 * time.Millisecond,
		DelayVariation:             500 * time.Microsecond,
		EnableDurabilitySimulation: false,
	}
}

// StorageFailureConfig contains failure injection parameters for storage operations.
type StorageFailureConfig struct {
	// WriteFailureRate is the probability of write operations failing
	WriteFailureRate float64
	// ReadFailureRate is the probability of read operations failing
	ReadFailureRate float64
	// CorruptionRate is the probability of data corruption during reads
	CorruptionRate float64
	// DiskFullSimulation simulates disk space exhaustion
	DiskFullSimulation bool
}

// DefaultStorageFailureConfig returns a failure configuration with no failures.
func DefaultStorageFailureConfig() StorageFailureConfig {
	return StorageFailureConfig{
		WriteFailureRate:   0.0,
		ReadFailureRate:    0.0,
		CorruptionRate:     0.0,
		DiskFullSimulation: false,
	}
}

// MockStorage implements StorageInterface for testing with configurable behavior.
type MockStorage struct {
	blocks      map[types.BlockHash]*types.Block
	qcs         map[string]*types.QuorumCertificate
	currentView types.ViewNumber
	config      StorageConfig
	failures    StorageFailureConfig
	mu          sync.RWMutex
	rand        *rand.Rand
	writeCount  uint64
	readCount   uint64
}

// NewMockStorage creates a new MockStorage instance.
func NewMockStorage(config StorageConfig, failures StorageFailureConfig) *MockStorage {
	return &MockStorage{
		blocks:   make(map[types.BlockHash]*types.Block),
		qcs:      make(map[string]*types.QuorumCertificate),
		config:   config,
		failures: failures,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// StoreBlock stores a block in the mock storage.
func (ms *MockStorage) StoreBlock(block *types.Block) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	
	ms.writeCount++
	
	// Simulate disk full
	if ms.failures.DiskFullSimulation {
		return storage.NewStorageError(storage.ErrorTypePersistence, "simulated disk full")
	}
	
	// Simulate write failures
	if ms.rand.Float64() < ms.failures.WriteFailureRate {
		return storage.NewStorageError(storage.ErrorTypePersistence, "simulated write failure")
	}
	
	// Simulate persistence delay
	if ms.config.PersistenceDelay > 0 {
		delay := ms.config.PersistenceDelay
		if ms.config.DelayVariation > 0 {
			variation := time.Duration(ms.rand.Int63n(int64(ms.config.DelayVariation)))
			delay += variation
		}
		time.Sleep(delay)
	}
	
	// Store the block
	ms.blocks[block.Hash] = block
	
	return nil
}

// GetBlock retrieves a block by hash from the mock storage.
func (ms *MockStorage) GetBlock(hash types.BlockHash) (*types.Block, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	ms.readCount++
	
	// Simulate read failures
	if ms.rand.Float64() < ms.failures.ReadFailureRate {
		return nil, storage.NewStorageError(storage.ErrorTypeRetrieval, "simulated read failure")
	}
	
	// Simulate persistence delay
	if ms.config.PersistenceDelay > 0 {
		delay := ms.config.PersistenceDelay / 2 // Reads are typically faster
		if ms.config.DelayVariation > 0 {
			variation := time.Duration(ms.rand.Int63n(int64(ms.config.DelayVariation / 2)))
			delay += variation
		}
		time.Sleep(delay)
	}
	
	block, exists := ms.blocks[hash]
	if !exists {
		return nil, storage.NewStorageError(storage.ErrorTypeNotFound, fmt.Sprintf("block not found: %x", hash))
	}
	
	// Simulate data corruption
	if ms.rand.Float64() < ms.failures.CorruptionRate {
		// Return a corrupted copy of the block
		corruptedBlock := *block
		corruptedBlock.Height = block.Height + 999999 // Obviously corrupted
		return &corruptedBlock, nil
	}
	
	return block, nil
}

// StoreQC stores a quorum certificate in the mock storage.
func (ms *MockStorage) StoreQC(qc *types.QuorumCertificate) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	
	ms.writeCount++
	
	// Simulate disk full
	if ms.failures.DiskFullSimulation {
		return storage.NewStorageError(storage.ErrorTypePersistence, "simulated disk full")
	}
	
	// Simulate write failures
	if ms.rand.Float64() < ms.failures.WriteFailureRate {
		return storage.NewStorageError(storage.ErrorTypePersistence, "simulated write failure")
	}
	
	// Simulate persistence delay
	if ms.config.PersistenceDelay > 0 {
		delay := ms.config.PersistenceDelay
		if ms.config.DelayVariation > 0 {
			variation := time.Duration(ms.rand.Int63n(int64(ms.config.DelayVariation)))
			delay += variation
		}
		time.Sleep(delay)
	}
	
	// Create composite key
	key := fmt.Sprintf("%x:%s", qc.BlockHash, qc.Phase.String())
	ms.qcs[key] = qc
	
	return nil
}

// GetQC retrieves a quorum certificate by block hash and phase.
func (ms *MockStorage) GetQC(blockHash types.BlockHash, phase types.ConsensusPhase) (*types.QuorumCertificate, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	ms.readCount++
	
	// Simulate read failures
	if ms.rand.Float64() < ms.failures.ReadFailureRate {
		return nil, storage.NewStorageError(storage.ErrorTypeRetrieval, "simulated read failure")
	}
	
	// Simulate persistence delay
	if ms.config.PersistenceDelay > 0 {
		delay := ms.config.PersistenceDelay / 2 // Reads are typically faster
		time.Sleep(delay)
	}
	
	// Create composite key
	key := fmt.Sprintf("%x:%s", blockHash, phase.String())
	qc, exists := ms.qcs[key]
	if !exists {
		return nil, storage.NewStorageError(storage.ErrorTypeNotFound, 
			fmt.Sprintf("QC not found: block=%x, phase=%s", blockHash, phase.String()))
	}
	
	// Simulate data corruption
	if ms.rand.Float64() < ms.failures.CorruptionRate {
		// Return a corrupted copy of the QC
		corruptedQC := *qc
		corruptedQC.VoterBitmap = []byte{0} // Corrupted bitmap
		return &corruptedQC, nil
	}
	
	return qc, nil
}

// StoreView stores the current view number.
func (ms *MockStorage) StoreView(view types.ViewNumber) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	
	ms.writeCount++
	
	// Simulate write failures
	if ms.rand.Float64() < ms.failures.WriteFailureRate {
		return storage.NewStorageError(storage.ErrorTypePersistence, "simulated write failure")
	}
	
	// Simulate persistence delay
	if ms.config.PersistenceDelay > 0 {
		delay := ms.config.PersistenceDelay / 4 // Small data write
		time.Sleep(delay)
	}
	
	ms.currentView = view
	return nil
}

// GetCurrentView retrieves the current view number.
func (ms *MockStorage) GetCurrentView() (types.ViewNumber, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	ms.readCount++
	
	// Simulate read failures
	if ms.rand.Float64() < ms.failures.ReadFailureRate {
		return 0, storage.NewStorageError(storage.ErrorTypeRetrieval, "simulated read failure")
	}
	
	return ms.currentView, nil
}

// GetBlocksByHeight retrieves all blocks at a given height.
func (ms *MockStorage) GetBlocksByHeight(height types.Height) ([]*types.Block, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	ms.readCount++
	
	// Simulate read failures
	if ms.rand.Float64() < ms.failures.ReadFailureRate {
		return nil, storage.NewStorageError(storage.ErrorTypeRetrieval, "simulated read failure")
	}
	
	// Simulate persistence delay
	if ms.config.PersistenceDelay > 0 {
		delay := ms.config.PersistenceDelay
		time.Sleep(delay)
	}
	
	var blocks []*types.Block
	for _, block := range ms.blocks {
		if block.Height == height {
			// Simulate corruption for individual blocks
			if ms.rand.Float64() < ms.failures.CorruptionRate {
				corruptedBlock := *block
				corruptedBlock.Height = block.Height + 999999
				blocks = append(blocks, &corruptedBlock)
			} else {
				blocks = append(blocks, block)
			}
		}
	}
	
	return blocks, nil
}

// GetHighestQC retrieves the QC with the highest view number.
func (ms *MockStorage) GetHighestQC() (*types.QuorumCertificate, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	ms.readCount++
	
	// Simulate read failures
	if ms.rand.Float64() < ms.failures.ReadFailureRate {
		return nil, storage.NewStorageError(storage.ErrorTypeRetrieval, "simulated read failure")
	}
	
	// Simulate persistence delay
	if ms.config.PersistenceDelay > 0 {
		delay := ms.config.PersistenceDelay
		time.Sleep(delay)
	}
	
	var highestQC *types.QuorumCertificate
	for _, qc := range ms.qcs {
		if highestQC == nil || qc.View > highestQC.View {
			highestQC = qc
		}
	}
	
	if highestQC == nil {
		return nil, storage.NewStorageError(storage.ErrorTypeNotFound, "no QC found")
	}
	
	// Simulate data corruption
	if ms.rand.Float64() < ms.failures.CorruptionRate {
		corruptedQC := *highestQC
		corruptedQC.VoterBitmap = []byte{0} // Corrupted bitmap
		return &corruptedQC, nil
	}
	
	return highestQC, nil
}

// Clear removes all stored data (useful for test cleanup).
func (ms *MockStorage) Clear() {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	
	ms.blocks = make(map[types.BlockHash]*types.Block)
	ms.qcs = make(map[string]*types.QuorumCertificate)
	ms.currentView = 0
	ms.writeCount = 0
	ms.readCount = 0
}

// UpdateFailures updates the failure configuration.
func (ms *MockStorage) UpdateFailures(failures StorageFailureConfig) {
	ms.mu.Lock()
	defer ms.mu.Unlock()
	ms.failures = failures
}

// GetStats returns storage statistics for monitoring.
func (ms *MockStorage) GetStats() StorageStats {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	
	return StorageStats{
		BlockCount:  len(ms.blocks),
		QCCount:     len(ms.qcs),
		CurrentView: ms.currentView,
		WriteCount:  ms.writeCount,
		ReadCount:   ms.readCount,
	}
}

// StorageStats contains runtime statistics for MockStorage.
type StorageStats struct {
	BlockCount  int
	QCCount     int
	CurrentView types.ViewNumber
	WriteCount  uint64
	ReadCount   uint64
}

// GetAllBlocks returns deep copies of all stored blocks.
// This is used for state synchronization in tests.
// Returns deep copies to prevent cross-node mutation leaks.
func (ms *MockStorage) GetAllBlocks() map[types.BlockHash]*types.Block {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make(map[types.BlockHash]*types.Block, len(ms.blocks))
	for hash, block := range ms.blocks {
		// Deep copy to prevent cross-node mutation
		blockCopy := *block
		result[hash] = &blockCopy
	}
	return result
}

// GetAllQCs returns deep copies of all stored quorum certificates.
// This is used for state synchronization in tests.
// Returns deep copies to prevent cross-node mutation leaks.
func (ms *MockStorage) GetAllQCs() map[string]*types.QuorumCertificate {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	result := make(map[string]*types.QuorumCertificate, len(ms.qcs))
	for key, qc := range ms.qcs {
		// Deep copy to prevent cross-node mutation
		qcCopy := *qc
		// Deep copy slices
		if qc.Votes != nil {
			qcCopy.Votes = make([]types.Vote, len(qc.Votes))
			copy(qcCopy.Votes, qc.Votes)
		}
		if qc.VoterBitmap != nil {
			qcCopy.VoterBitmap = make([]byte, len(qc.VoterBitmap))
			copy(qcCopy.VoterBitmap, qc.VoterBitmap)
		}
		result[key] = &qcCopy
	}
	return result
}

// ImportState imports blocks and QCs from another storage.
// This is used for state synchronization in tests.
// For blocks: always imports (blocks are immutable by hash).
// For QCs: overwrites if incoming QC has higher view (newer data wins).
func (ms *MockStorage) ImportState(blocks map[types.BlockHash]*types.Block, qcs map[string]*types.QuorumCertificate) error {
	ms.mu.Lock()
	defer ms.mu.Unlock()

	// Import blocks (blocks are immutable - same hash = same content)
	for hash, block := range blocks {
		if _, exists := ms.blocks[hash]; !exists {
			ms.blocks[hash] = block
		}
	}

	// Import QCs (overwrite if incoming has higher view - newer data wins)
	for key, qc := range qcs {
		existing, exists := ms.qcs[key]
		if !exists || qc.View > existing.View {
			ms.qcs[key] = qc
		}
	}

	return nil
}