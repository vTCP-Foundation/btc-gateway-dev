// Package mempool provides transaction pooling functionality for the mempool system.
package mempool

import (
	"errors"
	"sync"

	"btc-gateway/internal/statemachine"
)

// ErrDuplicateTransaction indicates the transaction already exists in the pool.
var ErrDuplicateTransaction = errors.New("duplicate transaction in pool")

// Pool defines the interface for transaction pooling.
type Pool interface {
	// Add adds a transaction to the pool.
	// Returns ErrDuplicateTransaction if the transaction already exists.
	Add(tx *statemachine.Transaction) error

	// Remove removes transactions with the given hashes from the pool.
	Remove(hashes [][32]byte)

	// Drain removes and returns up to limit bytes of transactions from the pool.
	// Transactions are returned in FIFO order.
	Drain(limitBytes int) []*statemachine.Transaction

	// Peek returns up to limit bytes of transactions from the pool without removing them.
	// Transactions are returned in FIFO order.
	Peek(limitBytes int) []*statemachine.Transaction

	// Contains checks if a transaction with the given hash is in the pool.
	Contains(hash [32]byte) bool

	// ContainsSenderNonce checks if a transaction with the given sender and nonce exists in the pool.
	ContainsSenderNonce(sender string, nonce uint64) bool

	// Size returns the number of transactions in the pool.
	Size() int

	// Pending returns the number of pending transactions for a given sender address.
	Pending(address string) int
}

// FIFOPool implements Pool using a FIFO queue with O(1) lookup.
type FIFOPool struct {
	mu           sync.RWMutex
	txs          []*statemachine.Transaction // FIFO order
	txByHash     map[[32]byte]*statemachine.Transaction
	senderIndex  map[string]int              // Count of pending TXs per sender
	senderNonces map[string]map[uint64]bool  // (sender, nonce) index for duplicate detection
}

// NewFIFOPool creates a new FIFO transaction pool.
func NewFIFOPool() *FIFOPool {
	return &FIFOPool{
		txs:          make([]*statemachine.Transaction, 0),
		txByHash:     make(map[[32]byte]*statemachine.Transaction),
		senderIndex:  make(map[string]int),
		senderNonces: make(map[string]map[uint64]bool),
	}
}

// Add adds a transaction to the pool.
// Returns ErrDuplicateTransaction if the transaction already exists.
func (p *FIFOPool) Add(tx *statemachine.Transaction) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	hash := tx.Hash()

	// Check if already exists by hash
	if _, exists := p.txByHash[hash]; exists {
		return ErrDuplicateTransaction
	}

	// Add to FIFO queue
	p.txs = append(p.txs, tx)
	p.txByHash[hash] = tx

	// Update sender index and sender-nonce tracking
	if tx.Sender != "" {
		p.senderIndex[tx.Sender]++

		// Track sender-nonce pair
		if p.senderNonces[tx.Sender] == nil {
			p.senderNonces[tx.Sender] = make(map[uint64]bool)
		}
		p.senderNonces[tx.Sender][tx.Nonce] = true
	}

	return nil
}

// Remove removes transactions with the given hashes from the pool.
func (p *FIFOPool) Remove(hashes [][32]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Build a set of hashes to remove for O(1) lookup
	toRemove := make(map[[32]byte]bool, len(hashes))
	for _, hash := range hashes {
		toRemove[hash] = true
	}

	// Filter out removed transactions
	newTxs := make([]*statemachine.Transaction, 0, len(p.txs))
	for _, tx := range p.txs {
		hash := tx.Hash()
		if toRemove[hash] {
			// Remove from maps
			delete(p.txByHash, hash)
			if tx.Sender != "" {
				p.senderIndex[tx.Sender]--
				if p.senderIndex[tx.Sender] <= 0 {
					delete(p.senderIndex, tx.Sender)
				}
				// Clean up sender-nonce index
				if p.senderNonces[tx.Sender] != nil {
					delete(p.senderNonces[tx.Sender], tx.Nonce)
					if len(p.senderNonces[tx.Sender]) == 0 {
						delete(p.senderNonces, tx.Sender)
					}
				}
			}
		} else {
			newTxs = append(newTxs, tx)
		}
	}
	p.txs = newTxs
}

// Drain removes and returns up to limitBytes of transactions from the pool.
func (p *FIFOPool) Drain(limitBytes int) []*statemachine.Transaction {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]*statemachine.Transaction, 0)
	totalSize := 0
	removeCount := 0

	for _, tx := range p.txs {
		// Estimate transaction size (JSON serialization)
		txData, err := tx.Serialize()
		if err != nil {
			continue
		}
		txSize := len(txData)

		// Check if adding this TX would exceed limit
		if totalSize+txSize > limitBytes && len(result) > 0 {
			break
		}

		result = append(result, tx)
		totalSize += txSize
		removeCount++

		// Remove from lookup maps
		hash := tx.Hash()
		delete(p.txByHash, hash)
		if tx.Sender != "" {
			p.senderIndex[tx.Sender]--
			if p.senderIndex[tx.Sender] <= 0 {
				delete(p.senderIndex, tx.Sender)
			}
			// Clean up sender-nonce index
			if p.senderNonces[tx.Sender] != nil {
				delete(p.senderNonces[tx.Sender], tx.Nonce)
				if len(p.senderNonces[tx.Sender]) == 0 {
					delete(p.senderNonces, tx.Sender)
				}
			}
		}
	}

	// Remove drained transactions from the slice
	if removeCount > 0 {
		p.txs = p.txs[removeCount:]
	}

	return result
}

// Peek returns up to limitBytes of transactions from the pool without removing them.
func (p *FIFOPool) Peek(limitBytes int) []*statemachine.Transaction {
	p.mu.RLock()
	defer p.mu.RUnlock()

	result := make([]*statemachine.Transaction, 0)
	totalSize := 0

	for _, tx := range p.txs {
		// Estimate transaction size (JSON serialization)
		txData, err := tx.Serialize()
		if err != nil {
			continue
		}
		txSize := len(txData)

		// Check if adding this TX would exceed limit
		if totalSize+txSize > limitBytes && len(result) > 0 {
			break
		}

		result = append(result, tx)
		totalSize += txSize
	}

	return result
}

// Contains checks if a transaction with the given hash is in the pool.
func (p *FIFOPool) Contains(hash [32]byte) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	_, exists := p.txByHash[hash]
	return exists
}

// ContainsSenderNonce checks if a transaction with the given sender and nonce exists in the pool.
func (p *FIFOPool) ContainsSenderNonce(sender string, nonce uint64) bool {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if p.senderNonces[sender] == nil {
		return false
	}
	return p.senderNonces[sender][nonce]
}

// Size returns the number of transactions in the pool.
func (p *FIFOPool) Size() int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return len(p.txs)
}

// Pending returns the number of pending transactions for a given sender address.
func (p *FIFOPool) Pending(address string) int {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.senderIndex[address]
}

// Ensure FIFOPool implements Pool.
var _ Pool = (*FIFOPool)(nil)
