// Package validation provides transaction validation functionality for the mempool system.
package validation

import "context"

// CommittedTxStore defines the interface for tracking committed transaction hashes.
type CommittedTxStore interface {
	// Contains checks if a transaction with the given hash has been committed.
	Contains(ctx context.Context, hash [32]byte) (bool, error)

	// Add marks a transaction hash as committed.
	Add(ctx context.Context, hash [32]byte) error

	// AddBatch marks multiple transaction hashes as committed.
	AddBatch(ctx context.Context, hashes [][32]byte) error
}
