// Package tikv provides TiKV-backed implementations of validation stores.
package tikv

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/tikv/client-go/v2/rawkv"

	"btc-gateway/internal/validation"
)

const (
	// Key prefix for committed transaction hashes
	prefixCommitted = "committed:"

	// Default timeouts
	defaultReadTimeout  = 5 * time.Second
	defaultWriteTimeout = 5 * time.Second
)

// CommittedStore implements validation.CommittedTxStore using TiKV as the backend.
type CommittedStore struct {
	client       *rawkv.Client
	keyPrefix    string
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewCommittedStore creates a new TiKV-backed committed transaction store.
func NewCommittedStore(client *rawkv.Client, keyPrefix string) *CommittedStore {
	return &CommittedStore{
		client:       client,
		keyPrefix:    keyPrefix,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}
}

// makeKey creates a full key with the configured prefix.
func (s *CommittedStore) makeKey(hash [32]byte) []byte {
	return []byte(s.keyPrefix + prefixCommitted + hex.EncodeToString(hash[:]))
}

// Contains checks if a transaction with the given hash has been committed.
func (s *CommittedStore) Contains(ctx context.Context, hash [32]byte) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()

	key := s.makeKey(hash)
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return false, fmt.Errorf("failed to check committed tx: %w", err)
	}

	return data != nil, nil
}

// Add marks a transaction hash as committed.
func (s *CommittedStore) Add(ctx context.Context, hash [32]byte) error {
	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()

	key := s.makeKey(hash)
	// Store empty value - we only care about key existence
	if err := s.client.Put(ctx, key, []byte{}); err != nil {
		return fmt.Errorf("failed to add committed tx: %w", err)
	}

	return nil
}

// AddBatch marks multiple transaction hashes as committed.
func (s *CommittedStore) AddBatch(ctx context.Context, hashes [][32]byte) error {
	if len(hashes) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()

	keys := make([][]byte, len(hashes))
	values := make([][]byte, len(hashes))
	emptyValue := []byte{}

	for i, hash := range hashes {
		keys[i] = s.makeKey(hash)
		values[i] = emptyValue
	}

	if err := s.client.BatchPut(ctx, keys, values); err != nil {
		return fmt.Errorf("failed to batch add committed txs: %w", err)
	}

	return nil
}

// Ensure CommittedStore implements validation.CommittedTxStore.
var _ validation.CommittedTxStore = (*CommittedStore)(nil)
