// Package tikv provides TiKV-backed implementations of account storage.
package tikv

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/tikv/client-go/v2/rawkv"

	"btc-gateway/internal/account"
)

const (
	// Key prefix for account data
	prefixAccount = "account:"

	// Default timeouts
	defaultReadTimeout  = 5 * time.Second
	defaultWriteTimeout = 5 * time.Second
)

// Store implements account.Store using TiKV as the backend.
type Store struct {
	client       *rawkv.Client
	keyPrefix    string
	readTimeout  time.Duration
	writeTimeout time.Duration
	mu           sync.RWMutex
}

// NewStore creates a new TiKV-backed account store.
func NewStore(client *rawkv.Client, keyPrefix string) *Store {
	return &Store{
		client:       client,
		keyPrefix:    keyPrefix,
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}
}

// makeKey creates a full key with the configured prefix.
func (s *Store) makeKey(address string) []byte {
	return []byte(s.keyPrefix + prefixAccount + address)
}

// Get retrieves an account by address.
func (s *Store) Get(ctx context.Context, address string) (*account.Account, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	ctx, cancel := context.WithTimeout(ctx, s.readTimeout)
	defer cancel()

	key := s.makeKey(address)
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if data == nil {
		return nil, nil // Not found
	}

	var acc account.Account
	if err := json.Unmarshal(data, &acc); err != nil {
		return nil, fmt.Errorf("failed to deserialize account: %w", err)
	}

	return &acc, nil
}

// Save persists an account.
func (s *Store) Save(ctx context.Context, acc *account.Account) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()

	data, err := json.Marshal(acc)
	if err != nil {
		return fmt.Errorf("failed to serialize account: %w", err)
	}

	key := s.makeKey(acc.Address)
	if err := s.client.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to save account: %w", err)
	}

	return nil
}

// IncrementNonce increments the nonce for the account at the given address.
func (s *Store) IncrementNonce(ctx context.Context, address string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()

	// Get existing account or create new one
	acc, err := s.getUnsafe(ctx, address)
	if err != nil {
		return err
	}
	if acc == nil {
		acc = account.NewAccount(address)
	}

	acc.IncrementNonce()

	return s.saveUnsafe(ctx, acc)
}

// SetBalance sets the balance for the account at the given address.
func (s *Store) SetBalance(ctx context.Context, address string, amount uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()

	// Get existing account or create new one
	acc, err := s.getUnsafe(ctx, address)
	if err != nil {
		return err
	}
	if acc == nil {
		acc = account.NewAccount(address)
	}

	acc.SetBalance(amount)

	return s.saveUnsafe(ctx, acc)
}

// GetOrCreate retrieves an account by address, creating it if it doesn't exist.
func (s *Store) GetOrCreate(ctx context.Context, address string) (*account.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, s.writeTimeout)
	defer cancel()

	acc, err := s.getUnsafe(ctx, address)
	if err != nil {
		return nil, err
	}
	if acc != nil {
		return acc, nil
	}

	// Create new account
	acc = account.NewAccount(address)
	if err := s.saveUnsafe(ctx, acc); err != nil {
		return nil, err
	}

	return acc, nil
}

// getUnsafe retrieves an account without locking (caller must hold lock).
func (s *Store) getUnsafe(ctx context.Context, address string) (*account.Account, error) {
	key := s.makeKey(address)
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("failed to get account: %w", err)
	}
	if data == nil {
		return nil, nil
	}

	var acc account.Account
	if err := json.Unmarshal(data, &acc); err != nil {
		return nil, fmt.Errorf("failed to deserialize account: %w", err)
	}

	return &acc, nil
}

// saveUnsafe saves an account without locking (caller must hold lock).
func (s *Store) saveUnsafe(ctx context.Context, acc *account.Account) error {
	data, err := json.Marshal(acc)
	if err != nil {
		return fmt.Errorf("failed to serialize account: %w", err)
	}

	key := s.makeKey(acc.Address)
	if err := s.client.Put(ctx, key, data); err != nil {
		return fmt.Errorf("failed to save account: %w", err)
	}

	return nil
}

// Ensure Store implements account.Store.
var _ account.Store = (*Store)(nil)
