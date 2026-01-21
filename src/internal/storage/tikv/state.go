package tikv

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"btc-gateway/pkg/consensus/types"
)

// Application State Operations
// These methods handle the map[string]int state for the KV store state machine.

// GetValue retrieves a value from the application state.
func (s *Storage) GetValue(ctx context.Context, key string) (int, bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return 0, false, ErrNotConnected
	}

	stateKey := s.makeKey(prefixAppState, key)
	data, err := s.client.Get(ctx, stateKey)
	if err != nil {
		return 0, false, fmt.Errorf("failed to get value: %w", err)
	}
	if data == nil {
		return 0, false, nil
	}

	if len(data) < 8 {
		return 0, false, fmt.Errorf("%w: invalid value data length", ErrDeserializationFailed)
	}

	value := int64(binary.BigEndian.Uint64(data))
	return int(value), true, nil
}

// SetValue sets a value in the application state.
func (s *Storage) SetValue(ctx context.Context, key string, value int) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return ErrNotConnected
	}

	stateKey := s.makeKey(prefixAppState, key)
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, uint64(int64(value)))

	if err := s.client.Put(ctx, stateKey, data); err != nil {
		return fmt.Errorf("failed to set value: %w", err)
	}

	return nil
}

// DeleteValue deletes a value from the application state.
func (s *Storage) DeleteValue(ctx context.Context, key string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return ErrNotConnected
	}

	stateKey := s.makeKey(prefixAppState, key)
	if err := s.client.Delete(ctx, stateKey); err != nil {
		return fmt.Errorf("failed to delete value: %w", err)
	}

	return nil
}

// GetSnapshot returns a snapshot of the entire application state.
func (s *Storage) GetSnapshot(ctx context.Context) (map[string]int, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return nil, ErrNotConnected
	}

	// Scan all keys with the app state prefix
	startKey := s.makeKey(prefixAppState)
	endKey := append(s.makeKey(prefixAppState), 0xFF) // End of prefix range

	keys, values, err := s.client.Scan(ctx, startKey, endKey, 10000)
	if err != nil {
		return nil, fmt.Errorf("failed to scan state: %w", err)
	}

	result := make(map[string]int)
	prefix := s.config.KeyPrefix + prefixAppState
	for i, key := range keys {
		// Extract the actual key from the full key
		keyStr := string(key)
		if !strings.HasPrefix(keyStr, prefix) {
			continue
		}
		actualKey := keyStr[len(prefix):]

		if len(values[i]) < 8 {
			continue
		}
		value := int64(binary.BigEndian.Uint64(values[i]))
		result[actualKey] = int(value)
	}

	return result, nil
}

// GetLastAppliedBlock returns the hash of the last applied block.
func (s *Storage) GetLastAppliedBlock(ctx context.Context) (types.BlockHash, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return types.BlockHash{}, ErrNotConnected
	}

	key := s.makeKey(prefixLastApplied)
	data, err := s.client.Get(ctx, key)
	if err != nil {
		return types.BlockHash{}, fmt.Errorf("failed to get last applied block: %w", err)
	}
	if data == nil {
		return types.BlockHash{}, nil // No block applied yet
	}

	var hash types.BlockHash
	if len(data) != len(hash) {
		return types.BlockHash{}, fmt.Errorf("%w: invalid hash data length", ErrDeserializationFailed)
	}
	copy(hash[:], data)

	return hash, nil
}

// SetLastAppliedBlock sets the hash of the last applied block.
func (s *Storage) SetLastAppliedBlock(ctx context.Context, hash types.BlockHash) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.connected {
		return ErrNotConnected
	}

	key := s.makeKey(prefixLastApplied)
	if err := s.client.Put(ctx, key, hash[:]); err != nil {
		return fmt.Errorf("failed to set last applied block: %w", err)
	}

	return nil
}
