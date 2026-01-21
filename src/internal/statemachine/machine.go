package statemachine

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"btc-gateway/internal/account"
	"btc-gateway/internal/storage/tikv"
	"btc-gateway/pkg/consensus/types"
)

// StateMachine implements a simple key-value store backed by TikV.
// All state operations go directly to TikV - no in-memory caching.
type StateMachine struct {
	storage      *tikv.Storage
	accountStore account.Store
	mu           sync.RWMutex
	logger       zerolog.Logger
}

// NewStateMachine creates a new state machine backed by TikV storage.
func NewStateMachine(storage *tikv.Storage) *StateMachine {
	return &StateMachine{
		storage: storage,
		logger:  log.With().Str("component", "statemachine").Logger(),
	}
}

// SetAccountStore sets the account store for the state machine.
func (sm *StateMachine) SetAccountStore(store account.Store) {
	sm.accountStore = store
}

// ApplyBlock executes all transactions in a block and writes to TikV.
// Returns an error if any transaction fails to apply.
func (sm *StateMachine) ApplyBlock(ctx context.Context, block *types.Block) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Skip genesis block (no transactions)
	if block.IsGenesis() || len(block.Payload) == 0 {
		sm.logger.Debug().
			Str("block_hash", fmt.Sprintf("%x", block.Hash[:8])).
			Msg("Skipping empty/genesis block")
		return nil
	}

	// Check if block was already applied
	lastApplied, err := sm.storage.GetLastAppliedBlock(ctx)
	if err != nil && err != tikv.ErrNotFound {
		return fmt.Errorf("failed to get last applied block: %w", err)
	}
	if lastApplied == block.Hash {
		sm.logger.Debug().
			Str("block_hash", fmt.Sprintf("%x", block.Hash[:8])).
			Msg("Block already applied, skipping")
		return nil
	}

	// Deserialize transactions from block payload
	batch, err := DeserializeTransactionBatch(block.Payload)
	if err != nil {
		return fmt.Errorf("failed to deserialize transactions: %w", err)
	}

	// Apply each transaction
	for i, tx := range batch.Transactions {
		if err := sm.applyTransaction(ctx, tx); err != nil {
			return fmt.Errorf("failed to apply transaction %d: %w", i, err)
		}
	}

	// Mark block as applied
	if err := sm.storage.SetLastAppliedBlock(ctx, block.Hash); err != nil {
		return fmt.Errorf("failed to mark block as applied: %w", err)
	}

	sm.logger.Info().
		Str("block_hash", fmt.Sprintf("%x", block.Hash[:8])).
		Uint64("height", uint64(block.Height)).
		Int("tx_count", len(batch.Transactions)).
		Msg("Applied block")

	return nil
}

// applyTransaction applies a single transaction to TikV.
func (sm *StateMachine) applyTransaction(ctx context.Context, tx *Transaction) error {
	// Validate transaction
	if err := tx.Validate(); err != nil {
		return err
	}

	// Nonce validation for transactions with sender - uses TiKV account nonce
	if tx.Sender != "" && sm.accountStore != nil {
		acc, err := sm.accountStore.GetOrCreate(ctx, tx.Sender)
		if err != nil {
			return fmt.Errorf("failed to get account: %w", err)
		}
		// Strict nonce match required
		if tx.Nonce != acc.Nonce {
			sm.logger.Warn().
				Str("sender", tx.Sender).
				Uint64("expected", acc.Nonce).
				Uint64("got", tx.Nonce).
				Msg("Nonce mismatch, skipping transaction")
			return nil // Skip invalid TX, don't fail entire block
		}
	}

	// Execute based on type
	switch tx.Type {
	case TxSet:
		if err := sm.storage.SetValue(ctx, tx.Key, tx.Value); err != nil {
			return fmt.Errorf("SET failed: %w", err)
		}
		sm.logger.Debug().
			Str("key", tx.Key).
			Int("value", tx.Value).
			Msg("SET executed")

	case TxAdd:
		current, exists, err := sm.storage.GetValue(ctx, tx.Key)
		if err != nil {
			return fmt.Errorf("ADD read failed: %w", err)
		}
		if !exists {
			current = 0
		}
		newValue := current + tx.Value
		if err := sm.storage.SetValue(ctx, tx.Key, newValue); err != nil {
			return fmt.Errorf("ADD write failed: %w", err)
		}
		sm.logger.Debug().
			Str("key", tx.Key).
			Int("delta", tx.Value).
			Int("new_value", newValue).
			Msg("ADD executed")

	case TxSub:
		current, exists, err := sm.storage.GetValue(ctx, tx.Key)
		if err != nil {
			return fmt.Errorf("SUB read failed: %w", err)
		}
		if !exists {
			current = 0
		}
		newValue := current - tx.Value
		if err := sm.storage.SetValue(ctx, tx.Key, newValue); err != nil {
			return fmt.Errorf("SUB write failed: %w", err)
		}
		sm.logger.Debug().
			Str("key", tx.Key).
			Int("delta", tx.Value).
			Int("new_value", newValue).
			Msg("SUB executed")

	case TxDel:
		if err := sm.storage.DeleteValue(ctx, tx.Key); err != nil {
			return fmt.Errorf("DEL failed: %w", err)
		}
		sm.logger.Debug().
			Str("key", tx.Key).
			Msg("DEL executed")

	case TxSetBalance:
		if sm.accountStore == nil {
			return fmt.Errorf("SET_BALANCE failed: account store not configured")
		}
		if err := sm.accountStore.SetBalance(ctx, tx.Key, uint64(tx.Value)); err != nil {
			return fmt.Errorf("SET_BALANCE failed: %w", err)
		}
		sm.logger.Debug().
			Str("address", tx.Key).
			Int("balance", tx.Value).
			Msg("SET_BALANCE executed")

	default:
		return fmt.Errorf("unknown transaction type: %d", tx.Type)
	}

	// Increment sender's nonce after successful execution
	if tx.Sender != "" && sm.accountStore != nil {
		if err := sm.accountStore.IncrementNonce(ctx, tx.Sender); err != nil {
			sm.logger.Warn().Err(err).Str("sender", tx.Sender).Msg("Failed to increment sender nonce")
		}
	}

	return nil
}

// Get retrieves a value from TikV.
func (sm *StateMachine) Get(ctx context.Context, key string) (int, bool, error) {
	return sm.storage.GetValue(ctx, key)
}

// GetSnapshot returns a snapshot of all key-value pairs.
func (sm *StateMachine) GetSnapshot(ctx context.Context) (map[string]int, error) {
	return sm.storage.GetSnapshot(ctx)
}

// GetLastAppliedBlock returns the hash of the last applied block.
func (sm *StateMachine) GetLastAppliedBlock(ctx context.Context) (types.BlockHash, error) {
	return sm.storage.GetLastAppliedBlock(ctx)
}
