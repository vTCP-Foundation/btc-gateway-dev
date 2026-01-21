package validation

import (
	"context"

	"btc-gateway/internal/account"
	"btc-gateway/internal/crypto"
	"btc-gateway/internal/mempool"
	"btc-gateway/internal/statemachine"
)

// TxValidator validates transactions before they enter the mempool.
type TxValidator struct {
	crypto         crypto.CryptoProvider
	accountStore   account.Store
	committedStore CommittedTxStore
	pool           mempool.Pool
}

// NewTxValidator creates a new transaction validator.
func NewTxValidator(
	crypto crypto.CryptoProvider,
	accountStore account.Store,
	committedStore CommittedTxStore,
	pool mempool.Pool,
) *TxValidator {
	return &TxValidator{
		crypto:         crypto,
		accountStore:   accountStore,
		committedStore: committedStore,
		pool:           pool,
	}
}

// Validate performs full validation on a transaction.
// Validation order:
// 0. Basic sanity checks (empty key, invalid type, negative value)
// 1. Check signature is present and valid
// 2. Check sender address matches derived from pubkey
// 3. Check not duplicate in mempool
// 4. Check not already committed (replay)
// 5. Check for duplicate (sender, nonce) in mempool - gaps are allowed
func (v *TxValidator) Validate(ctx context.Context, tx *statemachine.Transaction) error {
	// 0. Basic sanity checks
	if err := tx.Validate(); err != nil {
		return err
	}

	// 1. Check signature is present
	if len(tx.Signature) == 0 {
		return ErrMissingSignature
	}
	if tx.Sender == "" || len(tx.SenderPubKey) == 0 {
		return ErrMissingSender
	}

	// 2. Verify signature
	txHash := tx.Hash()
	valid, err := v.crypto.Verify(tx.SenderPubKey, txHash[:], tx.Signature)
	if err != nil {
		return ErrInvalidSignature
	}
	if !valid {
		return ErrInvalidSignature
	}

	// 3. Check sender address matches derived from pubkey
	derivedAddr := v.crypto.DeriveAddress(tx.SenderPubKey)
	if derivedAddr != tx.Sender {
		return ErrInvalidSender
	}

	// 4. Check not duplicate in mempool
	if v.pool.Contains(txHash) {
		return ErrDuplicateInMempool
	}

	// 5. Check not already committed
	committed, err := v.committedStore.Contains(ctx, txHash)
	if err != nil {
		return err
	}
	if committed {
		return ErrDuplicateCommitted
	}

	// 6. Check for duplicate (sender, nonce) in mempool - gaps are allowed
	if v.pool.ContainsSenderNonce(tx.Sender, tx.Nonce) {
		return ErrDuplicateNonce
	}

	return nil
}

// ValidateBasic performs basic validation without checking mempool or committed state.
// Useful for re-validating transactions that are already in the mempool.
func (v *TxValidator) ValidateBasic(ctx context.Context, tx *statemachine.Transaction) error {
	// Check signature is present
	if len(tx.Signature) == 0 {
		return ErrMissingSignature
	}
	if tx.Sender == "" || len(tx.SenderPubKey) == 0 {
		return ErrMissingSender
	}

	// Verify signature
	txHash := tx.Hash()
	valid, err := v.crypto.Verify(tx.SenderPubKey, txHash[:], tx.Signature)
	if err != nil {
		return ErrInvalidSignature
	}
	if !valid {
		return ErrInvalidSignature
	}

	// Check sender address matches derived from pubkey
	derivedAddr := v.crypto.DeriveAddress(tx.SenderPubKey)
	if derivedAddr != tx.Sender {
		return ErrInvalidSender
	}

	return nil
}
