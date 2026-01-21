// Package service provides application services that orchestrate business logic.
package service

import (
	"context"
	"errors"

	"btc-gateway/internal/account"
	"btc-gateway/internal/crypto"
)

// Account query errors
var (
	// ErrInvalidAddress indicates the address format is invalid.
	ErrInvalidAddress = errors.New("invalid address format")
)

// AccountQueryService provides account query operations.
type AccountQueryService struct {
	store          account.Store
	cryptoProvider crypto.CryptoProvider
}

// NewAccountQueryService creates a new account query service.
func NewAccountQueryService(store account.Store, cryptoProvider crypto.CryptoProvider) *AccountQueryService {
	return &AccountQueryService{
		store:          store,
		cryptoProvider: cryptoProvider,
	}
}

// GetAccount retrieves an account by address.
// Returns ErrInvalidAddress if the address format is invalid.
// Returns a zero-account (nonce=0, balance=0) if the account doesn't exist.
func (s *AccountQueryService) GetAccount(ctx context.Context, address string) (*account.Account, error) {
	// Validate address format
	if !s.cryptoProvider.ValidateAddress(address) {
		return nil, ErrInvalidAddress
	}

	acc, err := s.store.Get(ctx, address)
	if err != nil {
		return nil, err
	}
	if acc == nil {
		// Return a zero account for non-existent addresses
		return account.NewAccount(address), nil
	}
	return acc, nil
}
