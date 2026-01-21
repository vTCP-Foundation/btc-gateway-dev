// Package service provides application services that orchestrate business logic.
package service

import (
	"context"

	"btc-gateway/internal/account"
)

// AccountQueryService provides account query operations.
type AccountQueryService struct {
	store account.Store
}

// NewAccountQueryService creates a new account query service.
func NewAccountQueryService(store account.Store) *AccountQueryService {
	return &AccountQueryService{
		store: store,
	}
}

// GetAccount retrieves an account by address.
// Returns a zero-account (nonce=0, balance=0) if the account doesn't exist.
func (s *AccountQueryService) GetAccount(ctx context.Context, address string) (*account.Account, error) {
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
