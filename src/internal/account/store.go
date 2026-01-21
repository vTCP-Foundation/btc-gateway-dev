package account

import "context"

// Store defines the interface for account persistence operations.
type Store interface {
	// Get retrieves an account by address.
	// Returns (account, nil) if found, (nil, nil) if not found, or (nil, error) on failure.
	Get(ctx context.Context, address string) (*Account, error)

	// Save persists an account.
	Save(ctx context.Context, account *Account) error

	// IncrementNonce increments the nonce for the account at the given address.
	// Creates the account if it doesn't exist.
	IncrementNonce(ctx context.Context, address string) error

	// SetBalance sets the balance for the account at the given address.
	// Creates the account if it doesn't exist.
	SetBalance(ctx context.Context, address string, amount uint64) error

	// GetOrCreate retrieves an account by address, creating it if it doesn't exist.
	GetOrCreate(ctx context.Context, address string) (*Account, error)
}
