// Package account provides account management functionality for the mempool system.
package account

// Account represents a user account with address, nonce, and balance.
type Account struct {
	Address string `json:"address"`
	Nonce   uint64 `json:"nonce"`
	Balance uint64 `json:"balance"`
}

// NewAccount creates a new account with the given address.
// The account starts with nonce=0 and balance=0.
func NewAccount(address string) *Account {
	return &Account{
		Address: address,
		Nonce:   0,
		Balance: 0,
	}
}

// IncrementNonce increments the account's nonce by 1.
func (a *Account) IncrementNonce() {
	a.Nonce++
}

// SetBalance sets the account's balance to the given amount.
func (a *Account) SetBalance(amount uint64) {
	a.Balance = amount
}
