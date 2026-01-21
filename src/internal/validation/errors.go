package validation

import "errors"

var (
	// ErrNilTransaction indicates a nil transaction was provided.
	ErrNilTransaction = errors.New("transaction is nil")

	// ErrInvalidSignature indicates the transaction signature is invalid.
	ErrInvalidSignature = errors.New("invalid transaction signature")

	// ErrInvalidSender indicates the sender address doesn't match the public key.
	ErrInvalidSender = errors.New("sender address does not match public key")

	// ErrDuplicateInMempool indicates the transaction already exists in the mempool.
	ErrDuplicateInMempool = errors.New("transaction already exists in mempool")

	// ErrDuplicateCommitted indicates the transaction has already been committed.
	ErrDuplicateCommitted = errors.New("transaction has already been committed")

	// ErrInvalidNonce indicates the transaction nonce is invalid.
	ErrInvalidNonce = errors.New("invalid transaction nonce")

	// ErrDuplicateNonce indicates a transaction with the same sender and nonce already exists in the mempool.
	ErrDuplicateNonce = errors.New("duplicate nonce for sender in mempool")

	// ErrMissingSignature indicates the transaction is missing a signature.
	ErrMissingSignature = errors.New("transaction is missing signature")

	// ErrMissingSender indicates the transaction is missing sender information.
	ErrMissingSender = errors.New("transaction is missing sender information")
)
