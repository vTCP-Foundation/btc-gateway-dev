// Package statemachine implements a key-value store state machine for HotStuff consensus.
package statemachine

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// TransactionType represents the type of KV store operation.
type TransactionType uint8

const (
	// TxSet sets a key to a specific value
	TxSet TransactionType = iota
	// TxAdd adds a value to an existing key (key += value)
	TxAdd
	// TxSub subtracts a value from an existing key (key -= value)
	TxSub
	// TxDel deletes a key
	TxDel
	// TxSetBalance sets an account balance (used for account management)
	TxSetBalance
)

// String returns a string representation of the transaction type.
func (t TransactionType) String() string {
	switch t {
	case TxSet:
		return "SET"
	case TxAdd:
		return "ADD"
	case TxSub:
		return "SUB"
	case TxDel:
		return "DEL"
	case TxSetBalance:
		return "SET_BALANCE"
	default:
		return "UNKNOWN"
	}
}

// Transaction represents a KV store transaction.
type Transaction struct {
	Type         TransactionType `json:"type"`
	Key          string          `json:"key"`
	Value        int             `json:"value"`
	Nonce        uint64          `json:"nonce"`         // Replay protection
	Sender       string          `json:"sender"`        // Sender address
	SenderPubKey []byte          `json:"sender_pubkey"` // Sender's public key
	Signature    []byte          `json:"signature"`     // Transaction signature
}

// Hash computes a SHA-256 hash of the transaction, excluding the signature.
// This is used for signing and verification.
func (tx *Transaction) Hash() [32]byte {
	// Create a copy without signature for hashing
	hashTx := &Transaction{
		Type:         tx.Type,
		Key:          tx.Key,
		Value:        tx.Value,
		Nonce:        tx.Nonce,
		Sender:       tx.Sender,
		SenderPubKey: tx.SenderPubKey,
		// Signature is intentionally excluded
	}
	data, _ := json.Marshal(hashTx)
	return sha256.Sum256(data)
}

// NewSetTransaction creates a new SET transaction.
func NewSetTransaction(key string, value int, nonce uint64) *Transaction {
	return &Transaction{
		Type:  TxSet,
		Key:   key,
		Value: value,
		Nonce: nonce,
	}
}

// NewSetTransactionWithSender creates a new SET transaction with sender information.
func NewSetTransactionWithSender(key string, value int, nonce uint64, sender string, pubKey []byte) *Transaction {
	return &Transaction{
		Type:         TxSet,
		Key:          key,
		Value:        value,
		Nonce:        nonce,
		Sender:       sender,
		SenderPubKey: pubKey,
	}
}

// NewAddTransaction creates a new ADD transaction.
func NewAddTransaction(key string, value int, nonce uint64) *Transaction {
	return &Transaction{
		Type:  TxAdd,
		Key:   key,
		Value: value,
		Nonce: nonce,
	}
}

// NewAddTransactionWithSender creates a new ADD transaction with sender information.
func NewAddTransactionWithSender(key string, value int, nonce uint64, sender string, pubKey []byte) *Transaction {
	return &Transaction{
		Type:         TxAdd,
		Key:          key,
		Value:        value,
		Nonce:        nonce,
		Sender:       sender,
		SenderPubKey: pubKey,
	}
}

// NewSubTransaction creates a new SUB transaction.
func NewSubTransaction(key string, value int, nonce uint64) *Transaction {
	return &Transaction{
		Type:  TxSub,
		Key:   key,
		Value: value,
		Nonce: nonce,
	}
}

// NewSubTransactionWithSender creates a new SUB transaction with sender information.
func NewSubTransactionWithSender(key string, value int, nonce uint64, sender string, pubKey []byte) *Transaction {
	return &Transaction{
		Type:         TxSub,
		Key:          key,
		Value:        value,
		Nonce:        nonce,
		Sender:       sender,
		SenderPubKey: pubKey,
	}
}

// NewDelTransaction creates a new DEL transaction.
func NewDelTransaction(key string, nonce uint64) *Transaction {
	return &Transaction{
		Type:  TxDel,
		Key:   key,
		Nonce: nonce,
	}
}

// NewDelTransactionWithSender creates a new DEL transaction with sender information.
func NewDelTransactionWithSender(key string, nonce uint64, sender string, pubKey []byte) *Transaction {
	return &Transaction{
		Type:         TxDel,
		Key:          key,
		Nonce:        nonce,
		Sender:       sender,
		SenderPubKey: pubKey,
	}
}

// NewSetBalanceTransaction creates a new SET_BALANCE transaction for account management.
func NewSetBalanceTransaction(address string, amount int, nonce uint64) *Transaction {
	return &Transaction{
		Type:  TxSetBalance,
		Key:   address,
		Value: amount,
		Nonce: nonce,
	}
}

// NewSetBalanceTransactionWithSender creates a new SET_BALANCE transaction with sender information.
func NewSetBalanceTransactionWithSender(address string, amount int, nonce uint64, sender string, pubKey []byte) *Transaction {
	return &Transaction{
		Type:         TxSetBalance,
		Key:          address,
		Value:        amount,
		Nonce:        nonce,
		Sender:       sender,
		SenderPubKey: pubKey,
	}
}

// Validate performs basic validation on the transaction.
func (tx *Transaction) Validate() error {
	if tx.Key == "" {
		return fmt.Errorf("transaction key cannot be empty")
	}
	if tx.Type > TxSetBalance {
		return fmt.Errorf("invalid transaction type: %d", tx.Type)
	}
	// SET_BALANCE value cannot be negative (would cause uint64 underflow)
	if tx.Type == TxSetBalance && tx.Value < 0 {
		return fmt.Errorf("SET_BALANCE value cannot be negative: %d", tx.Value)
	}
	return nil
}

// Serialize serializes the transaction to JSON bytes.
func (tx *Transaction) Serialize() ([]byte, error) {
	return json.Marshal(tx)
}

// DeserializeTransaction deserializes a transaction from JSON bytes.
func DeserializeTransaction(data []byte) (*Transaction, error) {
	var tx Transaction
	if err := json.Unmarshal(data, &tx); err != nil {
		return nil, fmt.Errorf("failed to deserialize transaction: %w", err)
	}
	return &tx, nil
}

// TransactionBatch represents multiple transactions to be included in a block.
type TransactionBatch struct {
	Transactions []*Transaction `json:"transactions"`
}

// NewTransactionBatch creates a new transaction batch.
func NewTransactionBatch(txs ...*Transaction) *TransactionBatch {
	return &TransactionBatch{
		Transactions: txs,
	}
}

// Serialize serializes the batch to JSON bytes.
func (batch *TransactionBatch) Serialize() ([]byte, error) {
	return json.Marshal(batch)
}

// DeserializeTransactionBatch deserializes a transaction batch from JSON bytes.
func DeserializeTransactionBatch(data []byte) (*TransactionBatch, error) {
	var batch TransactionBatch
	if err := json.Unmarshal(data, &batch); err != nil {
		return nil, fmt.Errorf("failed to deserialize transaction batch: %w", err)
	}
	return &batch, nil
}
