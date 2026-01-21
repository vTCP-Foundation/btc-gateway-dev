package service

import (
	"context"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"btc-gateway/internal/mempool"
	"btc-gateway/internal/statemachine"
	"btc-gateway/internal/validation"
)

// TxSource indicates where a transaction originated from.
type TxSource string

const (
	// TxSourceREST indicates the transaction came from the REST API.
	TxSourceREST TxSource = "rest"
	// TxSourceGossip indicates the transaction came from gossip propagation.
	TxSourceGossip TxSource = "gossip"
	// TxSourceInternal indicates the transaction was generated internally.
	TxSourceInternal TxSource = "internal"
)

// TxBroadcaster defines the interface for broadcasting transactions to the network.
type TxBroadcaster interface {
	Broadcast(tx *statemachine.Transaction) error
}

// TxSubmitService handles transaction submission and validation.
type TxSubmitService struct {
	validator   *validation.TxValidator
	pool        mempool.Pool
	broadcaster TxBroadcaster
	logger      zerolog.Logger
}

// NewTxSubmitService creates a new transaction submission service.
func NewTxSubmitService(validator *validation.TxValidator, pool mempool.Pool) *TxSubmitService {
	return &TxSubmitService{
		validator: validator,
		pool:      pool,
		logger:    log.With().Str("component", "tx_submit").Logger(),
	}
}

// SetBroadcaster sets the broadcaster for transaction propagation.
func (s *TxSubmitService) SetBroadcaster(broadcaster TxBroadcaster) {
	s.broadcaster = broadcaster
}

// Submit validates and adds a transaction to the mempool.
// If the transaction came from REST API and a broadcaster is configured,
// it will also broadcast the transaction to the network.
func (s *TxSubmitService) Submit(ctx context.Context, tx *statemachine.Transaction, source TxSource) error {
	// Validate the transaction
	if err := s.validator.Validate(ctx, tx); err != nil {
		s.logger.Debug().
			Err(err).
			Str("source", string(source)).
			Str("sender", tx.Sender).
			Uint64("nonce", tx.Nonce).
			Msg("Transaction validation failed")
		return err
	}

	// Add to mempool
	if err := s.pool.Add(tx); err != nil {
		s.logger.Error().
			Err(err).
			Str("sender", tx.Sender).
			Uint64("nonce", tx.Nonce).
			Msg("Failed to add transaction to pool")
		return err
	}

	s.logger.Info().
		Str("source", string(source)).
		Str("sender", tx.Sender).
		Uint64("nonce", tx.Nonce).
		Str("type", tx.Type.String()).
		Msg("Transaction added to mempool")

	// Broadcast to network if from REST API and broadcaster is configured
	if source == TxSourceREST && s.broadcaster != nil {
		if err := s.broadcaster.Broadcast(tx); err != nil {
			// Log error but don't fail the submission - tx is already in local mempool
			s.logger.Warn().
				Err(err).
				Str("sender", tx.Sender).
				Uint64("nonce", tx.Nonce).
				Msg("Failed to broadcast transaction")
		}
	}

	return nil
}

// SubmitWithoutValidation adds a transaction to the mempool without validation.
// This is used for transactions that have already been validated (e.g., from block execution).
func (s *TxSubmitService) SubmitWithoutValidation(tx *statemachine.Transaction) error {
	return s.pool.Add(tx)
}
