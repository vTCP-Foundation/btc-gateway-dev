package builder

import (
	"context"
	"sort"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"btc-gateway/internal/account"
	"btc-gateway/internal/mempool"
	"btc-gateway/internal/statemachine"
)

// BlockBuildService builds and proposes blocks from the mempool.
type BlockBuildService struct {
	pool         mempool.Pool
	proposer     ConsensusProposer
	accountStore account.Store
	logger       zerolog.Logger
}

// NewBlockBuildService creates a new block build service.
func NewBlockBuildService(pool mempool.Pool, proposer ConsensusProposer, accountStore account.Store) *BlockBuildService {
	return &BlockBuildService{
		pool:         pool,
		proposer:     proposer,
		accountStore: accountStore,
		logger:       log.With().Str("component", "block_builder").Logger(),
	}
}

// BuildAndPropose peeks transactions from the mempool, orders them for valid application,
// and proposes them as a block. Transactions are only removed after successful commit.
// Returns the number of transactions included in the block.
func (s *BlockBuildService) BuildAndPropose() (int, error) {
	ctx := context.Background()

	// Peek transactions from pool (don't remove yet)
	// Reserve space for batch serialization overhead
	effectiveLimit := MaxBlockSizeBytes - BatchOverheadBytes
	txs := s.pool.Peek(effectiveLimit)

	// Order transactions for valid application:
	// - Group by sender
	// - Sort each group by nonce ascending
	// - Only include TXs that form valid sequences starting from account nonce
	orderedTxs := s.orderTransactions(ctx, txs)

	// Create transaction batch (even if empty for liveness)
	batch := statemachine.NewTransactionBatch(orderedTxs...)

	s.logger.Info().
		Int("tx_count", len(orderedTxs)).
		Int("peeked_count", len(txs)).
		Int("pool_remaining", s.pool.Size()).
		Msg("Building block")

	// Propose the block
	if err := s.proposer.ProposeBlock(batch); err != nil {
		// Transactions stay in pool since we used Peek()
		s.logger.Error().
			Err(err).
			Int("tx_count", len(orderedTxs)).
			Msg("Failed to propose block")
		return 0, err
	}

	s.logger.Info().
		Int("tx_count", len(orderedTxs)).
		Msg("Block proposed successfully")

	return len(orderedTxs), nil
}

// orderTransactions orders transactions for valid block application.
// It groups TXs by sender, sorts by nonce, and only includes TXs that form
// valid sequences starting from each sender's current account nonce.
func (s *BlockBuildService) orderTransactions(ctx context.Context, txs []*statemachine.Transaction) []*statemachine.Transaction {
	if len(txs) == 0 {
		return txs
	}

	// Group transactions by sender
	bySender := make(map[string][]*statemachine.Transaction)
	var noSenderTxs []*statemachine.Transaction

	for _, tx := range txs {
		if tx.Sender == "" {
			noSenderTxs = append(noSenderTxs, tx)
		} else {
			bySender[tx.Sender] = append(bySender[tx.Sender], tx)
		}
	}

	// Process each sender's transactions
	var result []*statemachine.Transaction

	for sender, senderTxs := range bySender {
		// Sort by nonce ascending
		sort.Slice(senderTxs, func(i, j int) bool {
			return senderTxs[i].Nonce < senderTxs[j].Nonce
		})

		// Get current account nonce
		var expectedNonce uint64
		if s.accountStore != nil {
			acc, err := s.accountStore.GetOrCreate(ctx, sender)
			if err != nil {
				s.logger.Warn().
					Err(err).
					Str("sender", sender).
					Msg("Failed to get account nonce, skipping sender's transactions")
				continue
			}
			expectedNonce = acc.Nonce
		}

		// Select TXs that form a valid sequence starting from expected nonce
		for _, tx := range senderTxs {
			if tx.Nonce == expectedNonce {
				result = append(result, tx)
				expectedNonce++
			} else if tx.Nonce > expectedNonce {
				// Gap in nonce - stop including this sender's transactions
				// The rest will stay in the pool for future blocks
				break
			}
			// tx.Nonce < expectedNonce means it's already been applied, skip it
		}
	}

	// Add transactions without sender (they don't have nonce requirements)
	result = append(result, noSenderTxs...)

	return result
}
