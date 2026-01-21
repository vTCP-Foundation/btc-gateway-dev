package gossip

import (
	"context"
	"sync"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"

	"btc-gateway/internal/service"
	"btc-gateway/internal/statemachine"
)

const (
	// SeenCacheSize is the size of the LRU cache for seen transaction hashes.
	SeenCacheSize = 1000
)

// Handler processes incoming gossip messages and submits valid transactions to the mempool.
type Handler struct {
	submitService *service.TxSubmitService
	subscription  *pubsub.Subscription
	seenCache     *lru.Cache[[32]byte, struct{}]
	logger        zerolog.Logger

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewHandler creates a new gossip handler.
func NewHandler(ctx context.Context, submitService *service.TxSubmitService, topic *pubsub.Topic) (*Handler, error) {
	// Subscribe to the topic
	sub, err := topic.Subscribe()
	if err != nil {
		return nil, err
	}

	// Create LRU cache for seen transactions
	cache, err := lru.New[[32]byte, struct{}](SeenCacheSize)
	if err != nil {
		sub.Cancel()
		return nil, err
	}

	ctx, cancel := context.WithCancel(ctx)

	return &Handler{
		submitService: submitService,
		subscription:  sub,
		seenCache:     cache,
		logger:        log.With().Str("component", "gossip_handler").Logger(),
		ctx:           ctx,
		cancel:        cancel,
	}, nil
}

// Start begins processing incoming gossip messages.
func (h *Handler) Start() {
	h.wg.Add(1)
	go h.processMessages()
}

// Stop gracefully shuts down the handler.
func (h *Handler) Stop() {
	h.cancel()
	h.subscription.Cancel()
	h.wg.Wait()
}

// processMessages reads and processes messages from the subscription.
func (h *Handler) processMessages() {
	defer h.wg.Done()

	for {
		msg, err := h.subscription.Next(h.ctx)
		if err != nil {
			if h.ctx.Err() != nil {
				// Context cancelled, shutting down
				return
			}
			h.logger.Debug().Err(err).Msg("Error reading gossip message")
			continue
		}

		// Note: LRU cache handles deduplication - no need to skip based on peer IDs
		h.handleMessage(msg.Data)
	}
}

// handleMessage processes a single gossip message.
func (h *Handler) handleMessage(data []byte) {
	// Deserialize transaction
	var tx statemachine.Transaction
	if err := msgpack.Unmarshal(data, &tx); err != nil {
		h.logger.Debug().Err(err).Msg("Failed to deserialize gossip message")
		return
	}

	// Check if we've already seen this transaction
	txHash := tx.Hash()
	if h.seenCache.Contains(txHash) {
		return
	}

	// Mark as seen
	h.seenCache.Add(txHash, struct{}{})

	// Submit to mempool via TxSubmitService
	if err := h.submitService.Submit(h.ctx, &tx, service.TxSourceGossip); err != nil {
		// Log at debug level - peers may send invalid transactions
		h.logger.Debug().
			Err(err).
			Str("sender", tx.Sender).
			Uint64("nonce", tx.Nonce).
			Msg("Failed to submit gossiped transaction")
		return
	}

	h.logger.Debug().
		Str("sender", tx.Sender).
		Uint64("nonce", tx.Nonce).
		Str("type", tx.Type.String()).
		Msg("Gossiped transaction added to mempool")
}
