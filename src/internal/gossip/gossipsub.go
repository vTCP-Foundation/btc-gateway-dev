package gossip

import (
	"context"
	"fmt"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/vmihailenco/msgpack/v5"

	"btc-gateway/internal/statemachine"
)

const (
	// TxTopic is the GossipSub topic for transaction propagation.
	TxTopic = "/btc-gateway/tx/1.0.0"
)

// GossipSubBroadcaster implements TxBroadcaster using libp2p GossipSub.
type GossipSubBroadcaster struct {
	host   host.Host
	pubsub *pubsub.PubSub
	topic  *pubsub.Topic
	logger zerolog.Logger
	ctx    context.Context
	cancel context.CancelFunc
}

// NewGossipSubBroadcaster creates a new GossipSub-based transaction broadcaster.
func NewGossipSubBroadcaster(ctx context.Context, h host.Host) (*GossipSubBroadcaster, error) {
	// Create GossipSub pubsub
	ps, err := pubsub.NewGossipSub(ctx, h)
	if err != nil {
		return nil, fmt.Errorf("failed to create gossipsub: %w", err)
	}

	// Join the transaction topic
	topic, err := ps.Join(TxTopic)
	if err != nil {
		return nil, fmt.Errorf("failed to join topic %s: %w", TxTopic, err)
	}

	ctx, cancel := context.WithCancel(ctx)

	return &GossipSubBroadcaster{
		host:   h,
		pubsub: ps,
		topic:  topic,
		logger: log.With().Str("component", "gossip_broadcaster").Logger(),
		ctx:    ctx,
		cancel: cancel,
	}, nil
}

// Broadcast sends a transaction to the gossip network.
func (b *GossipSubBroadcaster) Broadcast(tx *statemachine.Transaction) error {
	// Serialize transaction with msgpack (compact)
	data, err := msgpack.Marshal(tx)
	if err != nil {
		return fmt.Errorf("failed to serialize transaction: %w", err)
	}

	// Publish to topic
	if err := b.topic.Publish(b.ctx, data); err != nil {
		return fmt.Errorf("failed to publish transaction: %w", err)
	}

	b.logger.Debug().
		Str("sender", tx.Sender).
		Uint64("nonce", tx.Nonce).
		Str("type", tx.Type.String()).
		Msg("Transaction broadcasted")

	return nil
}

// GetTopic returns the pubsub topic for subscription.
func (b *GossipSubBroadcaster) GetTopic() *pubsub.Topic {
	return b.topic
}

// GetPubSub returns the pubsub instance.
func (b *GossipSubBroadcaster) GetPubSub() *pubsub.PubSub {
	return b.pubsub
}

// Close shuts down the broadcaster and releases resources.
func (b *GossipSubBroadcaster) Close() error {
	b.cancel()
	return b.topic.Close()
}

// Ensure GossipSubBroadcaster implements TxBroadcaster.
var _ TxBroadcaster = (*GossipSubBroadcaster)(nil)
