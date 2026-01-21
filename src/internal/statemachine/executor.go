package statemachine

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"btc-gateway/internal/storage/tikv"
	"btc-gateway/pkg/consensus/events"
	"btc-gateway/pkg/consensus/types"
)

// CommittedTxStore is a local interface to avoid import cycles.
// It mirrors validation.CommittedTxStore.
type CommittedTxStore interface {
	AddBatch(ctx context.Context, hashes [][32]byte) error
}

// TxPool is a local interface to avoid import cycles.
// It mirrors mempool.Pool.
type TxPool interface {
	Remove(hashes [][32]byte)
}

// LeadershipHandler handles leadership events for block building.
type LeadershipHandler interface {
	OnLeadershipGained(view types.ViewNumber)
}

// Executor listens for committed blocks from consensus and applies them to the state machine.
type Executor struct {
	storage           *tikv.Storage
	stateMachine      *StateMachine
	eventChan         chan events.ConsensusEvent
	logger            zerolog.Logger
	committedStore    CommittedTxStore
	pool              TxPool
	leadershipHandler LeadershipHandler

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewExecutor creates a new consensus executor.
func NewExecutor(storage *tikv.Storage) *Executor {
	ctx, cancel := context.WithCancel(context.Background())
	return &Executor{
		storage:      storage,
		stateMachine: NewStateMachine(storage),
		eventChan:    make(chan events.ConsensusEvent, 1000),
		logger:       log.With().Str("component", "executor").Logger(),
		ctx:          ctx,
		cancel:       cancel,
	}
}

// GetStateMachine returns the underlying state machine.
func (e *Executor) GetStateMachine() *StateMachine {
	return e.stateMachine
}

// SetCommittedStore sets the committed transaction store.
func (e *Executor) SetCommittedStore(store CommittedTxStore) {
	e.committedStore = store
}

// SetPool sets the mempool for transaction removal after block commit.
func (e *Executor) SetPool(pool TxPool) {
	e.pool = pool
}

// SetLeadershipHandler sets the handler for leadership events.
func (e *Executor) SetLeadershipHandler(handler LeadershipHandler) {
	e.leadershipHandler = handler
}

// GetEventChannel returns the channel for receiving consensus events.
// The consensus coordinator should send BlockCommitted events to this channel.
func (e *Executor) GetEventChannel() chan<- events.ConsensusEvent {
	return e.eventChan
}

// Start begins listening for committed blocks.
func (e *Executor) Start() error {
	e.logger.Info().Msg("Starting executor")

	e.wg.Add(1)
	go e.processEvents()

	return nil
}

// Stop gracefully shuts down the executor.
func (e *Executor) Stop() error {
	e.logger.Info().Msg("Stopping executor")
	e.cancel()
	close(e.eventChan)
	e.wg.Wait()
	return nil
}

// processEvents handles incoming consensus events.
func (e *Executor) processEvents() {
	defer e.wg.Done()

	for {
		select {
		case <-e.ctx.Done():
			e.logger.Info().Msg("Executor shutting down")
			return

		case event, ok := <-e.eventChan:
			if !ok {
				return
			}
			if err := e.handleEvent(event); err != nil {
				e.logger.Error().Err(err).Msg("Failed to handle event")
			}
		}
	}
}

// handleEvent processes a single consensus event.
func (e *Executor) handleEvent(event events.ConsensusEvent) error {
	// Handle LeaderElected events
	if event.EventType == events.EventLeaderElected {
		if e.leadershipHandler != nil {
			viewRaw, ok := event.Payload["view"]
			if ok {
				if view, ok := viewRaw.(types.ViewNumber); ok {
					e.leadershipHandler.OnLeadershipGained(view)
				}
			}
		}
		return nil
	}

	// Only process BlockCommitted events for state machine
	if event.EventType != events.EventBlockCommitted {
		return nil
	}

	// Extract block hash from payload
	blockHashRaw, ok := event.Payload["block_hash"]
	if !ok {
		return fmt.Errorf("block_hash not found in event payload")
	}

	blockHash, ok := blockHashRaw.(types.BlockHash)
	if !ok {
		return fmt.Errorf("invalid block_hash type in event payload")
	}

	// Retrieve the block from storage
	block, err := e.storage.GetBlock(blockHash)
	if err != nil {
		return fmt.Errorf("failed to retrieve committed block: %w", err)
	}

	// Apply the block to the state machine
	if err := e.stateMachine.ApplyBlock(e.ctx, block); err != nil {
		return fmt.Errorf("failed to apply block: %w", err)
	}

	// Extract transaction hashes and mark as committed, remove from pool
	if len(block.Payload) > 0 {
		batch, err := DeserializeTransactionBatch(block.Payload)
		if err == nil && len(batch.Transactions) > 0 {
			txHashes := make([][32]byte, len(batch.Transactions))
			for i, tx := range batch.Transactions {
				txHashes[i] = tx.Hash()
			}

			// Mark transactions as committed
			if e.committedStore != nil {
				if err := e.committedStore.AddBatch(e.ctx, txHashes); err != nil {
					e.logger.Warn().Err(err).Msg("Failed to mark transactions as committed")
				}
			}

			// Remove from mempool
			if e.pool != nil {
				e.pool.Remove(txHashes)
			}
		}
	}

	e.logger.Info().
		Str("block_hash", fmt.Sprintf("%x", blockHash[:8])).
		Uint64("height", uint64(block.Height)).
		Msg("Block committed and applied")

	return nil
}

// ExecutorEventTracer is an EventTracer that forwards events to the executor.
type ExecutorEventTracer struct {
	executor *Executor
}

// NewExecutorEventTracer creates a new event tracer that forwards to an executor.
func NewExecutorEventTracer(executor *Executor) *ExecutorEventTracer {
	return &ExecutorEventTracer{executor: executor}
}

// RecordEvent implements the events.EventTracer interface.
func (t *ExecutorEventTracer) RecordEvent(nodeID uint16, eventType events.EventType, payload events.EventPayload) {
	// Forward BlockCommitted and LeaderElected events
	if eventType == events.EventBlockCommitted || eventType == events.EventLeaderElected {
		event := events.ConsensusEvent{
			NodeID:    nodeID,
			EventType: eventType,
			Payload:   payload,
		}
		select {
		case t.executor.eventChan <- event:
		default:
			// Channel full, log warning but don't block
			t.executor.logger.Warn().Msg("Event channel full, dropping event")
		}
	}
}

// RecordTransition implements the events.EventTracer interface.
func (t *ExecutorEventTracer) RecordTransition(nodeID uint16, from, to events.State, trigger string) {
	// Not needed for executor
}

// RecordMessage implements the events.EventTracer interface.
func (t *ExecutorEventTracer) RecordMessage(nodeID uint16, direction events.MessageDirection, msgType string, payload events.EventPayload) {
	// Not needed for executor
}

// Ensure ExecutorEventTracer implements EventTracer
var _ events.EventTracer = (*ExecutorEventTracer)(nil)
