package integration

import (
	"context"
	"fmt"
	"sync"

	"github.com/rs/zerolog"

	"btc-gateway/pkg/consensus/messages"
	"btc-gateway/pkg/consensus/network"
	"btc-gateway/pkg/consensus/types"
)

// MessageProcessor handles incoming messages for a node in a background goroutine
type MessageProcessor struct {
	nodeID      types.NodeID
	coordinator *HotStuffCoordinator
	network     network.NetworkInterface
	logger      zerolog.Logger
	
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	
	// Configuration
	enableLogging bool
}

// NewMessageProcessor creates a new message processor for a node
func NewMessageProcessor(nodeID types.NodeID, coordinator *HotStuffCoordinator, network network.NetworkInterface, logger zerolog.Logger) *MessageProcessor {
	ctx, cancel := context.WithCancel(context.Background())
	
	return &MessageProcessor{
		nodeID:        nodeID,
		coordinator:   coordinator,
		network:       network,
		logger:        logger.With().Uint16("node_id", uint16(nodeID)).Str("component", "message_processor").Logger(),
		ctx:           ctx,
		cancel:        cancel,
		enableLogging: false, // Default to quiet operation
	}
}

// EnableLogging enables debug logging for message processing
func (mp *MessageProcessor) EnableLogging(enable bool) {
	mp.enableLogging = enable
}

// Start begins processing messages in a background goroutine
func (mp *MessageProcessor) Start() {
	mp.wg.Add(1)
	go mp.processMessages()
}

// Stop gracefully stops message processing
func (mp *MessageProcessor) Stop() {
	mp.cancel()
	mp.wg.Wait()
}

// processMessages runs the main message processing loop
func (mp *MessageProcessor) processMessages() {
	defer mp.wg.Done()
	
	if mp.enableLogging {
		mp.logger.Info().
			Str("action", "message_processing_started").
			Msg("Message processor started")
	}
	
	for {
		select {
		case <-mp.ctx.Done():
			if mp.enableLogging {
				mp.logger.Info().
					Str("action", "message_processing_stopped").
					Msg("Message processor shutting down")
			}
			return
			
		case msg := <-mp.network.Receive():
			mp.handleMessage(msg)
		}
	}
}

// handleMessage processes a single incoming message
func (mp *MessageProcessor) handleMessage(msg network.ReceivedMessage) {
	// Route message based on type following protocol specification
	switch m := msg.Message.(type) {
	case *messages.ProposalMsg:
		if err := mp.coordinator.ProcessProposal(m); err != nil {
			if mp.enableLogging {
				mp.logger.Warn().
					Err(err).
					Uint64("view", uint64(m.View())).
					Uint16("sender", uint16(m.Sender())).
					Hex("block_hash", m.Block.Hash[:]).
					Str("message_type", "proposal").
					Str("action", "message_processing_failed").
					Msg("Failed to process proposal message")
			}
		}
		
	case *messages.VoteMsg:
		if err := mp.coordinator.ProcessVote(m.Vote); err != nil {
			if mp.enableLogging {
				mp.logger.Warn().
					Err(err).
					Uint64("view", uint64(m.Vote.View)).
					Uint16("voter", uint16(m.Vote.Voter)).
					Hex("block_hash", m.Vote.BlockHash[:]).
					Str("phase", m.Vote.Phase.String()).
					Str("message_type", "vote").
					Str("action", "message_processing_failed").
					Msg("Failed to process vote message")
			}
		}
		
	case *messages.QCMsg:
		// Process QC messages based on type
		switch m.QCType {
		case messages.MsgTypePrepareQC:
			if err := mp.coordinator.ProcessPrepareQC(m.QC); err != nil {
				if mp.enableLogging {
					fmt.Printf("[Node %d] ⚠️ Failed to process PrepareQC: %v\n", mp.nodeID, err)
				}
			}
		case messages.MsgTypePreCommitQC:
			if err := mp.coordinator.ProcessPreCommitQC(m.QC); err != nil {
				if mp.enableLogging {
					fmt.Printf("[Node %d] ⚠️ Failed to process PreCommitQC: %v\n", mp.nodeID, err)
				}
			}
		case messages.MsgTypeCommitQC:
			if err := mp.coordinator.ProcessCommitQC(m.QC); err != nil {
				if mp.enableLogging {
					fmt.Printf("[Node %d] ⚠️ Failed to process CommitQC: %v\n", mp.nodeID, err)
				}
			}
		}
		
	case *messages.TimeoutMsg:
		if err := mp.coordinator.ProcessTimeoutMessage(m); err != nil {
			if mp.enableLogging {
				fmt.Printf("[Node %d] ⚠️ Failed to process timeout: %v\n", mp.nodeID, err)
			}
		}
		
	case *messages.NewViewMsg:
		if err := mp.coordinator.ProcessNewViewMessage(m); err != nil {
			if mp.enableLogging {
				fmt.Printf("[Node %d] ⚠️ Failed to process NewView: %v\n", mp.nodeID, err)
			}
		}
		
	default:
		if mp.enableLogging {
			fmt.Printf("[Node %d] ⚠️ Unknown message type: %T\n", mp.nodeID, m)
		}
	}
}

// NetworkManager manages message processors for multiple nodes
type NetworkManager struct {
	processors map[types.NodeID]*MessageProcessor
	mu         sync.RWMutex
}

// NewNetworkManager creates a new network manager
func NewNetworkManager() *NetworkManager {
	return &NetworkManager{
		processors: make(map[types.NodeID]*MessageProcessor),
	}
}

// AddNode adds a node to the network manager
func (nm *NetworkManager) AddNode(node *Node) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	
	processor := NewMessageProcessor(node.GetNodeID(), node.GetCoordinator(), node.GetNetwork(), zerolog.Nop())
	nm.processors[node.GetNodeID()] = processor
}

// EnableLogging enables logging for all message processors
func (nm *NetworkManager) EnableLogging(enable bool) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	
	for _, processor := range nm.processors {
		processor.EnableLogging(enable)
	}
}

// StartAll starts message processing for all nodes
func (nm *NetworkManager) StartAll() {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	
	for _, processor := range nm.processors {
		processor.Start()
	}
}

// StopAll stops message processing for all nodes
func (nm *NetworkManager) StopAll() {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	
	for _, processor := range nm.processors {
		processor.Stop()
	}
}

