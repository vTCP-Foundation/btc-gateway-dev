// Package adapter provides adapters to bridge libp2p networking with consensus interfaces.
package adapter

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/rs/zerolog/log"

	"btc-gateway/pkg/consensus/messages"
	consensusnetwork "btc-gateway/pkg/consensus/network"
	"btc-gateway/pkg/consensus/types"
)

const (
	// ConsensusProtocolID is the libp2p protocol ID for consensus messages
	ConsensusProtocolID = "/hotstuff/consensus/1.0.0"
	// MaxMessageSize is the maximum size of a consensus message
	MaxMessageSize = 1024 * 1024 // 1MB
	// ReadTimeout is the timeout for reading a message from a stream
	ReadTimeout = 30 * time.Second
	// WriteTimeout is the timeout for writing a message to a stream
	WriteTimeout = 30 * time.Second
)

// ConsensusNetworkAdapter bridges libp2p host to consensus NetworkInterface.
// It handles message serialization/deserialization and node ID to peer ID mapping.
type ConsensusNetworkAdapter struct {
	host        host.Host
	nodeMapping map[types.NodeID]peer.ID
	peerMapping map[peer.ID]types.NodeID
	receiveChan chan consensusnetwork.ReceivedMessage
	localNodeID types.NodeID
	mu          sync.RWMutex
	ctx         context.Context
	cancel      context.CancelFunc
	started     bool
}

// NewConsensusNetworkAdapter creates a new adapter bridging libp2p to consensus.
func NewConsensusNetworkAdapter(h host.Host, localNodeID types.NodeID) *ConsensusNetworkAdapter {
	ctx, cancel := context.WithCancel(context.Background())
	adapter := &ConsensusNetworkAdapter{
		host:        h,
		nodeMapping: make(map[types.NodeID]peer.ID),
		peerMapping: make(map[peer.ID]types.NodeID),
		receiveChan: make(chan consensusnetwork.ReceivedMessage, 1000),
		localNodeID: localNodeID,
		ctx:         ctx,
		cancel:      cancel,
	}
	return adapter
}

// RegisterNode maps a consensus NodeID to a libp2p peer.ID.
func (a *ConsensusNetworkAdapter) RegisterNode(nodeID types.NodeID, peerID peer.ID) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nodeMapping[nodeID] = peerID
	a.peerMapping[peerID] = nodeID
	log.Debug().
		Uint16("node_id", uint16(nodeID)).
		Str("peer_id", peerID.String()).
		Msg("Registered node in network adapter")
}

// Start initializes the adapter and begins listening for incoming messages.
func (a *ConsensusNetworkAdapter) Start() error {
	a.mu.Lock()
	if a.started {
		a.mu.Unlock()
		return fmt.Errorf("adapter already started")
	}
	a.started = true
	a.mu.Unlock()

	// Register the stream handler for incoming consensus messages
	a.host.SetStreamHandler(protocol.ID(ConsensusProtocolID), a.handleIncomingStream)
	return nil
}

// Stop shuts down the adapter.
func (a *ConsensusNetworkAdapter) Stop() error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if !a.started {
		return nil
	}

	a.cancel()
	a.host.RemoveStreamHandler(protocol.ID(ConsensusProtocolID))
	close(a.receiveChan)
	a.started = false
	return nil
}

// handleIncomingStream processes an incoming stream with a consensus message.
func (a *ConsensusNetworkAdapter) handleIncomingStream(stream network.Stream) {
	defer stream.Close()

	senderPeerID := stream.Conn().RemotePeer()
	log.Debug().
		Str("from_peer", senderPeerID.String()).
		Msg("Received incoming consensus stream")

	// Set read deadline
	if err := stream.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		log.Error().Err(err).Msg("Failed to set read deadline")
		return
	}

	// Read the message
	data, err := io.ReadAll(io.LimitReader(stream, MaxMessageSize))
	if err != nil {
		log.Error().Err(err).Str("from_peer", senderPeerID.String()).Msg("Failed to read message")
		return
	}

	// Deserialize the message
	var envelope messageEnvelope
	if err := json.Unmarshal(data, &envelope); err != nil {
		log.Error().Err(err).Str("from_peer", senderPeerID.String()).Msg("Failed to unmarshal envelope")
		return
	}

	// Decode the actual message
	msg, err := decodeMessage(envelope.Type, envelope.Data)
	if err != nil {
		log.Error().Err(err).Str("type", envelope.Type).Msg("Failed to decode message")
		return
	}

	// Get sender's node ID
	a.mu.RLock()
	senderNodeID, ok := a.peerMapping[senderPeerID]
	a.mu.RUnlock()
	if !ok {
		log.Warn().
			Str("from_peer", senderPeerID.String()).
			Str("msg_type", envelope.Type).
			Msg("Received message from unknown peer - not in peerMapping")
		return
	}

	log.Debug().
		Uint16("from_node", uint16(senderNodeID)).
		Str("msg_type", envelope.Type).
		Msg("Received consensus message")

	// Queue the message
	select {
	case a.receiveChan <- consensusnetwork.ReceivedMessage{
		Message:    msg,
		Sender:     senderNodeID,
		ReceivedAt: time.Now(),
	}:
		log.Debug().
			Uint16("from_node", uint16(senderNodeID)).
			Str("msg_type", envelope.Type).
			Msg("Message queued to receive channel")
	case <-a.ctx.Done():
		log.Warn().Msg("Context cancelled, dropping message")
		return
	default:
		log.Warn().
			Uint16("from_node", uint16(senderNodeID)).
			Str("msg_type", envelope.Type).
			Msg("Receive channel full, dropping message")
	}
}

// Send sends a consensus message to a specific node.
func (a *ConsensusNetworkAdapter) Send(ctx context.Context, nodeID types.NodeID, message messages.ConsensusMessage) error {
	a.mu.RLock()
	peerID, ok := a.nodeMapping[nodeID]
	a.mu.RUnlock()
	if !ok {
		return fmt.Errorf("unknown node ID: %d", nodeID)
	}

	return a.sendToPeer(ctx, peerID, message)
}

// Broadcast sends a consensus message to all registered peers except self.
func (a *ConsensusNetworkAdapter) Broadcast(ctx context.Context, message messages.ConsensusMessage) error {
	a.mu.RLock()
	peers := make([]peer.ID, 0, len(a.nodeMapping))
	nodeIDs := make([]types.NodeID, 0, len(a.nodeMapping))
	for nodeID, peerID := range a.nodeMapping {
		if nodeID != a.localNodeID {
			peers = append(peers, peerID)
			nodeIDs = append(nodeIDs, nodeID)
		}
	}
	a.mu.RUnlock()

	log.Debug().
		Int("peer_count", len(peers)).
		Str("msg_type", fmt.Sprintf("%T", message)).
		Msg("Broadcasting consensus message")

	var lastErr error
	for i, peerID := range peers {
		if err := a.sendToPeer(ctx, peerID, message); err != nil {
			log.Error().
				Err(err).
				Uint16("to_node", uint16(nodeIDs[i])).
				Str("to_peer", peerID.String()).
				Msg("Failed to send broadcast message")
			lastErr = err
		} else {
			log.Debug().
				Uint16("to_node", uint16(nodeIDs[i])).
				Str("msg_type", fmt.Sprintf("%T", message)).
				Msg("Broadcast sent to peer")
		}
	}
	return lastErr
}

// Receive returns the channel for receiving consensus messages.
func (a *ConsensusNetworkAdapter) Receive() <-chan consensusnetwork.ReceivedMessage {
	return a.receiveChan
}

// sendToPeer sends a message to a specific peer.
func (a *ConsensusNetworkAdapter) sendToPeer(ctx context.Context, peerID peer.ID, message messages.ConsensusMessage) error {
	// Open a stream to the peer
	stream, err := a.host.NewStream(ctx, peerID, protocol.ID(ConsensusProtocolID))
	if err != nil {
		log.Debug().
			Err(err).
			Str("peer_id", peerID.String()).
			Msg("Failed to open stream to peer")
		return fmt.Errorf("failed to open stream to peer %s: %w", peerID, err)
	}
	defer stream.Close()

	// Set write deadline
	if err := stream.SetWriteDeadline(time.Now().Add(WriteTimeout)); err != nil {
		return fmt.Errorf("failed to set write deadline: %w", err)
	}

	// Serialize and send the message
	envelope, err := encodeMessage(message)
	if err != nil {
		return fmt.Errorf("failed to encode message: %w", err)
	}

	data, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("failed to marshal envelope: %w", err)
	}

	if _, err := stream.Write(data); err != nil {
		return fmt.Errorf("failed to write message: %w", err)
	}

	return nil
}

// messageEnvelope wraps a consensus message for serialization.
type messageEnvelope struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`
}

// encodeMessage serializes a consensus message into an envelope.
func encodeMessage(msg messages.ConsensusMessage) (*messageEnvelope, error) {
	var msgType string
	switch msg.(type) {
	case *messages.ProposalMsg:
		msgType = "proposal"
	case *messages.VoteMsg:
		msgType = "vote"
	case *messages.TimeoutMsg:
		msgType = "timeout"
	case *messages.NewViewMsg:
		msgType = "newview"
	case *messages.QCMsg:
		msgType = "qc"
	default:
		return nil, fmt.Errorf("unknown message type: %T", msg)
	}

	data, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return &messageEnvelope{
		Type: msgType,
		Data: data,
	}, nil
}

// decodeMessage deserializes a consensus message from an envelope.
func decodeMessage(msgType string, data json.RawMessage) (messages.ConsensusMessage, error) {
	switch msgType {
	case "proposal":
		var msg messages.ProposalMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	case "vote":
		var msg messages.VoteMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	case "timeout":
		var msg messages.TimeoutMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	case "newview":
		var msg messages.NewViewMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	case "qc":
		var msg messages.QCMsg
		if err := json.Unmarshal(data, &msg); err != nil {
			return nil, err
		}
		return &msg, nil
	default:
		return nil, fmt.Errorf("unknown message type: %s", msgType)
	}
}

// Ensure ConsensusNetworkAdapter implements NetworkInterface
var _ consensusnetwork.NetworkInterface = (*ConsensusNetworkAdapter)(nil)
