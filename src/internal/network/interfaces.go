package network

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"btc-gateway/internal/types"
)

// NetworkManager defines the core network management interface
type NetworkManager interface {
	// Core lifecycle
	Start(ctx context.Context) error
	Stop() error

	// Connection management
	Connect(ctx context.Context, addr multiaddr.Multiaddr) error
	Disconnect(peerID peer.ID) error
	GetConnections() []ConnectionInfo
	GetHost() host.Host

	// Protocol management
	RegisterProtocolHandler(protocolID protocol.ID, handler ProtocolHandler) error
	UnregisterProtocolHandler(protocolID protocol.ID) error

	// Event handling for Task 2.3 integration
	RegisterConnectionHandler(handler ConnectionEventHandler) error
	UnregisterConnectionHandler(handler ConnectionEventHandler) error

	// Configuration management for Task 2.5 integration
	UpdateConfig(config *types.NetworkConfig) error
	ValidateConfig(config *types.NetworkConfig) error
	GetCurrentConfig() *types.NetworkConfig
}

// ProtocolHandler defines interface for protocol message handling (Task 2.4)
type ProtocolHandler interface {
	HandleStream(stream network.Stream) error
	ProtocolID() protocol.ID
}

// ConnectionEventHandler defines interface for connection events (Task 2.3)
type ConnectionEventHandler interface {
	OnPeerConnected(peerID peer.ID, addr multiaddr.Multiaddr) error
	OnPeerDisconnected(peerID peer.ID, reason error) error
}

// ConfigurableComponent defines interface for hot-reload capability (Task 2.5)
type ConfigurableComponent interface {
	UpdateConfig(config *types.NetworkConfig) error
	ValidateConfig(config *types.NetworkConfig) error
}

// ConnectionInfo represents information about a connection
type ConnectionInfo struct {
	PeerID      peer.ID
	Address     multiaddr.Multiaddr
	State       ConnectionState
	ConnectedAt time.Time
	LastSeen    time.Time
}

// ConnectionState represents the state of a connection
type ConnectionState int

const (
	StateIdle ConnectionState = iota
	StateConnecting
	StateConnected
	StateDisconnecting
	StateFailed
)

// String returns string representation of connection state
func (s ConnectionState) String() string {
	switch s {
	case StateIdle:
		return "idle"
	case StateConnecting:
		return "connecting"
	case StateConnected:
		return "connected"
	case StateDisconnecting:
		return "disconnecting"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// ConnectionEvent represents a connection lifecycle event
type ConnectionEvent struct {
	Type      ConnectionEventType
	PeerID    peer.ID
	Address   multiaddr.Multiaddr
	Timestamp time.Time
	Error     error
}

// ConnectionEventType represents the type of connection event
type ConnectionEventType int

const (
	EventConnected ConnectionEventType = iota
	EventDisconnected
	EventConnectionFailed
)

// String returns string representation of connection event type
func (t ConnectionEventType) String() string {
	switch t {
	case EventConnected:
		return "connected"
	case EventDisconnected:
		return "disconnected"
	case EventConnectionFailed:
		return "connection_failed"
	default:
		return "unknown"
	}
}
