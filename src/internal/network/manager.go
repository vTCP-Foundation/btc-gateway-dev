package network

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"btc-gateway/internal/storage"
	"btc-gateway/internal/types"
)

// Manager implements the NetworkManager interface
type Manager struct {
	// Configuration
	config      *types.NetworkConfig
	peersConfig *types.PeersConfig

	// libp2p host wrapper
	hostWrapper *HostWrapper

	// Peer storage and management
	peerStorage  storage.PeerStorage
	peerHandlers map[string]*PeerHandler
	peerMutex    sync.RWMutex

	// Protocol handlers
	protocolHandlers map[protocol.ID]ProtocolHandler
	protocolMutex    sync.RWMutex

	// Connection tracking
	connections      map[peer.ID]*ConnectionInfo
	connectionsMutex sync.RWMutex

	// Event handlers
	connectionHandlers      []ConnectionEventHandler
	connectionHandlersMutex sync.RWMutex

	// State management
	isStarted  bool
	startMutex sync.Mutex

	// Context for operations
	ctx    context.Context
	cancel context.CancelFunc
}

// NewManager creates a new network manager with the given configuration
func NewManager(config *types.Config) (*Manager, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	// Convert private key to libp2p format
	var privateKey crypto.PrivKey
	if config.Node.PrivateKey != "" {
		key, err := ConvertPrivateKeyFromBase64(config.Node.PrivateKey)
		if err != nil {
			return nil, fmt.Errorf("failed to convert private key: %w", err)
		}
		privateKey = key
	}

	// Create host wrapper
	hostWrapper, err := NewHostWrapper(&config.Network, privateKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create host wrapper: %w", err)
	}

	// Initialize peer storage
	peerStorage := storage.NewFilePeerStorage("peers.yaml")

	ctx, cancel := context.WithCancel(context.Background())

	manager := &Manager{
		config:           &config.Network,
		peersConfig:      &config.Peers,
		hostWrapper:      hostWrapper,
		peerStorage:      peerStorage,
		peerHandlers:     make(map[string]*PeerHandler),
		protocolHandlers: make(map[protocol.ID]ProtocolHandler),
		connections:      make(map[peer.ID]*ConnectionInfo),
		ctx:              ctx,
		cancel:           cancel,
	}

	return manager, nil
}

// Start starts the network manager and begins listening for connections
func (m *Manager) Start(ctx context.Context) error {
	m.startMutex.Lock()
	defer m.startMutex.Unlock()

	if m.isStarted {
		return fmt.Errorf("network manager is already started")
	}

	// Register network event handlers
	m.hostWrapper.Host().Network().Notify(&networkNotifiee{manager: m})

	m.isStarted = true

	// Log successful start
	// TODO: Use proper logging system
	fmt.Printf("Network manager started successfully. Listening on: %v\n", m.hostWrapper.Host().Addrs())

	// Print peer ID for configuration
	peerID := m.hostWrapper.Host().ID()
	fmt.Printf("NODE_PEER_ID: %s\n", peerID)

	// Load peers and perform bootstrap validation
	if err := m.initializePeers(ctx); err != nil {
		m.Stop()
		return fmt.Errorf("bootstrap validation failed: %w", err)
	}

	return nil
}

// Stop stops the network manager and closes all connections
func (m *Manager) Stop() error {
	m.startMutex.Lock()
	defer m.startMutex.Unlock()

	if !m.isStarted {
		return nil // Already stopped
	}

	// Cancel context to stop all operations
	m.cancel()

	// Close host wrapper
	if err := m.hostWrapper.Close(); err != nil {
		return fmt.Errorf("failed to close host wrapper: %w", err)
	}

	// Stop all peer handlers
	m.peerMutex.Lock()
	for _, handler := range m.peerHandlers {
		handler.Stop()
	}
	m.peerHandlers = make(map[string]*PeerHandler)
	m.peerMutex.Unlock()

	// Clear connections
	m.connectionsMutex.Lock()
	m.connections = make(map[peer.ID]*ConnectionInfo)
	m.connectionsMutex.Unlock()

	m.isStarted = false

	return nil
}

// Connect establishes a connection to a peer at the given address
func (m *Manager) Connect(ctx context.Context, addr multiaddr.Multiaddr) error {
	if !m.isStarted {
		return fmt.Errorf("network manager is not started")
	}

	return m.hostWrapper.Connect(ctx, addr)
}

// Disconnect disconnects from the specified peer
func (m *Manager) Disconnect(peerID peer.ID) error {
	if !m.isStarted {
		return fmt.Errorf("network manager is not started")
	}

	return m.hostWrapper.Host().Network().ClosePeer(peerID)
}

// GetConnections returns information about all current connections
func (m *Manager) GetConnections() []ConnectionInfo {
	m.connectionsMutex.RLock()
	defer m.connectionsMutex.RUnlock()

	connections := make([]ConnectionInfo, 0, len(m.connections))
	for _, conn := range m.connections {
		connections = append(connections, *conn)
	}

	return connections
}

// GetHost returns the underlying libp2p host
func (m *Manager) GetHost() host.Host {
	return m.hostWrapper.Host()
}

// RegisterProtocolHandler registers a handler for a specific protocol
func (m *Manager) RegisterProtocolHandler(protocolID protocol.ID, handler ProtocolHandler) error {
	m.protocolMutex.Lock()
	defer m.protocolMutex.Unlock()

	if _, exists := m.protocolHandlers[protocolID]; exists {
		return fmt.Errorf("protocol handler for %s already registered", protocolID)
	}

	m.protocolHandlers[protocolID] = handler

	// Register with libp2p host
	m.hostWrapper.Host().SetStreamHandler(protocolID, func(stream network.Stream) {
		if err := handler.HandleStream(stream); err != nil {
			// TODO: Use proper logging
			fmt.Printf("Error handling stream for protocol %s: %v\n", protocolID, err)
			stream.Reset()
		}
	})

	return nil
}

// UnregisterProtocolHandler unregisters a protocol handler
func (m *Manager) UnregisterProtocolHandler(protocolID protocol.ID) error {
	m.protocolMutex.Lock()
	defer m.protocolMutex.Unlock()

	if _, exists := m.protocolHandlers[protocolID]; !exists {
		return fmt.Errorf("protocol handler for %s not found", protocolID)
	}

	delete(m.protocolHandlers, protocolID)

	// Remove from libp2p host
	m.hostWrapper.Host().RemoveStreamHandler(protocolID)

	return nil
}

// RegisterConnectionHandler registers a connection event handler
func (m *Manager) RegisterConnectionHandler(handler ConnectionEventHandler) error {
	m.connectionHandlersMutex.Lock()
	defer m.connectionHandlersMutex.Unlock()

	m.connectionHandlers = append(m.connectionHandlers, handler)
	return nil
}

// UnregisterConnectionHandler unregisters a connection event handler
func (m *Manager) UnregisterConnectionHandler(handler ConnectionEventHandler) error {
	m.connectionHandlersMutex.Lock()
	defer m.connectionHandlersMutex.Unlock()

	for i, h := range m.connectionHandlers {
		if h == handler {
			m.connectionHandlers = append(m.connectionHandlers[:i], m.connectionHandlers[i+1:]...)
			return nil
		}
	}

	return fmt.Errorf("connection handler not found")
}

// UpdateConfig updates the network configuration (for Task 2.5 hot-reload)
func (m *Manager) UpdateConfig(config *types.NetworkConfig) error {
	// For Task 2.5 implementation - placeholder for now
	// This will be fully implemented in Task 2.5
	m.config = config
	return nil
}

// ValidateConfig validates a network configuration
func (m *Manager) ValidateConfig(config *types.NetworkConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	if len(config.Addresses) == 0 {
		return fmt.Errorf("at least one listen address must be specified")
	}

	// Validate each address
	for i, addrStr := range config.Addresses {
		_, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return fmt.Errorf("invalid address at index %d: %w", i, err)
		}
	}

	return nil
}

// GetCurrentConfig returns the current network configuration
func (m *Manager) GetCurrentConfig() *types.NetworkConfig {
	return m.config
}

// emitConnectionEvent emits a connection event to all registered handlers
func (m *Manager) emitConnectionEvent(eventType ConnectionEventType, peerID peer.ID, addr multiaddr.Multiaddr, err error) {
	m.connectionHandlersMutex.RLock()
	handlers := make([]ConnectionEventHandler, len(m.connectionHandlers))
	copy(handlers, m.connectionHandlers)
	m.connectionHandlersMutex.RUnlock()

	// Emit events asynchronously to avoid blocking
	go func() {
		for _, handler := range handlers {
			switch eventType {
			case EventConnected:
				if handlerErr := handler.OnPeerConnected(peerID, addr); handlerErr != nil {
					// TODO: Use proper logging
					fmt.Printf("Error in connection handler: %v\n", handlerErr)
				}
			case EventDisconnected, EventConnectionFailed:
				if handlerErr := handler.OnPeerDisconnected(peerID, err); handlerErr != nil {
					// TODO: Use proper logging
					fmt.Printf("Error in disconnection handler: %v\n", handlerErr)
				}
			}
		}
	}()
}

// updateConnectionState updates the connection state for a peer
func (m *Manager) updateConnectionState(peerID peer.ID, state ConnectionState, addr multiaddr.Multiaddr) {
	m.connectionsMutex.Lock()
	defer m.connectionsMutex.Unlock()

	now := time.Now()

	if conn, exists := m.connections[peerID]; exists {
		conn.State = state
		conn.LastSeen = now
		if addr != nil {
			conn.Address = addr
		}
	} else {
		m.connections[peerID] = &ConnectionInfo{
			PeerID:      peerID,
			Address:     addr,
			State:       state,
			ConnectedAt: now,
			LastSeen:    now,
		}
	}
}

// initializePeers loads peers from storage and performs bootstrap validation
func (m *Manager) initializePeers(ctx context.Context) error {
	// Load peers from storage
	if err := m.peerStorage.LoadPeers(); err != nil {
		return fmt.Errorf("failed to load peers: %w", err)
	}

	peers, err := m.peerStorage.GetPeers()
	if err != nil {
		return fmt.Errorf("failed to get peers: %w", err)
	}

	if len(peers) == 0 {
		fmt.Println("NetworkManager: No peers configured - running in standalone mode")
		return nil
	}

	fmt.Printf("NetworkManager: Loaded %d peers from storage\n", len(peers))

	// Create peer handlers for each peer
	for _, peer := range peers {
		handler, err := NewPeerHandler(peer, m.hostWrapper.Host(), m.peersConfig.ConnectionTimeout)
		if err != nil {
			fmt.Printf("NetworkManager: Failed to create handler for peer %s: %v\n",
				peer.PublicKey[:min(20, len(peer.PublicKey))], err)
			continue
		}

		// Set state change callback to track connection status
		handler.SetStateChangeCallback(m.onPeerStateChange)

		m.peerMutex.Lock()
		m.peerHandlers[peer.PublicKey] = handler
		m.peerMutex.Unlock()

		// Start the peer handler
		handler.Start()
	}

	// Bootstrap validation: wait for at least one successful connection
	return m.waitForBootstrap(ctx)
}

// waitForBootstrap waits for at least one peer connection to be established
func (m *Manager) waitForBootstrap(ctx context.Context) error {
	bootstrapTimeout := time.Minute * 2 // Allow 2 minutes for bootstrap
	deadline := time.Now().Add(bootstrapTimeout)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	fmt.Printf("NetworkManager: Waiting for bootstrap connections (timeout: %v)...\n", bootstrapTimeout)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if time.Now().After(deadline) {
				connectedCount := m.getConnectedPeerCount()
				return fmt.Errorf("bootstrap timeout: only %d peers connected after %v",
					connectedCount, bootstrapTimeout)
			}

			connectedCount := m.getConnectedPeerCount()
			if connectedCount > 0 {
				fmt.Printf("NetworkManager: Bootstrap successful - %d peers connected\n", connectedCount)
				return nil
			}

			// Check if all peers have permanently failed
			if m.allPeersPermanentlyFailed() {
				return fmt.Errorf("bootstrap failed: all peers permanently failed")
			}
		}
	}
}

// getConnectedPeerCount returns the number of currently connected peers
func (m *Manager) getConnectedPeerCount() int {
	m.peerMutex.RLock()
	defer m.peerMutex.RUnlock()

	connected := 0
	for _, handler := range m.peerHandlers {
		if handler.IsConnected() {
			connected++
		}
	}
	return connected
}

// allPeersPermanentlyFailed checks if all peers have permanently failed
func (m *Manager) allPeersPermanentlyFailed() bool {
	m.peerMutex.RLock()
	defer m.peerMutex.RUnlock()

	for _, handler := range m.peerHandlers {
		state := handler.GetState()
		if state != PeerStatePermanentFailure {
			return false
		}
	}
	return len(m.peerHandlers) > 0 // Only return true if we have handlers and all failed
}

// onPeerStateChange handles peer state changes
func (m *Manager) onPeerStateChange(handler *PeerHandler, oldState, newState PeerState) {
	peer := handler.GetPeer()
	fmt.Printf("NetworkManager: Peer %s state changed: %s -> %s\n",
		peer.PublicKey[:min(20, len(peer.PublicKey))], oldState, newState)

	// Update connection tracking based on state
	if newState == PeerStateConnected {
		peerID := handler.GetPeerID()
		if peerID != "" {
			m.updateConnectionState(peerID, StateConnected, handler.GetCurrentAddress())
		}
	} else if oldState == PeerStateConnected {
		peerID := handler.GetPeerID()
		if peerID != "" {
			m.updateConnectionState(peerID, StateFailed, nil)
		}
	}
}

// handleIncomingConnection processes incoming connections and updates peer handlers immediately
func (m *Manager) handleIncomingConnection(peerID peer.ID, conn network.Conn) {
	m.peerMutex.RLock()
	defer m.peerMutex.RUnlock()

	// Find peer handler that matches this connection
	for _, handler := range m.peerHandlers {
		if handler.matchesPeerID(peerID) {
			// Found matching peer handler - immediately update it to connected state
			fmt.Printf("NetworkManager: Incoming connection matched configured peer, updating state immediately\n")
			handler.adoptIncomingConnection(conn)
			return
		}
	}

	// No configured peer handler found - this is fine, just log it
	fmt.Printf("NetworkManager: Received connection from unconfigured peer %s\n", peerID.String())
}

// GetPeerHandlers returns information about all peer handlers
func (m *Manager) GetPeerHandlers() map[string]*PeerHandler {
	m.peerMutex.RLock()
	defer m.peerMutex.RUnlock()

	// Return a copy to prevent external modification
	handlers := make(map[string]*PeerHandler)
	for key, handler := range m.peerHandlers {
		handlers[key] = handler
	}
	return handlers
}

// Helper function for minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// handleConnectionDeduplication checks for and handles redundant connections
// Returns true if the connection was closed due to deduplication
func (m *Manager) handleConnectionDeduplication(newConn network.Conn) bool {
	peerID := newConn.RemotePeer()

	// Get all existing connections to this peer
	existingConns := m.hostWrapper.Host().Network().ConnsToPeer(peerID)

	fmt.Printf("DEDUP DEBUG: Checking connection to peer %s\n", peerID.String())
	fmt.Printf("DEDUP DEBUG: Found %d existing connections to this peer\n", len(existingConns))
	for i, conn := range existingConns {
		fmt.Printf("DEDUP DEBUG: Connection %d: opened=%v, direction=%s\n",
			i, conn.Stat().Opened, conn.Stat().Direction)
	}

	// If we only have one connection (the new one), no deduplication needed
	if len(existingConns) <= 1 {
		fmt.Printf("DEDUP DEBUG: Only %d connection(s), no deduplication needed\n", len(existingConns))
		return false
	}

	// We have multiple connections to the same peer - need to deduplicate
	fmt.Printf("Connection deduplication: Found %d connections to peer %s\n",
		len(existingConns), peerID.String())

	// Strategy: Keep the oldest connection, close newer ones
	// This prevents the race condition where both sides close connections
	oldestConn := existingConns[0]
	for _, conn := range existingConns {
		// Find the connection that was opened first (has lowest ID or earliest timestamp)
		if conn.Stat().Opened.Before(oldestConn.Stat().Opened) {
			oldestConn = conn
		}
	}

	// If the new connection is the oldest (shouldn't happen but safety check)
	if newConn == oldestConn {
		// Close all other connections
		fmt.Printf("Connection deduplication: Keeping new connection (oldest), closing %d newer connections\n",
			len(existingConns)-1)

		for _, conn := range existingConns {
			if conn != newConn {
				go func(c network.Conn) {
					fmt.Printf("Connection deduplication: Closing connection %s (opened: %v)\n",
						c.RemotePeer().String(), c.Stat().Opened)
					c.Close()
				}(conn)
			}
		}
		return false // Don't close the new connection
	} else {
		// Close the new connection, keep the oldest existing one
		fmt.Printf("Connection deduplication: Closing new connection (opened: %v), keeping oldest (opened: %v)\n",
			newConn.Stat().Opened, oldestConn.Stat().Opened)

		go func() {
			newConn.Close()
		}()
		return true // New connection was closed
	}
}
