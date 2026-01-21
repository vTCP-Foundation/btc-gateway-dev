package network

import (
	"context"
	"encoding/base64"
	"fmt"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"btc-gateway/internal/storage"
)

// PeerState represents the current state of a peer
type PeerState int

const (
	PeerStateIdle PeerState = iota
	PeerStateConnecting
	PeerStateConnected
	PeerStateTemporaryFailure
	PeerStatePermanentFailure
)

// String returns string representation of peer state
func (s PeerState) String() string {
	switch s {
	case PeerStateIdle:
		return "idle"
	case PeerStateConnecting:
		return "connecting"
	case PeerStateConnected:
		return "connected"
	case PeerStateTemporaryFailure:
		return "temporary_failure"
	case PeerStatePermanentFailure:
		return "permanent_failure"
	default:
		return "unknown"
	}
}

// PeerHandler manages the complete lifecycle and connection of an individual peer
type PeerHandler struct {
	// Peer information
	peer        storage.Peer
	addresses   []multiaddr.Multiaddr
	currentAddr multiaddr.Multiaddr
	peerID      peer.ID

	// libp2p host for direct connection management
	host host.Host

	// State management
	state        PeerState
	stateMutex   sync.RWMutex
	lastAttempt  time.Time
	failureCount int
	nextRetry    time.Time

	// Connection tracking
	connection network.Conn
	connMutex  sync.RWMutex

	// Context and cancellation
	ctx    context.Context
	cancel context.CancelFunc

	// Configuration
	connectionTimeout time.Duration
	maxFailures       int
	baseRetryDelay    time.Duration
	maxRetryDelay     time.Duration

	// Event callbacks
	onStateChange func(handler *PeerHandler, oldState, newState PeerState)
}

// NewPeerHandler creates a new peer handler for managing a specific peer
func NewPeerHandler(peer storage.Peer, host host.Host, connectionTimeout time.Duration) (*PeerHandler, error) {
	// Parse multiaddresses
	addresses := make([]multiaddr.Multiaddr, 0, len(peer.Addresses))
	for _, addrStr := range peer.Addresses {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return nil, fmt.Errorf("invalid address %s: %w", addrStr, err)
		}
		addresses = append(addresses, addr)
	}

	if len(addresses) == 0 {
		return nil, fmt.Errorf("no valid addresses for peer")
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &PeerHandler{
		peer:              peer,
		addresses:         addresses,
		host:              host,
		state:             PeerStateIdle,
		connectionTimeout: connectionTimeout,
		maxFailures:       5, // Allow 5 failures before marking as permanent
		baseRetryDelay:    time.Second * 2,
		maxRetryDelay:     time.Minute * 5,
		ctx:               ctx,
		cancel:            cancel,
	}, nil
}

// SetStateChangeCallback sets a callback for state changes
func (h *PeerHandler) SetStateChangeCallback(callback func(handler *PeerHandler, oldState, newState PeerState)) {
	h.onStateChange = callback
}

// Start begins managing this peer's connection lifecycle
func (h *PeerHandler) Start() {
	go h.connectionLoop()
}

// Stop stops managing this peer and closes any active connection
func (h *PeerHandler) Stop() {
	h.cancel()
	h.closeConnection()
}

// GetState returns the current state of this peer
func (h *PeerHandler) GetState() PeerState {
	h.stateMutex.RLock()
	defer h.stateMutex.RUnlock()
	return h.state
}

// GetPeer returns the peer information
func (h *PeerHandler) GetPeer() storage.Peer {
	return h.peer
}

// GetPeerID returns the libp2p peer ID if connected
func (h *PeerHandler) GetPeerID() peer.ID {
	h.stateMutex.RLock()
	defer h.stateMutex.RUnlock()
	return h.peerID
}

// IsConnected returns true if peer is currently connected
func (h *PeerHandler) IsConnected() bool {
	return h.GetState() == PeerStateConnected
}

// GetConnection returns the current connection if available
func (h *PeerHandler) GetConnection() network.Conn {
	h.connMutex.RLock()
	defer h.connMutex.RUnlock()
	return h.connection
}

// GetCurrentAddress returns the current connected address
func (h *PeerHandler) GetCurrentAddress() multiaddr.Multiaddr {
	h.stateMutex.RLock()
	defer h.stateMutex.RUnlock()
	return h.currentAddr
}

// setState changes the peer state and triggers callbacks
func (h *PeerHandler) setState(newState PeerState) {
	h.stateMutex.Lock()
	oldState := h.state
	h.state = newState
	h.stateMutex.Unlock()

	// Trigger callback if state actually changed
	if oldState != newState && h.onStateChange != nil {
		h.onStateChange(h, oldState, newState)
	}
}

// connectionLoop manages the connection lifecycle for this peer
func (h *PeerHandler) connectionLoop() {
	// Initial connection attempt
	h.attemptConnection()

	ticker := time.NewTicker(time.Second * 30) // Check every 30 seconds
	defer ticker.Stop()

	for {
		select {
		case <-h.ctx.Done():
			return
		case <-ticker.C:
			h.handlePeriodicCheck()
		}
	}
}

// attemptConnection tries to establish a connection to this peer
func (h *PeerHandler) attemptConnection() {
	currentState := h.GetState()

	fmt.Printf("PeerHandler: attemptConnection() called for peer %s (state: %s) at %v\n",
		h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], currentState, time.Now())

	if currentState == PeerStateConnecting {
		fmt.Printf("PeerHandler: Already connecting, skipping\n")
		return // Already connecting
	}

	if currentState == PeerStatePermanentFailure {
		fmt.Printf("PeerHandler: In permanent failure state, skipping\n")
		return // Don't retry permanent failures
	}

	// Check if we should retry (if we're in temporary failure state)
	if currentState == PeerStateTemporaryFailure {
		h.stateMutex.RLock()
		shouldWait := time.Now().Before(h.nextRetry)
		nextRetryTime := h.nextRetry
		h.stateMutex.RUnlock()

		fmt.Printf("PeerHandler: In temporary failure state - shouldWait: %v, nextRetry: %v, now: %v\n",
			shouldWait, nextRetryTime, time.Now())

		if shouldWait {
			fmt.Printf("PeerHandler: Not time to retry yet, skipping\n")
			return // Not time to retry yet
		}
		fmt.Printf("PeerHandler: Time to retry! Proceeding with connection attempt\n")
	}

	// Check if we already have a connection to this peer (if we can derive peer ID)
	if peerID, err := h.derivePeerIDFromPublicKey(); err == nil {
		existingConns := h.host.Network().ConnsToPeer(peerID)
		if len(existingConns) > 0 {
			// We already have a connection to this peer, adopt it silently
			fmt.Printf("PeerHandler: Found existing connection to peer %s, adopting instead of creating new one\n",
				h.peer.PublicKey[:min(20, len(h.peer.PublicKey))])
			fmt.Printf("PeerHandler: Existing connections: %d\n", len(existingConns))
			for i, conn := range existingConns {
				fmt.Printf("PeerHandler: Connection %d: direction=%s, opened=%v\n",
					i, conn.Stat().Direction, conn.Stat().Opened)
			}

			h.peerID = peerID
			h.setConnection(existingConns[0])
			h.setState(PeerStateConnected)

			// Reset failure count
			h.stateMutex.Lock()
			h.failureCount = 0
			h.stateMutex.Unlock()

			fmt.Printf("PeerHandler: Successfully adopted existing connection to peer %s\n",
				h.peer.PublicKey[:min(20, len(h.peer.PublicKey))])
			return
		}
	}

	h.setState(PeerStateConnecting)
	h.stateMutex.Lock()
	h.lastAttempt = time.Now()
	h.stateMutex.Unlock()

	fmt.Printf("PeerHandler: Attempting to connect to peer %s...\n", h.peer.PublicKey[:min(20, len(h.peer.PublicKey))])

	// Try each address until one succeeds
	for i, addr := range h.addresses {
		select {
		case <-h.ctx.Done():
			return
		default:
		}

		if h.connectToAddress(addr) {
			h.stateMutex.Lock()
			h.currentAddr = addr
			h.failureCount = 0 // Reset failure count on success
			h.stateMutex.Unlock()

			h.setState(PeerStateConnected)
			fmt.Printf("PeerHandler: Successfully connected to peer at %s\n", addr)
			return
		}

		fmt.Printf("PeerHandler: Failed to connect to peer at %s (attempt %d/%d)\n",
			addr, i+1, len(h.addresses))
	}

	// All addresses failed
	h.handleConnectionFailure()
}

// connectToAddress attempts to connect to a specific address
func (h *PeerHandler) connectToAddress(addr multiaddr.Multiaddr) bool {
	ctx, cancel := context.WithTimeout(h.ctx, h.connectionTimeout)
	defer cancel()

	// Try to extract peer info from address
	addrInfo, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		// Address doesn't include peer ID, derive it from the public key
		peerID, err := h.derivePeerIDFromPublicKey()
		if err != nil {
			fmt.Printf("PeerHandler: Failed to derive peer ID from public key: %v\n", err)
			return false
		}

		fmt.Printf("PeerHandler: Derived peer ID %s for address %s\n", peerID, addr)

		// Add address to peerstore
		h.host.Peerstore().AddAddr(peerID, addr, time.Hour)

		// Create AddrInfo and connect
		addrInfo = &peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{addr},
		}
	}

	// Check existing connections before attempting new connection
	existingConnsBefore := h.host.Network().ConnsToPeer(addrInfo.ID)
	fmt.Printf("PeerHandler: Before Connect() call - existing connections to %s: %d\n",
		addrInfo.ID, len(existingConnsBefore))
	for i, conn := range existingConnsBefore {
		fmt.Printf("PeerHandler: Existing connection %d: direction=%s, opened=%v\n",
			i, conn.Stat().Direction, conn.Stat().Opened)
	}

	fmt.Printf("PeerHandler: Calling host.Connect() to %s...\n", addrInfo.ID)

	// Connect using peer info
	err = h.host.Connect(ctx, *addrInfo)
	if err != nil {
		fmt.Printf("PeerHandler: Connect failed to %s: %v\n", addrInfo.ID, err)
		return false
	}

	fmt.Printf("PeerHandler: host.Connect() succeeded to %s\n", addrInfo.ID)

	// Get the connection after connect
	connAfter := h.host.Network().ConnsToPeer(addrInfo.ID)
	fmt.Printf("PeerHandler: After Connect() call - connections to %s: %d\n",
		addrInfo.ID, len(connAfter))
	for i, conn := range connAfter {
		fmt.Printf("PeerHandler: Connection %d: direction=%s, opened=%v\n",
			i, conn.Stat().Direction, conn.Stat().Opened)
	}

	if len(connAfter) == 0 {
		return false
	}

	h.setConnection(connAfter[0])
	h.peerID = addrInfo.ID
	return true
}

// derivePeerIDFromPublicKey converts the base64 public key to a libp2p peer ID
func (h *PeerHandler) derivePeerIDFromPublicKey() (peer.ID, error) {
	// Decode the base64 public key
	pubKeyBytes, err := base64.StdEncoding.DecodeString(h.peer.PublicKey)
	if err != nil {
		return "", fmt.Errorf("failed to decode public key: %w", err)
	}

	// Create libp2p public key from Ed25519 public key
	pubKey, err := crypto.UnmarshalEd25519PublicKey(pubKeyBytes)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal Ed25519 public key: %w", err)
	}

	// Derive peer ID from public key
	peerID, err := peer.IDFromPublicKey(pubKey)
	if err != nil {
		return "", fmt.Errorf("failed to derive peer ID: %w", err)
	}

	return peerID, nil
}

// setConnection sets the current connection
func (h *PeerHandler) setConnection(conn network.Conn) {
	h.connMutex.Lock()
	defer h.connMutex.Unlock()

	// Only close old connection if it's different from the new one
	// This prevents closing the same connection we're trying to set
	if h.connection != nil && h.connection != conn {
		fmt.Printf("PeerHandler: Closing old connection to set new one\n")
		h.connection.Close()
	}

	h.connection = conn
}

// closeConnection closes the current connection
func (h *PeerHandler) closeConnection() {
	h.connMutex.Lock()
	defer h.connMutex.Unlock()

	if h.connection != nil {
		h.connection.Close()
		h.connection = nil
	}
}

// handleConnectionFailure processes a connection failure
func (h *PeerHandler) handleConnectionFailure() {
	h.stateMutex.Lock()
	h.failureCount++

	if h.failureCount >= h.maxFailures {
		h.stateMutex.Unlock()
		h.setState(PeerStatePermanentFailure)
		fmt.Printf("PeerHandler: Peer %s marked as permanently failed after %d attempts\n",
			h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], h.failureCount)
		return
	}

	// Calculate retry delay with exponential backoff
	retryDelay := time.Duration(h.failureCount) * h.baseRetryDelay
	if retryDelay > h.maxRetryDelay {
		retryDelay = h.maxRetryDelay
	}

	h.nextRetry = time.Now().Add(retryDelay)
	fmt.Printf("PeerHandler: RETRY TIMING - Current time: %v, retry scheduled for: %v (in %v)\n",
		time.Now(), h.nextRetry, retryDelay)
	h.stateMutex.Unlock()

	h.setState(PeerStateTemporaryFailure)
	fmt.Printf("PeerHandler: Peer %s failed (attempt %d/%d), retrying in %v\n",
		h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], h.failureCount, h.maxFailures, retryDelay)
}

// matchesPeerID checks if this handler is managing the given peer ID
func (h *PeerHandler) matchesPeerID(peerID peer.ID) bool {
	// Try to derive the peer ID from our public key and compare
	derivedPeerID, err := h.derivePeerIDFromPublicKey()
	if err != nil {
		return false
	}
	return derivedPeerID == peerID
}

// useExistingConnection adopts an existing connection instead of creating a new one
func (h *PeerHandler) useExistingConnection(conn network.Conn) {
	currentState := h.GetState()

	// Only adopt if we're not already connected or we're trying to connect
	if currentState == PeerStateConnected {
		return // Already have a connection
	}

	fmt.Printf("PeerHandler: Using existing connection to peer %s (was in state: %s)\n",
		h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], currentState)

	// Set the connection and update state
	h.setConnection(conn)
	h.peerID = conn.RemotePeer()
	h.setState(PeerStateConnected)

	// Reset failure count since we now have a connection
	h.stateMutex.Lock()
	h.failureCount = 0
	h.stateMutex.Unlock()
}

// adoptIncomingConnection immediately adopts an incoming connection and updates state
func (h *PeerHandler) adoptIncomingConnection(conn network.Conn) {
	currentState := h.GetState()

	fmt.Printf("PeerHandler: Adopting incoming connection to peer %s (current state: %s)\n",
		h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], currentState)

	// Check if we already have this connection
	h.connMutex.RLock()
	existingConn := h.connection
	h.connMutex.RUnlock()

	if existingConn == conn {
		fmt.Printf("PeerHandler: Already have this connection, skipping adoption\n")
		return
	}

	// Set the connection and update state immediately
	h.setConnection(conn)
	h.peerID = conn.RemotePeer()
	h.setState(PeerStateConnected)

	// Reset failure count since we now have a connection
	h.stateMutex.Lock()
	h.failureCount = 0
	h.stateMutex.Unlock()

	fmt.Printf("PeerHandler: Successfully adopted incoming connection to peer %s (was in state: %s)\n",
		h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], currentState)
}

// handlePeriodicCheck performs periodic health checks and maintenance
func (h *PeerHandler) handlePeriodicCheck() {
	state := h.GetState()
	fmt.Printf("PeerHandler: Periodic check for peer %s (state: %s) at %v\n",
		h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], state, time.Now())

	switch state {
	case PeerStateTemporaryFailure:
		// Check if it's time to retry
		h.stateMutex.RLock()
		shouldRetry := time.Now().After(h.nextRetry)
		nextRetryTime := h.nextRetry
		h.stateMutex.RUnlock()

		fmt.Printf("PeerHandler: In temporary failure state - shouldRetry: %v, nextRetry: %v, now: %v\n",
			shouldRetry, nextRetryTime, time.Now())

		if shouldRetry {
			fmt.Printf("PeerHandler: *** PERIODIC RETRY TRIGGERED *** for peer %s\n",
				h.peer.PublicKey[:min(20, len(h.peer.PublicKey))])
			h.attemptConnection()
		} else {
			fmt.Printf("PeerHandler: Not time for periodic retry yet\n")
		}

	case PeerStateConnected:
		// Check if connection is still alive
		conn := h.GetConnection()
		if conn == nil {
			fmt.Printf("PeerHandler: Connection to peer %s lost, attempting reconnection\n",
				h.peer.PublicKey[:min(20, len(h.peer.PublicKey))])
			h.setState(PeerStateIdle)
			h.attemptConnection()
		} else {
			fmt.Printf("PeerHandler: Connection to peer %s is alive\n",
				h.peer.PublicKey[:min(20, len(h.peer.PublicKey))])
		}

	default:
		fmt.Printf("PeerHandler: No action needed for peer %s in state %s\n",
			h.peer.PublicKey[:min(20, len(h.peer.PublicKey))], state)
	}
}
