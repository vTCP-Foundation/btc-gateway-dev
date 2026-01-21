package network

import (
	"fmt"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
)

// networkNotifiee implements the libp2p network.Notifiee interface
// to handle connection events and update the manager's state
type networkNotifiee struct {
	manager *Manager
}

// Connected is called when a connection is established
func (n *networkNotifiee) Connected(network network.Network, conn network.Conn) {
	peerID := conn.RemotePeer()
	remoteAddr := conn.RemoteMultiaddr()
	direction := conn.Stat().Direction

	fmt.Printf("NetworkManager: Connection established with peer %s from %s (direction: %s)\n",
		peerID, remoteAddr, direction)

	// Add detailed timing and libp2p connection debugging
	fmt.Printf("CONNECT DEBUG: Connection opened at %v, processing at %v (delay: %v)\n",
		conn.Stat().Opened, time.Now(), time.Since(conn.Stat().Opened))

	// Debug: Show all connections to this peer from libp2p's perspective
	allConnsToPeer := n.manager.hostWrapper.Host().Network().ConnsToPeer(peerID)
	fmt.Printf("LIBP2P DEBUG: Total connections to peer %s: %d\n", peerID, len(allConnsToPeer))
	for i, c := range allConnsToPeer {
		fmt.Printf("LIBP2P DEBUG: Connection %d: opened=%v, direction=%s\n",
			i, c.Stat().Opened, c.Stat().Direction)
	}

	// Check for and handle redundant connections
	if n.manager.handleConnectionDeduplication(conn) {
		// Connection was closed due to deduplication, don't proceed
		return
	}

	// Update connection state
	n.manager.updateConnectionState(peerID, StateConnected, remoteAddr)

	// Immediately update any relevant peer handler state
	n.manager.handleIncomingConnection(peerID, conn)

	// Emit connection event to handlers
	n.manager.emitConnectionEvent(EventConnected, peerID, remoteAddr, nil)

	fmt.Printf("NetworkManager: Successfully established connection with peer %s\n", peerID.String())
}

// Disconnected is called when a connection is closed
func (n *networkNotifiee) Disconnected(network network.Network, conn network.Conn) {
	peerID := conn.RemotePeer()
	addr := conn.RemoteMultiaddr()

	fmt.Printf("NetworkManager: Connection closed with peer %s (was at %s)\n",
		peerID.String(), addr.String())

	// Add debug info about the connection that was closed
	fmt.Printf("DISCONNECT DEBUG: Connection details - Direction: %s, Opened: %v, Duration: %v\n",
		conn.Stat().Direction, conn.Stat().Opened, time.Since(conn.Stat().Opened))

	// Check how many connections we still have to this peer
	remainingConns := n.manager.hostWrapper.Host().Network().ConnsToPeer(peerID)
	fmt.Printf("DISCONNECT DEBUG: Remaining connections to peer %s: %d\n",
		peerID.String(), len(remainingConns))

	// Update connection state
	n.manager.updateConnectionState(peerID, StateDisconnecting, addr)

	// Emit disconnection event to handlers
	n.manager.emitConnectionEvent(EventDisconnected, peerID, addr, nil)

	// Remove from connections map after a brief delay to allow handlers to process
	go func() {
		n.manager.connectionsMutex.Lock()
		delete(n.manager.connections, peerID)
		n.manager.connectionsMutex.Unlock()
	}()
}

// Listen is called when the network starts listening on an address
func (n *networkNotifiee) Listen(network network.Network, addr multiaddr.Multiaddr) {
	// TODO: Log that we're listening on this address
	// fmt.Printf("Started listening on: %s\n", addr.String())
}

// ListenClose is called when the network stops listening on an address
func (n *networkNotifiee) ListenClose(network network.Network, addr multiaddr.Multiaddr) {
	// TODO: Log that we stopped listening on this address
	// fmt.Printf("Stopped listening on: %s\n", addr.String())
}
