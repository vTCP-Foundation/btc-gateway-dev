package network

import (
	"context"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"

	"btc-gateway/internal/keys"
	"btc-gateway/internal/types"
)

func TestNewManager(t *testing.T) {
	config := &types.Config{
		Node: types.NodeConfig{
			PrivateKey: "",
		},
		Network: types.NetworkConfig{
			Addresses: []string{
				"/ip4/127.0.0.1/tcp/0",
			},
		},
		Peers: types.PeersConfig{
			ConnectionTimeout: 10 * time.Second,
		},
	}

	// Test with empty private key
	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}
	if manager == nil {
		t.Fatal("Manager should not be nil")
	}

	// Test with generated private key
	keyManager := keys.NewKeyManager()
	privateKey, err := keyManager.GeneratePrivateKey()
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	configWithKey := &types.Config{
		Node: types.NodeConfig{
			PrivateKey: privateKey,
		},
		Network: types.NetworkConfig{
			Addresses: []string{
				"/ip4/127.0.0.1/tcp/0",
			},
		},
		Peers: types.PeersConfig{
			ConnectionTimeout: 10 * time.Second,
		},
	}

	manager2, err := NewManager(configWithKey)
	if err != nil {
		t.Fatalf("Failed to create manager with private key: %v", err)
	}
	if manager2 == nil {
		t.Fatal("Manager should not be nil")
	}
}

func TestManagerLifecycle(t *testing.T) {
	config := &types.Config{
		Node: types.NodeConfig{
			PrivateKey: "",
		},
		Network: types.NetworkConfig{
			Addresses: []string{
				"/ip4/127.0.0.1/tcp/0",
			},
		},
		Peers: types.PeersConfig{
			ConnectionTimeout: 10 * time.Second,
		},
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	// Test starting the manager
	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}

	// Test that we can't start twice
	if err := manager.Start(ctx); err == nil {
		t.Error("Expected error when starting already started manager")
	}

	// Test getting host
	host := manager.GetHost()
	if host == nil {
		t.Error("Host should not be nil after starting")
	}

	// Test getting connections (should be empty initially)
	connections := manager.GetConnections()
	if len(connections) != 0 {
		t.Errorf("Expected 0 connections, got %d", len(connections))
	}

	// Test stopping the manager
	if err := manager.Stop(); err != nil {
		t.Fatalf("Failed to stop manager: %v", err)
	}

	// Test that we can stop twice (should not error)
	if err := manager.Stop(); err != nil {
		t.Errorf("Unexpected error when stopping already stopped manager: %v", err)
	}
}

func TestConfigValidation(t *testing.T) {
	manager := &Manager{}

	// Test nil config
	if err := manager.ValidateConfig(nil); err == nil {
		t.Error("Expected error for nil config")
	}

	// Test empty addresses
	config := &types.NetworkConfig{
		Addresses: []string{},
	}
	if err := manager.ValidateConfig(config); err == nil {
		t.Error("Expected error for empty addresses")
	}

	// Test invalid address
	config = &types.NetworkConfig{
		Addresses: []string{"invalid-address"},
	}
	if err := manager.ValidateConfig(config); err == nil {
		t.Error("Expected error for invalid address")
	}

	// Test valid config with TCP
	config = &types.NetworkConfig{
		Addresses: []string{
			"/ip4/127.0.0.1/tcp/9000",
			"/ip6/::/tcp/9000",
		},
	}
	if err := manager.ValidateConfig(config); err != nil {
		t.Errorf("Unexpected error for valid config: %v", err)
	}
}

func TestProtocolHandling(t *testing.T) {
	config := &types.Config{
		Node: types.NodeConfig{
			PrivateKey: "",
		},
		Network: types.NetworkConfig{
			Addresses: []string{"/ip4/127.0.0.1/tcp/0"},
		},
		Peers: types.PeersConfig{
			ConnectionTimeout: 10 * time.Second,
		},
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	ctx := context.Background()
	if err := manager.Start(ctx); err != nil {
		t.Fatalf("Failed to start manager: %v", err)
	}
	defer manager.Stop()

	// Create a test protocol handler
	testProtocol := protocol.ID("/test/v1.0.0")
	handler := &testProtocolHandler{protocolID: testProtocol}

	// Test registering protocol handler
	if err := manager.RegisterProtocolHandler(testProtocol, handler); err != nil {
		t.Errorf("Failed to register protocol handler: %v", err)
	}

	// Test registering same handler twice (should error)
	if err := manager.RegisterProtocolHandler(testProtocol, handler); err == nil {
		t.Error("Expected error when registering same protocol twice")
	}

	// Test unregistering protocol handler
	if err := manager.UnregisterProtocolHandler(testProtocol); err != nil {
		t.Errorf("Failed to unregister protocol handler: %v", err)
	}

	// Test unregistering non-existent handler (should error)
	if err := manager.UnregisterProtocolHandler(testProtocol); err == nil {
		t.Error("Expected error when unregistering non-existent protocol")
	}
}

func TestConnectionEventHandlers(t *testing.T) {
	config := &types.Config{
		Node: types.NodeConfig{
			PrivateKey: "",
		},
		Network: types.NetworkConfig{
			Addresses: []string{"/ip4/127.0.0.1/tcp/0"},
		},
		Peers: types.PeersConfig{
			ConnectionTimeout: 10 * time.Second,
		},
	}

	manager, err := NewManager(config)
	if err != nil {
		t.Fatalf("Failed to create manager: %v", err)
	}

	handler := &testConnectionHandler{}

	// Test registering connection handler
	if err := manager.RegisterConnectionHandler(handler); err != nil {
		t.Errorf("Failed to register connection handler: %v", err)
	}

	// Test unregistering connection handler
	if err := manager.UnregisterConnectionHandler(handler); err != nil {
		t.Errorf("Failed to unregister connection handler: %v", err)
	}

	// Test unregistering non-existent handler (should error)
	if err := manager.UnregisterConnectionHandler(handler); err == nil {
		t.Error("Expected error when unregistering non-existent handler")
	}
}

func TestPrivateKeyConversion(t *testing.T) {
	// Test with empty key
	_, err := ConvertPrivateKeyFromBase64("")
	if err == nil {
		t.Error("Expected error for empty private key")
	}

	// Test with invalid base64
	_, err = ConvertPrivateKeyFromBase64("invalid-base64")
	if err == nil {
		t.Error("Expected error for invalid base64")
	}

	// Test with valid key from KeyManager
	keyManager := keys.NewKeyManager()
	privateKeyBase64, err := keyManager.GeneratePrivateKey()
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	libp2pKey, err := ConvertPrivateKeyFromBase64(privateKeyBase64)
	if err != nil {
		t.Errorf("Failed to convert valid private key: %v", err)
	}
	if libp2pKey == nil {
		t.Error("Converted key should not be nil")
	}
}

// Test helper types
type testProtocolHandler struct {
	protocolID protocol.ID
}

func (h *testProtocolHandler) HandleStream(stream network.Stream) error {
	// Mock implementation
	defer stream.Close()
	return nil
}

func (h *testProtocolHandler) ProtocolID() protocol.ID {
	return h.protocolID
}

type testConnectionHandler struct {
	connectedCalls    int
	disconnectedCalls int
}

func (h *testConnectionHandler) OnPeerConnected(peerID peer.ID, addr multiaddr.Multiaddr) error {
	h.connectedCalls++
	return nil
}

func (h *testConnectionHandler) OnPeerDisconnected(peerID peer.ID, reason error) error {
	h.disconnectedCalls++
	return nil
}
