package network

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"fmt"

	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/multiformats/go-multiaddr"

	"btc-gateway/internal/types"
)

// HostWrapper wraps libp2p host with additional functionality
type HostWrapper struct {
	host   host.Host
	config *types.NetworkConfig
}

// NewHostWrapper creates a new host wrapper with the given configuration
func NewHostWrapper(config *types.NetworkConfig, privateKey crypto.PrivKey) (*HostWrapper, error) {
	var opts []libp2p.Option

	// Set private key if provided
	if privateKey != nil {
		opts = append(opts, libp2p.Identity(privateKey))
	}

	// Enable TCP transport
	opts = append(opts, libp2p.Transport(tcp.NewTCPTransport))

	// Use default security with specific protocols
	opts = append(opts, libp2p.DefaultSecurity)

	// Enable default muxers
	opts = append(opts, libp2p.DefaultMuxers)

	// Configure Connection Manager for automatic connection pruning and deduplication
	// This ensures we maintain reasonable connection counts and prune duplicates
	connManager, err := connmgr.NewConnManager(
		1024, // Low watermark: minimum connections to maintain
		1152, // High watermark: when to start pruning connections
		connmgr.WithGracePeriod(10*time.Second),  // Grace period before pruning new connections
		connmgr.WithSilencePeriod(5*time.Second), // How often to check for pruning
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection manager: %w", err)
	}
	opts = append(opts, libp2p.ConnectionManager(connManager))

	// Configure Resource Manager with custom limits to enforce 1 connection per peer
	// Start with default scaling limits and modify them for our use case
	scalingLimits := rcmgr.DefaultLimits

	// Scale the limits based on system resources
	concreteLimits := scalingLimits.AutoScale()

	// Create partial configuration to override specific limits
	cfg := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			// Allow reasonable system-wide connection limits
			Conns:         rcmgr.LimitVal(768),
			ConnsInbound:  rcmgr.LimitVal(512),
			ConnsOutbound: rcmgr.LimitVal(512),
		},
		// CRITICAL: Limit connections per peer to 1
		PeerDefault: rcmgr.ResourceLimits{
			Conns:         rcmgr.LimitVal(1),
			ConnsInbound:  rcmgr.LimitVal(1),
			ConnsOutbound: rcmgr.LimitVal(1),
		},
	}

	// Build the final configuration by merging our overrides with defaults
	finalLimits := cfg.Build(concreteLimits)

	// Create the limiter and resource manager
	limiter := rcmgr.NewFixedLimiter(finalLimits)
	resourceManager, err := rcmgr.NewResourceManager(limiter)
	if err != nil {
		return nil, fmt.Errorf("failed to create resource manager: %w", err)
	}
	opts = append(opts, libp2p.ResourceManager(resourceManager))

	// Parse and set listen addresses
	var listenAddrs []multiaddr.Multiaddr
	for _, addrStr := range config.Addresses {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return nil, err
		}
		listenAddrs = append(listenAddrs, addr)
	}

	if len(listenAddrs) > 0 {
		opts = append(opts, libp2p.ListenAddrs(listenAddrs...))
	}

	// Enable debug logging for connection issues
	// This will help us see what's happening at the libp2p level
	fmt.Printf("LIBP2P HOST: Creating host with connection and resource management\n")
	fmt.Printf("LIBP2P HOST: Connection Manager - Low: 10, High: 50, Grace: 1m\n")
	fmt.Printf("LIBP2P HOST: Resource Manager - Max connections per peer: 1\n")
	fmt.Printf("LIBP2P HOST: Listen addresses: %v\n", config.Addresses)

	// Create libp2p host with connection management configuration
	h, err := libp2p.New(opts...)
	if err != nil {
		return nil, err
	}

	fmt.Printf("LIBP2P HOST: Created host with ID: %s\n", h.ID().String())
	fmt.Printf("LIBP2P HOST: Host listening on: %v\n", h.Addrs())

	return &HostWrapper{
		host:   h,
		config: config,
	}, nil
}

// Host returns the underlying libp2p host
func (hw *HostWrapper) Host() host.Host {
	return hw.host
}

// Close closes the host
func (hw *HostWrapper) Close() error {
	return hw.host.Close()
}

// Connect connects to a peer at the given address
func (hw *HostWrapper) Connect(ctx context.Context, addr multiaddr.Multiaddr) error {
	addrInfo, err := parseMultiaddr(addr)
	if err != nil {
		return err
	}

	return hw.host.Connect(ctx, *addrInfo)
}

// parseMultiaddr parses a multiaddr and extracts peer info
func parseMultiaddr(addr multiaddr.Multiaddr) (*peer.AddrInfo, error) {
	// Try to extract peer ID from the address first
	addrInfo, err := peer.AddrInfoFromP2pAddr(addr)
	if err == nil {
		return addrInfo, nil
	}

	// If no peer ID in address, return without peer ID
	// The caller will need to set the peer ID separately
	return &peer.AddrInfo{
		Addrs: []multiaddr.Multiaddr{addr},
	}, nil
}

// ConvertPrivateKeyFromBase64 converts a base64 Ed25519 private key (from KeyManager)
// to a libp2p crypto.PrivKey for use with libp2p
func ConvertPrivateKeyFromBase64(privateKeyBase64 string) (crypto.PrivKey, error) {
	if privateKeyBase64 == "" {
		return nil, fmt.Errorf("private key cannot be empty")
	}

	keyBytes, err := base64.StdEncoding.DecodeString(privateKeyBase64)
	if err != nil {
		return nil, fmt.Errorf("invalid private key base64: %w", err)
	}

	if len(keyBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid private key length: expected %d, got %d", ed25519.PrivateKeySize, len(keyBytes))
	}

	ed25519Key := ed25519.PrivateKey(keyBytes)
	libp2pKey, err := crypto.UnmarshalEd25519PrivateKey(ed25519Key)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal Ed25519 private key: %w", err)
	}

	return libp2pKey, nil
}
