// Package main provides the entry point for the KV Store node.
// This node participates in HotStuff BFT consensus and executes
// key-value transactions committed by the consensus layer.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	accounttikv "btc-gateway/internal/account/tikv"
	"btc-gateway/internal/api"
	"btc-gateway/internal/builder"
	"btc-gateway/internal/config"
	"btc-gateway/internal/crypto/ed25519"
	"btc-gateway/internal/gossip"
	"btc-gateway/internal/keys"
	"btc-gateway/internal/mempool"
	"btc-gateway/internal/network/adapter"
	"btc-gateway/internal/service"
	"btc-gateway/internal/statemachine"
	"btc-gateway/internal/storage/tikv"
	inttypes "btc-gateway/internal/types"
	"btc-gateway/internal/validation"
	validationtikv "btc-gateway/internal/validation/tikv"
	"btc-gateway/pkg/consensus/integration"
	"btc-gateway/pkg/consensus/mocks"
	"btc-gateway/pkg/consensus/types"
)

// Command line flags
var (
	configFile = flag.String("config", "", "Path to configuration file (required)")
	apiPort    = flag.Int("api-port", 8080, "REST API listen port (overrides config)")
)

func main() {
	flag.Parse()

	if *configFile == "" {
		fmt.Fprintln(os.Stderr, "Error: -config flag is required")
		flag.Usage()
		os.Exit(1)
	}

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error loading config: %v\n", err)
		os.Exit(1)
	}

	// Configure logging
	setupLogging(cfg.Logging.Level)

	log.Info().
		Uint16("node_id", cfg.Node.ID).
		Int("validators", len(cfg.Validators)).
		Strs("pd_addrs", cfg.Storage.PDAddrs).
		Msg("Starting KV Store Node")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Info().Msg("Received shutdown signal")
		cancel()
	}()

	if err := run(ctx, cfg); err != nil {
		log.Fatal().Err(err).Msg("Node failed")
	}
}

func run(ctx context.Context, cfg *inttypes.Config) error {
	km := keys.NewKeyManager()

	// 1. Initialize TikV storage
	tikvConfig := tikv.DefaultConfig()
	if len(cfg.Storage.PDAddrs) > 0 {
		tikvConfig.PDAddrs = cfg.Storage.PDAddrs
	}
	if cfg.Storage.KeyPrefix != "" {
		tikvConfig.KeyPrefix = cfg.Storage.KeyPrefix
	}

	storage := tikv.NewStorage(tikvConfig)
	if err := storage.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to TikV: %w", err)
	}
	defer storage.Close()
	log.Info().Msg("Connected to TikV")

	// 2. Create CryptoProvider (ed25519)
	cryptoProvider := ed25519.NewProvider()
	log.Info().Msg("Initialized Ed25519 crypto provider")

	// 3. Create AccountStore (tikv)
	accountStore := accounttikv.NewStore(storage.GetClient(), tikvConfig.KeyPrefix)
	log.Info().Msg("Initialized account store")

	// 4. Create CommittedTxStore (tikv)
	committedStore := validationtikv.NewCommittedStore(storage.GetClient(), tikvConfig.KeyPrefix)
	log.Info().Msg("Initialized committed transaction store")

	// 5. Create Pool (in-memory FIFO)
	pool := mempool.NewFIFOPool()
	log.Info().Msg("Initialized mempool")

	// 6. Create TxValidator
	txValidator := validation.NewTxValidator(cryptoProvider, accountStore, committedStore, pool)
	log.Info().Msg("Initialized transaction validator")

	// 7. Create TxSubmitService (broadcaster=nil initially)
	txSubmitService := service.NewTxSubmitService(txValidator, pool)
	log.Info().Msg("Initialized transaction submit service")

	// Create state machine executor
	executor := statemachine.NewExecutor(storage)

	// Set up executor with committed store and pool
	executor.SetCommittedStore(committedStore)
	executor.SetPool(pool)

	// Set account store on state machine
	executor.GetStateMachine().SetAccountStore(accountStore)

	if err := executor.Start(); err != nil {
		return fmt.Errorf("failed to start executor: %w", err)
	}
	defer executor.Stop()
	log.Info().Msg("Started state machine executor")

	// Create event tracer that forwards to executor
	eventTracer := statemachine.NewExecutorEventTracer(executor)

	// Create libp2p host with deterministic identity from config
	privKey, err := km.ToLibp2pPrivateKey(cfg.Node.PrivateKey)
	if err != nil {
		return fmt.Errorf("failed to parse private key: %w", err)
	}

	// Parse listen addresses from config
	listenAddrs := make([]multiaddr.Multiaddr, len(cfg.Network.Addresses))
	for i, addrStr := range cfg.Network.Addresses {
		addr, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			return fmt.Errorf("failed to parse listen address %s: %w", addrStr, err)
		}
		listenAddrs[i] = addr
	}

	host, err := libp2p.New(
		libp2p.Identity(privKey),
		libp2p.ListenAddrs(listenAddrs...),
	)
	if err != nil {
		return fmt.Errorf("failed to create libp2p host: %w", err)
	}
	defer host.Close()

	log.Info().
		Str("peer_id", host.ID().String()).
		Strs("addrs", multiaddrsToStrings(host.Addrs())).
		Msg("libp2p host started")

	// 8. Create GossipSubBroadcaster + GossipHandler
	gossipBroadcaster, err := gossip.NewGossipSubBroadcaster(ctx, host)
	if err != nil {
		return fmt.Errorf("failed to create gossip broadcaster: %w", err)
	}
	defer gossipBroadcaster.Close()

	gossipHandler, err := gossip.NewHandler(ctx, txSubmitService, gossipBroadcaster.GetTopic())
	if err != nil {
		return fmt.Errorf("failed to create gossip handler: %w", err)
	}
	gossipHandler.Start()
	defer gossipHandler.Stop()
	log.Info().Msg("Started gossip layer")

	// 9. Set broadcaster on TxSubmitService
	txSubmitService.SetBroadcaster(gossipBroadcaster)

	// Create consensus network adapter
	nodeID := types.NodeID(cfg.Node.ID)
	networkAdapter := adapter.NewConsensusNetworkAdapter(host, nodeID)
	if err := networkAdapter.Start(); err != nil {
		return fmt.Errorf("failed to start network adapter: %w", err)
	}
	defer networkAdapter.Stop()

	// Register all validators in the network adapter
	for _, validator := range cfg.Validators {
		peerID, err := km.PublicKeyToPeerID(validator.PublicKey)
		if err != nil {
			return fmt.Errorf("failed to derive peer ID for validator %d: %w", validator.ID, err)
		}

		networkAdapter.RegisterNode(types.NodeID(validator.ID), peerID)
		log.Debug().
			Uint16("validator_id", validator.ID).
			Str("peer_id", peerID.String()).
			Str("address", validator.Address).
			Msg("Registered validator")
	}
	log.Info().Int("count", len(cfg.Validators)).Msg("Registered all validators")

	// Connect to all other validators (with retry) - BLOCKING
	// We need a quorum of connections before starting consensus
	expectedPeers := len(cfg.Validators) - 1
	quorum := (expectedPeers / 2) + 1 // Need majority connected

	for attempt := 0; attempt < 12; attempt++ {
		connectToValidators(ctx, host, cfg, km)

		// Check connection count
		connectedCount := 0
		for _, validator := range cfg.Validators {
			if validator.ID == cfg.Node.ID {
				continue
			}
			peerID, _ := km.PublicKeyToPeerID(validator.PublicKey)
			conns := host.Network().ConnsToPeer(peerID)
			if len(conns) > 0 {
				connectedCount++
			}
		}

		if connectedCount >= expectedPeers {
			log.Info().Int("connected", connectedCount).Msg("All validators connected")
			break
		}

		if connectedCount >= quorum {
			log.Info().
				Int("connected", connectedCount).
				Int("quorum", quorum).
				Msg("Quorum of validators connected, proceeding")
			break
		}

		log.Info().
			Int("connected", connectedCount).
			Int("quorum", quorum).
			Int("attempt", attempt+1).
			Msg("Waiting for validator connections")

		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting for connections")
		case <-time.After(2 * time.Second):
		}
	}

	// Create consensus configuration
	publicKeys := make([]types.PublicKey, len(cfg.Validators))
	for i, v := range cfg.Validators {
		publicKeys[i] = []byte(v.PublicKey)
	}

	consensusConfig, err := types.NewConsensusConfig(publicKeys)
	if err != nil {
		return fmt.Errorf("failed to create consensus config: %w", err)
	}

	// Create validators list
	validators := make([]types.NodeID, len(cfg.Validators))
	for i, v := range cfg.Validators {
		validators[i] = types.NodeID(v.ID)
	}

	// Create deterministic genesis block (must be same across all nodes)
	genesisBlock := types.NewGenesisBlock()

	// Store genesis block in TiKV if not already present
	if _, err := storage.GetBlock(genesisBlock.Hash); err != nil {
		if err := storage.StoreBlock(genesisBlock); err != nil {
			return fmt.Errorf("failed to store genesis block: %w", err)
		}
		log.Info().
			Hex("hash", genesisBlock.Hash[:]).
			Msg("Stored genesis block")
	}

	// Create mock crypto for consensus (production would use real crypto)
	mockCrypto := mocks.NewMockCrypto(
		nodeID,
		mocks.DefaultCryptoConfig(),
		mocks.DefaultCryptoFailureConfig(),
	)
	for _, v := range validators {
		mockCrypto.AddNode(v)
	}

	// Create and start consensus node
	nodeConfig := &integration.NodeConfig{
		NodeID:       nodeID,
		Validators:   validators,
		Config:       consensusConfig,
		EventTracer:  eventTracer,
		GenesisBlock: genesisBlock,
	}

	// Use TikV storage for consensus (implements StorageInterface)
	node, err := integration.NewNode(nodeConfig, networkAdapter, storage, mockCrypto)
	if err != nil {
		return fmt.Errorf("failed to create consensus node: %w", err)
	}

	if err := node.Start(); err != nil {
		return fmt.Errorf("failed to start consensus node: %w", err)
	}
	defer node.Stop()

	// Start message processor to route incoming network messages to coordinator
	msgProcessor := integration.NewMessageProcessor(
		nodeID,
		node.GetCoordinator(),
		networkAdapter,
		log.Logger,
	)
	msgProcessor.EnableLogging(true)
	msgProcessor.Start()
	defer msgProcessor.Stop()

	log.Info().
		Uint16("node_id", cfg.Node.ID).
		Msg("Consensus node started")

	// 10. Create NodeProposer
	nodeProposer := builder.NewNodeProposer(node, consensusConfig, nodeID)

	// 11. Create BlockBuildService
	blockBuildService := builder.NewBlockBuildService(pool, nodeProposer, accountStore)

	// 12. Create LeadershipHandler
	leadershipHandler := builder.NewLeadershipHandler(blockBuildService, nodeProposer, nodeID)

	// 13. Set leadership handler on executor
	executor.SetLeadershipHandler(leadershipHandler)
	log.Info().Msg("Initialized block builder")

	// 14. Create AccountQueryService
	accountQueryService := service.NewAccountQueryService(accountStore, cryptoProvider)

	// Determine API port
	apiPortToUse := *apiPort

	// 15. Start REST API server with new services
	apiHandler := api.NewHandler(executor.GetStateMachine(), node, consensusConfig, nodeID)
	apiHandler.SetAccountQueryService(accountQueryService)
	apiHandler.SetTxSubmitService(txSubmitService)

	mux := http.NewServeMux()
	apiHandler.SetupRoutes(mux)

	apiServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", apiPortToUse),
		Handler: mux,
	}

	go func() {
		log.Info().Int("port", apiPortToUse).Msg("Starting REST API server")
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error().Err(err).Msg("API server error")
		}
	}()
	defer apiServer.Shutdown(context.Background())

	// Wait for shutdown
	<-ctx.Done()
	log.Info().Msg("Shutting down...")

	// Give components time to gracefully shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	<-shutdownCtx.Done()

	return nil
}

func setupLogging(level string) {
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix

	// Parse log level
	lvl, err := zerolog.ParseLevel(level)
	if err != nil {
		lvl = zerolog.InfoLevel
	}
	zerolog.SetGlobalLevel(lvl)

	// Pretty console output for development
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
}

func multiaddrsToStrings(addrs []multiaddr.Multiaddr) []string {
	result := make([]string, len(addrs))
	for i, addr := range addrs {
		result[i] = addr.String()
	}
	return result
}

// connectToValidators connects to all other validators in the network
func connectToValidators(ctx context.Context, h host.Host, cfg *inttypes.Config, km *keys.KeyManager) error {
	var lastErr error

	for _, validator := range cfg.Validators {
		// Skip self
		if validator.ID == cfg.Node.ID {
			continue
		}

		// Parse the validator's multiaddr
		maddr, err := multiaddr.NewMultiaddr(validator.Address)
		if err != nil {
			log.Warn().
				Uint16("validator_id", validator.ID).
				Str("address", validator.Address).
				Err(err).
				Msg("Failed to parse validator address")
			lastErr = err
			continue
		}

		// Get peer ID from public key
		peerID, err := km.PublicKeyToPeerID(validator.PublicKey)
		if err != nil {
			log.Warn().
				Uint16("validator_id", validator.ID).
				Err(err).
				Msg("Failed to derive peer ID")
			lastErr = err
			continue
		}

		// Always add peer address to peerstore so libp2p can connect later
		// This is important because if the initial connection fails, we still
		// want libp2p to be able to connect when we try to send a message
		h.Peerstore().AddAddrs(peerID, []multiaddr.Multiaddr{maddr}, time.Hour*24)

		// Create AddrInfo
		addrInfo := peer.AddrInfo{
			ID:    peerID,
			Addrs: []multiaddr.Multiaddr{maddr},
		}

		// Connect with timeout
		connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err = h.Connect(connectCtx, addrInfo)
		cancel()

		if err != nil {
			log.Warn().
				Uint16("validator_id", validator.ID).
				Str("peer_id", peerID.String()).
				Err(err).
				Msg("Failed to connect to validator (will retry on demand)")
			lastErr = err
			continue
		}

		log.Info().
			Uint16("validator_id", validator.ID).
			Str("peer_id", peerID.String()).
			Msg("Connected to validator")
	}

	return lastErr
}
