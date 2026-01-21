// Package main provides a tool for generating validator keys and cluster configuration.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"

	"btc-gateway/internal/keys"
	"btc-gateway/internal/types"
)

var (
	nodeCount  = flag.Int("nodes", 5, "Number of validator nodes")
	outputDir  = flag.String("output", "./configs", "Output directory for config files")
	basePort   = flag.Int("base-port", 4000, "Base libp2p port (each node gets base-port + node_id)")
	baseAPIPort = flag.Int("base-api-port", 8080, "Base API port (each node gets base-api-port + node_id)")
	hostPrefix = flag.String("host-prefix", "kvnode", "Host prefix for docker containers")
	pdPrefix   = flag.String("pd-prefix", "pd", "PD prefix for docker containers")
)

func main() {
	flag.Parse()

	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	km := keys.NewKeyManager()

	// Create output directory
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate keys for all nodes
	type nodeKeys struct {
		ID         uint16
		PrivateKey string
		PublicKey  string
		PeerID     string
		Address    string
	}

	allNodes := make([]nodeKeys, *nodeCount)

	fmt.Printf("Generating keys for %d nodes...\n", *nodeCount)

	for i := 0; i < *nodeCount; i++ {
		privKey, err := km.GeneratePrivateKey()
		if err != nil {
			return fmt.Errorf("failed to generate key for node %d: %w", i, err)
		}

		pubKey, err := km.GetPublicKey(privKey)
		if err != nil {
			return fmt.Errorf("failed to get public key for node %d: %w", i, err)
		}

		peerID, err := km.PrivateKeyToPeerID(privKey)
		if err != nil {
			return fmt.Errorf("failed to get peer ID for node %d: %w", i, err)
		}

		// Address format for docker network
		address := fmt.Sprintf("/dns4/%s%d/tcp/%d", *hostPrefix, i, *basePort)

		allNodes[i] = nodeKeys{
			ID:         uint16(i),
			PrivateKey: privKey,
			PublicKey:  pubKey,
			PeerID:     peerID.String(),
			Address:    address,
		}

		fmt.Printf("  Node %d: PeerID=%s\n", i, peerID.String())
	}

	// Create validator config (shared across all nodes)
	validators := make([]types.ValidatorConfig, *nodeCount)
	for i, node := range allNodes {
		validators[i] = types.ValidatorConfig{
			ID:        node.ID,
			PublicKey: node.PublicKey,
			Address:   node.Address,
		}
	}

	// Generate config file for each node
	for i, node := range allNodes {
		cfg := &types.Config{
			Node: types.NodeConfig{
				ID:         node.ID,
				PrivateKey: node.PrivateKey,
			},
			Network: types.NetworkConfig{
				Addresses: []string{
					fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", *basePort),
				},
			},
			Peers: types.PeersConfig{
				ConnectionTimeout: 10 * time.Second,
			},
			Logging: types.LoggingConfig{
				Level:  "info",
				Format: "text",
			},
			Consensus: types.ConsensusConfig{
				ViewTimeout:     5 * time.Second,
				MaxBlockSize:    128 * 1024 * 1024,
				ProposalTimeout: 2 * time.Second,
			},
			Validators: validators,
			Storage: types.StorageConfig{
				PDAddrs:   []string{fmt.Sprintf("%s%d:2379", *pdPrefix, i)},
				KeyPrefix: fmt.Sprintf("kvstore:node%d:", i),
			},
		}

		// Write config file
		configPath := filepath.Join(*outputDir, fmt.Sprintf("node%d.yaml", i))
		data, err := yaml.Marshal(cfg)
		if err != nil {
			return fmt.Errorf("failed to marshal config for node %d: %w", i, err)
		}

		if err := os.WriteFile(configPath, data, 0644); err != nil {
			return fmt.Errorf("failed to write config for node %d: %w", i, err)
		}

		fmt.Printf("  Written: %s\n", configPath)
	}

	// Also write a summary file with all peer IDs
	summaryPath := filepath.Join(*outputDir, "cluster-info.txt")
	summary := "Cluster Information\n"
	summary += "==================\n\n"
	for _, node := range allNodes {
		summary += fmt.Sprintf("Node %d:\n", node.ID)
		summary += fmt.Sprintf("  Peer ID:    %s\n", node.PeerID)
		summary += fmt.Sprintf("  Address:    %s\n", node.Address)
		summary += fmt.Sprintf("  Public Key: %s\n\n", node.PublicKey)
	}

	if err := os.WriteFile(summaryPath, []byte(summary), 0644); err != nil {
		return fmt.Errorf("failed to write summary: %w", err)
	}

	fmt.Printf("\nCluster configuration generated successfully!\n")
	fmt.Printf("Config files written to: %s\n", *outputDir)

	return nil
}
