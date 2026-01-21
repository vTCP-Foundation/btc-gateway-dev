package config

import (
	"encoding/base64"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"btc-gateway/internal/keys"
	"btc-gateway/internal/types"
)

// Manager handles configuration loading, validation, and management
type Manager struct {
	keyManager *keys.KeyManager
}

// NewManager creates a new configuration manager with dependencies
func NewManager(keyManager *keys.KeyManager) *Manager {
	return &Manager{
		keyManager: keyManager,
	}
}

// LoadConfig loads configuration from the specified file path
func (m *Manager) LoadConfig(filePath string) (*types.Config, error) {
	// Check if file exists
	if _, err := os.Stat(filePath); os.IsNotExist(err) {
		// Create default config file
		cfg := types.DefaultConfig()
		if err := m.CreateConfigFile(filePath, cfg); err != nil {
			return nil, fmt.Errorf("failed to create default config file: %w", err)
		}
		// TODO: Log that default configuration file was created
	}

	// Read configuration file
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", filePath, err)
	}

	// Parse YAML
	var cfg types.Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("failed to parse YAML config: %w", err)
	}

	// Generate private key if empty
	if cfg.Node.PrivateKey == "" {
		privateKey, err := m.keyManager.GeneratePrivateKey()
		if err != nil {
			return nil, fmt.Errorf("failed to generate private key: %w", err)
		}
		cfg.Node.PrivateKey = privateKey

		// Save updated config with generated key
		if err := m.SaveConfig(filePath, &cfg); err != nil {
			return nil, fmt.Errorf("failed to save config with generated private key: %w", err)
		}
		// TODO: Log that new private key was generated and saved
	}

	// Validate configuration
	if err := m.ValidateConfig(&cfg); err != nil {
		return nil, fmt.Errorf("configuration validation failed: %w", err)
	}

	return &cfg, nil
}

// CreateConfigFile creates a new configuration file with the given config
func (m *Manager) CreateConfigFile(filePath string, cfg *types.Config) error {
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("failed to marshal config to YAML: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

// SaveConfig saves the configuration to the specified file
func (m *Manager) SaveConfig(filePath string, cfg *types.Config) error {
	return m.CreateConfigFile(filePath, cfg)
}

// ValidateConfig validates the configuration structure and values
func (m *Manager) ValidateConfig(cfg *types.Config) error {
	if cfg == nil {
		return fmt.Errorf("configuration is nil")
	}

	// Validate node configuration
	if err := m.validateNodeConfig(&cfg.Node); err != nil {
		return fmt.Errorf("node config validation failed: %w", err)
	}

	// Validate network configuration
	if err := validateNetworkConfig(&cfg.Network); err != nil {
		return fmt.Errorf("network config validation failed: %w", err)
	}

	// Validate peers configuration
	if err := validatePeersConfig(&cfg.Peers); err != nil {
		return fmt.Errorf("peers config validation failed: %w", err)
	}

	// Validate logging configuration
	if err := validateLoggingConfig(&cfg.Logging); err != nil {
		return fmt.Errorf("logging config validation failed: %w", err)
	}

	// Validate validators configuration
	if err := m.validateValidatorsConfig(cfg.Validators, cfg.Node.ID); err != nil {
		return fmt.Errorf("validators config validation failed: %w", err)
	}

	return nil
}

func (m *Manager) validateNodeConfig(cfg *types.NodeConfig) error {
	return m.keyManager.ValidatePrivateKey(cfg.PrivateKey)
}

func validateNetworkConfig(cfg *types.NetworkConfig) error {
	if len(cfg.Addresses) == 0 {
		return fmt.Errorf("network.addresses cannot be empty")
	}

	for i, addr := range cfg.Addresses {
		if err := validateMultiaddr(addr); err != nil {
			return fmt.Errorf("invalid address at index %d: %w", i, err)
		}
	}

	return nil
}

func validatePeersConfig(cfg *types.PeersConfig) error {
	if cfg.ConnectionTimeout < time.Second {
		return fmt.Errorf("peers.connection_timeout must be at least 1 second")
	}

	return nil
}

func validateLoggingConfig(cfg *types.LoggingConfig) error {
	validLevels := map[string]bool{
		"debug": true, "info": true, "warn": true, "error": true,
	}
	if !validLevels[cfg.Level] {
		return fmt.Errorf("logging.level must be one of: debug, info, warn, error")
	}

	validFormats := map[string]bool{
		"json": true, "text": true,
	}
	if !validFormats[cfg.Format] {
		return fmt.Errorf("logging.format must be one of: json, text")
	}

	return nil
}

func (m *Manager) validateValidatorsConfig(validators []types.ValidatorConfig, nodeID uint16) error {
	if len(validators) == 0 {
		return fmt.Errorf("validators list cannot be empty")
	}

	// Check for duplicate IDs
	seenIDs := make(map[uint16]bool)
	foundSelf := false

	for i, v := range validators {
		if seenIDs[v.ID] {
			return fmt.Errorf("duplicate validator ID %d", v.ID)
		}
		seenIDs[v.ID] = true

		if v.ID == nodeID {
			foundSelf = true
		}

		// Validate public key
		if !isValidBase64(v.PublicKey) {
			return fmt.Errorf("validator %d has invalid public key (not base64)", v.ID)
		}

		// Validate address
		if err := validateMultiaddr(v.Address); err != nil {
			return fmt.Errorf("validator %d at index %d: %w", v.ID, i, err)
		}
	}

	if !foundSelf {
		return fmt.Errorf("node ID %d not found in validators list", nodeID)
	}

	return nil
}

// isValidBase64 checks if a string is valid base64
func isValidBase64(s string) bool {
	_, err := base64.StdEncoding.DecodeString(s)
	return err == nil
}

// validateMultiaddr validates a multiaddr string format
func validateMultiaddr(addr string) error {
	if addr == "" {
		return fmt.Errorf("address cannot be empty")
	}

	if !strings.HasPrefix(addr, "/") {
		return fmt.Errorf("multiaddr must start with '/'")
	}

	// Basic validation for common patterns
	// This is a simplified validation - in production you'd use libp2p's multiaddr parser
	patterns := []string{
		// TCP patterns
		`^/ip4/[0-9.]+/tcp/[0-9]+$`,
		`^/ip6/.*/tcp/[0-9]+$`,
		`^/dns4/.+/tcp/[0-9]+$`,
		`^/dns6/.+/tcp/[0-9]+$`,
		// QUIC patterns (for future use)
		`^/ip4/[0-9.]+/udp/[0-9]+/quic$`,
		`^/ip6/.*/udp/[0-9]+/quic$`,
		`^/dns4/.+/udp/[0-9]+/quic$`,
		`^/dns6/.+/udp/[0-9]+/quic$`,
	}

	for _, pattern := range patterns {
		if matched, _ := regexp.MatchString(pattern, addr); matched {
			return nil
		}
	}

	return fmt.Errorf("unsupported multiaddr format: %s", addr)
}

// LoadConfig is a convenience function that creates a manager and loads config
func LoadConfig(filePath string) (*types.Config, error) {
	keyManager := keys.NewKeyManager()
	configManager := NewManager(keyManager)
	return configManager.LoadConfig(filePath)
}
