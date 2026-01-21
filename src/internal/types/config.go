package types

import "time"

// Config represents the complete application configuration
type Config struct {
	Node       NodeConfig       `yaml:"node" validate:"required"`
	Network    NetworkConfig    `yaml:"network" validate:"required"`
	Peers      PeersConfig      `yaml:"peers" validate:"required"`
	Logging    LoggingConfig    `yaml:"logging" validate:"required"`
	Consensus  ConsensusConfig  `yaml:"consensus" validate:"required"`
	Validators []ValidatorConfig `yaml:"validators" validate:"required,min=1"`
	Storage    StorageConfig    `yaml:"storage"`
}

// NodeConfig contains node-specific configuration
type NodeConfig struct {
	ID         uint16 `yaml:"id" validate:"required"`
	PrivateKey string `yaml:"private_key" validate:"omitempty,base64"`
}

// ValidatorConfig describes a validator node in the network
type ValidatorConfig struct {
	ID        uint16 `yaml:"id" validate:"required"`
	PublicKey string `yaml:"public_key" validate:"required,base64"`
	Address   string `yaml:"address" validate:"required"` // multiaddr format
}

// ConsensusConfig contains consensus-related configuration
type ConsensusConfig struct {
	ViewTimeout     time.Duration `yaml:"view_timeout"`
	MaxBlockSize    int           `yaml:"max_block_size"`
	ProposalTimeout time.Duration `yaml:"proposal_timeout"`
}

// StorageConfig contains TiKV storage configuration
type StorageConfig struct {
	PDAddrs   []string `yaml:"pd_addrs"`
	KeyPrefix string   `yaml:"key_prefix"`
}

// NetworkConfig contains network-related configuration
type NetworkConfig struct {
	Addresses []string `yaml:"addresses" validate:"required,min=1,dive,required"`
}

// PeersConfig contains peer management configuration
type PeersConfig struct {
	ConnectionTimeout time.Duration `yaml:"connection_timeout" validate:"required,min=1s"`
}

// LoggingConfig contains logging configuration
type LoggingConfig struct {
	Level  string `yaml:"level" validate:"required,oneof=debug info warn error"`
	Format string `yaml:"format" validate:"required,oneof=json text"`
}

// DefaultConfig returns a configuration with sensible defaults
func DefaultConfig() *Config {
	return &Config{
		Node: NodeConfig{
			ID:         0,
			PrivateKey: "", // Will be generated if empty
		},
		Network: NetworkConfig{
			Addresses: []string{
				"/ip4/0.0.0.0/tcp/4000",
			},
		},
		Peers: PeersConfig{
			ConnectionTimeout: 10 * time.Second,
		},
		Logging: LoggingConfig{
			Level:  "info",
			Format: "text",
		},
		Consensus: ConsensusConfig{
			ViewTimeout:     5 * time.Second,
			MaxBlockSize:    128 * 1024 * 1024, // 128 MB
			ProposalTimeout: 2 * time.Second,
		},
		Validators: []ValidatorConfig{},
		Storage: StorageConfig{
			PDAddrs:   []string{"127.0.0.1:2379"},
			KeyPrefix: "kvstore:",
		},
	}
}

// GetValidatorByID returns the validator config for a given node ID
func (c *Config) GetValidatorByID(id uint16) *ValidatorConfig {
	for i := range c.Validators {
		if c.Validators[i].ID == id {
			return &c.Validators[i]
		}
	}
	return nil
}

// GetNodeIDs returns all validator node IDs
func (c *Config) GetNodeIDs() []uint16 {
	ids := make([]uint16, len(c.Validators))
	for i, v := range c.Validators {
		ids[i] = v.ID
	}
	return ids
}
