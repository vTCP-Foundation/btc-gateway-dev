package types

import (
	"fmt"
	"math"
	"time"
)

// PublicKey represents a public key as a byte array.
// In a real implementation, this would be replaced with a proper cryptographic type.
type PublicKey []byte

// ConsensusConfig defines the static configuration for the consensus protocol.
// It contains the mapping of NodeIDs to their public keys and timeout settings.
type ConsensusConfig struct {
	// PublicKeys maps each NodeID to its corresponding public key.
	// The map keys define the set of consensus participants (NodeIDs must be sequential starting from 0).
	PublicKeys map[NodeID]PublicKey
	
	// Timeout configuration for view changes
	BaseTimeout      time.Duration // Base timeout for view changes
	TimeoutMultiplier float64      // Multiplier for exponential backoff (default 2.0)
	MaxTimeout       time.Duration // Maximum timeout duration
}

// NewConsensusConfig creates a new consensus configuration with the given public keys.
// The NodeIDs are assigned sequentially starting from 0.
func NewConsensusConfig(publicKeys []PublicKey) (*ConsensusConfig, error) {
	nodeCount := len(publicKeys)
	if nodeCount < 4 {
		return nil, fmt.Errorf("BFT consensus requires at least 4 nodes for meaningful fault tolerance, got %d", nodeCount)
	}

	if nodeCount > 512 {
		return nil, fmt.Errorf("node count cannot exceed 512, got %d", nodeCount)
	}

	// Create public key map with sequential NodeIDs
	keyMap := make(map[NodeID]PublicKey)

	for i := 0; i < nodeCount; i++ {
		nodeID := NodeID(i)
		keyMap[nodeID] = publicKeys[i]
	}

	return &ConsensusConfig{
		PublicKeys:        keyMap,
		BaseTimeout:       5 * time.Second,  // Default 5 second base timeout
		TimeoutMultiplier: 2.0,              // Exponential backoff: timeout = baseTimeout * multiplier^view
		MaxTimeout:        60 * time.Second, // Maximum 60 second timeout
	}, nil
}

// TotalNodes returns the total number of consensus participants.
func (c *ConsensusConfig) TotalNodes() int {
	return len(c.PublicKeys)
}

// FaultyNodes returns the maximum number of Byzantine nodes that can be tolerated (f).
// In BFT systems: n = 3f + 1, so f = (n-1)/3
// Returns -1 if the configuration is invalid.
func (c *ConsensusConfig) FaultyNodes() int {
	totalNodes := c.TotalNodes()
	if totalNodes == 0 {
		return -1
	}
	return (totalNodes - 1) / 3
}

// QuorumThreshold returns the minimum number of votes required for a quorum.
// HotStuff BFT requires ⌊2n/3⌋+1 votes to ensure >2/3 honest majority.
// For n=5: ⌊10/3⌋+1 = 4 votes required.
func (c *ConsensusConfig) QuorumThreshold() int {
	return (c.TotalNodes()*2)/3 + 1
}

// IsValidNodeID returns true if the given NodeID is valid in this configuration.
func (c *ConsensusConfig) IsValidNodeID(nodeID NodeID) bool {
	return int(nodeID) < c.TotalNodes()
}

// GetPublicKey returns the public key for the given NodeID.
func (c *ConsensusConfig) GetPublicKey(nodeID NodeID) (PublicKey, error) {
	if !c.IsValidNodeID(nodeID) {
		return nil, fmt.Errorf("invalid node ID: %d", nodeID)
	}

	pubKey, exists := c.PublicKeys[nodeID]
	if !exists {
		return nil, fmt.Errorf("no public key found for node ID: %d", nodeID)
	}

	return pubKey, nil
}

// HasPublicKey returns true if the configuration has a public key for the given NodeID.
func (c *ConsensusConfig) HasPublicKey(nodeID NodeID) bool {
	_, exists := c.PublicKeys[nodeID]
	return exists
}

// HasQuorum returns true if the given vote count meets the quorum threshold.
func (c *ConsensusConfig) HasQuorum(voteCount int) bool {
	return voteCount >= c.QuorumThreshold()
}

// GetLeaderForView returns the leader NodeID for a given view using round-robin selection.
func (c *ConsensusConfig) GetLeaderForView(view ViewNumber) (NodeID, error) {
	totalNodes := c.TotalNodes()
	if totalNodes == 0 {
		return NodeID(0), fmt.Errorf("consensus configuration is invalid")
	}
	return NodeID(int(view) % totalNodes), nil
}

// CreateVoterBitmap creates a bitmap representing the given voters.
func (c *ConsensusConfig) CreateVoterBitmap(voters []NodeID) ([]byte, error) {
	// Calculate bitmap size (round up to nearest byte)
	bitmapSize := (c.TotalNodes() + 7) / 8
	bitmap := make([]byte, bitmapSize)

	for _, voterID := range voters {
		if !c.IsValidNodeID(voterID) {
			return nil, fmt.Errorf("invalid voter ID: %d", voterID)
		}

		byteIndex := int(voterID) / 8
		bitIndex := int(voterID) % 8
		bitmap[byteIndex] |= 1 << bitIndex
	}

	return bitmap, nil
}

// ParseVoterBitmap extracts the list of voters from a bitmap.
func (c *ConsensusConfig) ParseVoterBitmap(bitmap []byte) []NodeID {
	var voters []NodeID

	expectedSize := (c.TotalNodes() + 7) / 8
	if len(bitmap) != expectedSize {
		return voters // Return empty if bitmap size is incorrect
	}

	for nodeID := 0; nodeID < c.TotalNodes(); nodeID++ {
		byteIndex := nodeID / 8
		bitIndex := nodeID % 8

		if bitmap[byteIndex]&(1<<bitIndex) != 0 {
			voters = append(voters, NodeID(nodeID))
		}
	}

	return voters
}

// CountVotesInBitmap returns the number of votes represented in the bitmap.
func (c *ConsensusConfig) CountVotesInBitmap(bitmap []byte) int {
	count := 0
	expectedSize := (c.TotalNodes() + 7) / 8
	if len(bitmap) != expectedSize {
		return 0
	}

	for nodeID := 0; nodeID < c.TotalNodes(); nodeID++ {
		byteIndex := nodeID / 8
		bitIndex := nodeID % 8

		if bitmap[byteIndex]&(1<<bitIndex) != 0 {
			count++
		}
	}

	return count
}

// IsValidForBFT returns true if the configuration is valid for Byzantine Fault Tolerance.
// BFT consensus requires at least 4 nodes to tolerate 1 Byzantine fault (n = 3f + 1).
func (c *ConsensusConfig) IsValidForBFT() bool {
	return c.TotalNodes() >= 4
}

// Validate checks if the consensus configuration is valid and returns an error if not.
func (c *ConsensusConfig) Validate() error {
	totalNodes := c.TotalNodes()

	if totalNodes < 4 {
		return fmt.Errorf("BFT consensus requires at least 4 nodes for meaningful fault tolerance, got %d", totalNodes)
	}

	if totalNodes > 512 {
		return fmt.Errorf("node count cannot exceed 512, got %d", totalNodes)
	}

	// Verify node IDs are sequential starting from 0
	for expectedNodeID := 0; expectedNodeID < totalNodes; expectedNodeID++ {
		nodeID := NodeID(expectedNodeID)
		pubKey, exists := c.PublicKeys[nodeID]
		if !exists {
			return fmt.Errorf("missing node ID %d - node IDs must be sequential starting from 0", nodeID)
		}

		if len(pubKey) == 0 {
			return fmt.Errorf("empty public key for node ID: %d", nodeID)
		}
	}

	// Verify no extra node IDs beyond the expected sequential range
	for nodeID := range c.PublicKeys {
		if int(nodeID) >= totalNodes {
			return fmt.Errorf("node ID %d is out of sequential range [0, %d)", nodeID, totalNodes)
		}
	}

	return nil
}

// CanTolerateFailures returns true if the configuration can tolerate the given number of node failures.
func (c *ConsensusConfig) CanTolerateFailures(failures int) bool {
	return failures <= c.FaultyNodes()
}

// GetTimeoutForView returns the timeout duration for a given view using exponential backoff.
// Formula: timeout = min(baseTimeout * multiplier^view, maxTimeout)
func (c *ConsensusConfig) GetTimeoutForView(view ViewNumber) time.Duration {
	if view < 0 {
		return c.BaseTimeout
	}
	
	// Calculate exponential backoff: baseTimeout * multiplier^view
	multiplier := c.TimeoutMultiplier
	if multiplier <= 1.0 {
		multiplier = 2.0 // Default fallback
	}
	
	// Use math.Pow for accurate calculation
	timeout := float64(c.BaseTimeout) * math.Pow(multiplier, float64(view))
	
	// Cap at maximum timeout
	if time.Duration(timeout) > c.MaxTimeout {
		return c.MaxTimeout
	}
	
	return time.Duration(timeout)
}

// powFloat computes base^exp for floating point values (simple implementation)
func powFloat(base, exp float64) float64 {
	if exp == 0 {
		return 1
	}
	if exp == 1 {
		return base
	}
	
	result := 1.0
	for i := 0; i < int(exp); i++ {
		result *= base
		// Prevent excessive growth
		if result > 1e12 {
			return 1e12
		}
	}
	return result
}

// IsTimeoutEnabled returns true if timeout handling is enabled (BaseTimeout > 0)
func (c *ConsensusConfig) IsTimeoutEnabled() bool {
	return c.BaseTimeout > 0
}

// String returns a string representation of the consensus configuration.
func (c *ConsensusConfig) String() string {
	return fmt.Sprintf("ConsensusConfig{Nodes: %d, Faulty: %d, Quorum: %d, BaseTimeout: %v}",
		c.TotalNodes(), c.FaultyNodes(), c.QuorumThreshold(), c.BaseTimeout)
}
