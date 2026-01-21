package types

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"time"
)

// Block represents a block in the HotStuff consensus protocol.
// Each block contains metadata about its position in the chain and opaque payload data.
type Block struct {
	// Hash is the unique identifier for this block
	Hash BlockHash
	// ParentHash is the hash of the parent block
	ParentHash BlockHash
	// Height is the position of this block in the blockchain
	Height Height
	// View is the consensus view when this block was proposed
	View ViewNumber
	// Proposer is the node that proposed this block
	Proposer NodeID
	// Timestamp is when this block was created
	Timestamp Timestamp
	// Payload contains the opaque block data
	Payload []byte
}

// NewBlock creates a new block with the specified parameters.
// The block hash is automatically calculated based on the block contents.
func NewBlock(parentHash BlockHash, height Height, view ViewNumber, proposer NodeID, payload []byte) *Block {
	block := &Block{
		ParentHash: parentHash,
		Height:     height,
		View:       view,
		Proposer:   proposer,
		Timestamp:  Timestamp(time.Now()),
		Payload:    payload,
	}
	block.Hash = block.CalculateHash()
	return block
}

// CalculateHash computes the SHA-256 hash of the block based on its contents.
func (b *Block) CalculateHash() BlockHash {
	hasher := sha256.New()

	// Write parent hash
	hasher.Write(b.ParentHash[:])

	// Write height (8 bytes)
	heightBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(heightBytes, uint64(b.Height))
	hasher.Write(heightBytes)

	// Write view (8 bytes)
	viewBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(viewBytes, uint64(b.View))
	hasher.Write(viewBytes)

	// Write proposer (2 bytes)
	proposerBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(proposerBytes, uint16(b.Proposer))
	hasher.Write(proposerBytes)

	// Write timestamp (8 bytes)
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes, uint64(time.Time(b.Timestamp).UnixNano()))
	hasher.Write(timestampBytes)

	// Write payload
	hasher.Write(b.Payload)

	var hash BlockHash
	copy(hash[:], hasher.Sum(nil))
	return hash
}

// Validate performs basic validation on the block structure.
func (b *Block) Validate(config *ConsensusConfig) error {
	if config != nil && !config.IsValidNodeID(b.Proposer) {
		return fmt.Errorf("invalid proposer node ID: %d", b.Proposer)
	}

	if time.Time(b.Timestamp).IsZero() {
		return fmt.Errorf("block timestamp cannot be zero")
	}

	// Verify hash matches calculated hash
	expectedHash := b.CalculateHash()
	if b.Hash != expectedHash {
		return fmt.Errorf("block hash mismatch: expected %x, got %x", expectedHash, b.Hash)
	}

	return nil
}

// IsGenesis returns true if this is the genesis block (height 0 with no parent).
func (b *Block) IsGenesis() bool {
	return b.Height == 0 && b.ParentHash == BlockHash{}
}

// String returns a string representation of the block for debugging.
func (b *Block) String() string {
	return fmt.Sprintf("Block{Hash: %x, Height: %d, View: %d, Proposer: %d}",
		b.Hash[:8], b.Height, b.View, b.Proposer)
}

// NewDeterministicBlock creates a block with a specified timestamp for deterministic hashing.
// This is useful for genesis blocks that must have the same hash across all nodes.
func NewDeterministicBlock(parentHash BlockHash, height Height, view ViewNumber, proposer NodeID, payload []byte, timestamp time.Time) *Block {
	block := &Block{
		ParentHash: parentHash,
		Height:     height,
		View:       view,
		Proposer:   proposer,
		Timestamp:  Timestamp(timestamp),
		Payload:    payload,
	}
	block.Hash = block.CalculateHash()
	return block
}

// NewGenesisBlock creates a deterministic genesis block that will have the same hash across all nodes.
func NewGenesisBlock() *Block {
	// Use Unix epoch as the deterministic timestamp for genesis
	genesisTime := time.Unix(0, 0).UTC()
	return NewDeterministicBlock(
		BlockHash{}, // No parent
		0,           // Height 0
		0,           // View 0
		0,           // Genesis always proposed by Node 0
		[]byte("btc-gateway-genesis-block"),
		genesisTime,
	)
}
