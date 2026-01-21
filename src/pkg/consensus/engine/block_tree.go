package engine

import (
	"fmt"

	"btc-gateway/pkg/consensus/types"
)

// BlockTree manages the block chain structure and handles forks in the HotStuff protocol.
// It maintains the relationship between blocks and provides methods for navigating the tree.
type BlockTree struct {
	// blocks stores all known blocks indexed by their hash
	blocks map[types.BlockHash]*types.Block

	// children maps each block hash to its child blocks (for fork management)
	children map[types.BlockHash][]*types.Block

	// root is the genesis block of the chain
	root *types.Block

	// highestCommitted is the highest committed block
	highestCommitted *types.Block
}

// NewBlockTree creates a new block tree with the given genesis block as root.
func NewBlockTree(genesisBlock *types.Block) *BlockTree {
	if genesisBlock == nil {
		panic("genesis block cannot be nil")
	}

	bt := &BlockTree{
		blocks:           make(map[types.BlockHash]*types.Block),
		children:         make(map[types.BlockHash][]*types.Block),
		root:             genesisBlock,
		highestCommitted: genesisBlock, // Genesis is immediately committed
	}

	// Add genesis block to the tree
	bt.blocks[genesisBlock.Hash] = genesisBlock

	return bt
}

// AddBlock adds a new block to the tree.
func (bt *BlockTree) AddBlock(block *types.Block) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}

	// Check if block already exists
	if _, exists := bt.blocks[block.Hash]; exists {
		return fmt.Errorf("block %x already exists in tree", block.Hash)
	}

	// For non-genesis blocks, verify parent exists
	if !block.IsGenesis() {
		parent, exists := bt.blocks[block.ParentHash]
		if !exists {
			return fmt.Errorf("parent block %x not found in tree", block.ParentHash)
		}

		// Verify height is correct (parent height + 1)
		if block.Height != parent.Height+1 {
			return fmt.Errorf("invalid block height: expected %d, got %d",
				parent.Height+1, block.Height)
		}
	} else {
		// Genesis block should have height 0
		if block.Height != 0 {
			return fmt.Errorf("genesis block must have height 0, got %d", block.Height)
		}
	}

	// Add block to storage
	bt.blocks[block.Hash] = block

	// Add to parent's children list
	bt.children[block.ParentHash] = append(bt.children[block.ParentHash], block)

	return nil
}

// GetBlock retrieves a block by its hash.
func (bt *BlockTree) GetBlock(hash types.BlockHash) (*types.Block, error) {
	block, exists := bt.blocks[hash]
	if !exists {
		return nil, fmt.Errorf("block %x not found in tree", hash)
	}
	return block, nil
}

// HasBlock returns true if a block with the given hash exists in the tree.
func (bt *BlockTree) HasBlock(hash types.BlockHash) bool {
	_, exists := bt.blocks[hash]
	return exists
}

// GetChildren returns all direct children of the specified block.
func (bt *BlockTree) GetChildren(hash types.BlockHash) []*types.Block {
	return bt.children[hash]
}

// GetAncestors returns all ancestors of the specified block up to the root.
// The result is ordered from the given block's parent to the root.
func (bt *BlockTree) GetAncestors(hash types.BlockHash) ([]*types.Block, error) {
	block, err := bt.GetBlock(hash)
	if err != nil {
		return nil, err
	}

	var ancestors []*types.Block
	current := block

	// Walk up the chain until we reach the root
	for !current.IsGenesis() {
		parent, err := bt.GetBlock(current.ParentHash)
		if err != nil {
			return nil, fmt.Errorf("missing parent block %x: %w", current.ParentHash, err)
		}
		ancestors = append(ancestors, parent)
		current = parent
	}

	return ancestors, nil
}

// IsAncestor returns true if ancestorHash is an ancestor of blockHash.
func (bt *BlockTree) IsAncestor(ancestorHash, blockHash types.BlockHash) (bool, error) {
	// Same block is not considered an ancestor of itself
	if ancestorHash == blockHash {
		return false, nil
	}

	ancestors, err := bt.GetAncestors(blockHash)
	if err != nil {
		return false, err
	}

	for _, ancestor := range ancestors {
		if ancestor.Hash == ancestorHash {
			return true, nil
		}
	}

	return false, nil
}

// FindLCA finds the Lowest Common Ancestor of two blocks.
func (bt *BlockTree) FindLCA(hash1, hash2 types.BlockHash) (*types.Block, error) {
	// If one of the blocks is the same, return it
	if hash1 == hash2 {
		return bt.GetBlock(hash1)
	}

	// Get ancestors for both blocks
	ancestors1, err := bt.GetAncestors(hash1)
	if err != nil {
		return nil, fmt.Errorf("failed to get ancestors of %x: %w", hash1, err)
	}

	ancestors2, err := bt.GetAncestors(hash2)
	if err != nil {
		return nil, fmt.Errorf("failed to get ancestors of %x: %w", hash2, err)
	}

	// Add the blocks themselves to their ancestor lists
	block1, _ := bt.GetBlock(hash1)
	block2, _ := bt.GetBlock(hash2)
	ancestors1 = append([]*types.Block{block1}, ancestors1...)
	ancestors2 = append([]*types.Block{block2}, ancestors2...)

	// Create a set of ancestors for block1 for O(1) lookup
	ancestorSet := make(map[types.BlockHash]bool)
	for _, ancestor := range ancestors1 {
		ancestorSet[ancestor.Hash] = true
	}

	// Find first common ancestor in block2's ancestry
	for _, ancestor := range ancestors2 {
		if ancestorSet[ancestor.Hash] {
			return ancestor, nil
		}
	}

	// Should never happen in a valid tree (root should always be common)
	return nil, fmt.Errorf("no common ancestor found between %x and %x", hash1, hash2)
}

// GetRoot returns the root (genesis) block of the tree.
func (bt *BlockTree) GetRoot() *types.Block {
	return bt.root
}

// GetCommitted returns the highest committed block.
func (bt *BlockTree) GetCommitted() *types.Block {
	return bt.highestCommitted
}

// CommitBlock marks a block as committed and advances the committed pointer.
func (bt *BlockTree) CommitBlock(block *types.Block) error {
	if block == nil {
		return fmt.Errorf("block cannot be nil")
	}

	// Verify block exists in tree
	if _, err := bt.GetBlock(block.Hash); err != nil {
		return fmt.Errorf("cannot commit unknown block %x", block.Hash)
	}

	// Verify this block extends the current committed chain
	// (In a real implementation, this would be more sophisticated)
	if !block.IsGenesis() {
		isAncestor, err := bt.IsAncestor(bt.highestCommitted.Hash, block.Hash)
		if err != nil {
			return fmt.Errorf("failed to check ancestry: %w", err)
		}
		if !isAncestor && bt.highestCommitted.Hash != block.ParentHash {
			return fmt.Errorf("block %x does not extend committed chain", block.Hash)
		}
	}

	// Update committed block
	bt.highestCommitted = block

	return nil
}

// GetPath returns the path of blocks from one block to another.
// Returns nil if no path exists (blocks are on different branches).
func (bt *BlockTree) GetPath(fromHash, toHash types.BlockHash) ([]*types.Block, error) {
	if fromHash == toHash {
		block, err := bt.GetBlock(fromHash)
		return []*types.Block{block}, err
	}

	// Check if toHash is an ancestor of fromHash
	if isAncestor, err := bt.IsAncestor(toHash, fromHash); err != nil {
		return nil, err
	} else if isAncestor {
		// Path goes backward (up the tree)
		var path []*types.Block
		current, err := bt.GetBlock(fromHash)
		if err != nil {
			return nil, err
		}

		path = append(path, current)
		for current.Hash != toHash {
			parent, err := bt.GetBlock(current.ParentHash)
			if err != nil {
				return nil, err
			}
			path = append(path, parent)
			current = parent
		}

		return path, nil
	}

	// Check if fromHash is an ancestor of toHash
	if isAncestor, err := bt.IsAncestor(fromHash, toHash); err != nil {
		return nil, err
	} else if isAncestor {
		// Path goes forward (down the tree) - this is more complex
		// For now, return nil as this requires more sophisticated traversal
		return nil, fmt.Errorf("forward path traversal not implemented")
	}

	// Blocks are on different branches, no direct path exists
	return nil, nil
}

// GetBlockCount returns the total number of blocks in the tree.
func (bt *BlockTree) GetBlockCount() int {
	return len(bt.blocks)
}

// GetForkCount returns the number of blocks that have multiple children (forks).
func (bt *BlockTree) GetForkCount() int {
	forkCount := 0
	for _, children := range bt.children {
		if len(children) > 1 {
			forkCount++
		}
	}
	return forkCount
}

// ValidateTree performs consistency checks on the entire tree structure.
func (bt *BlockTree) ValidateTree() error {
	// Check root exists
	if bt.root == nil {
		return fmt.Errorf("tree root cannot be nil")
	}

	// Check committed exists
	if bt.highestCommitted == nil {
		return fmt.Errorf("committed block cannot be nil")
	}

	// Validate each block
	for hash, block := range bt.blocks {
		if block.Hash != hash {
			return fmt.Errorf("block hash mismatch in storage: key %x, block hash %x",
				hash, block.Hash)
		}

		// Check parent-child relationships
		if !block.IsGenesis() {
			parent, exists := bt.blocks[block.ParentHash]
			if !exists {
				return fmt.Errorf("orphaned block %x: parent %x not found",
					block.Hash, block.ParentHash)
			}

			// Check if this block is in parent's children list
			found := false
			for _, child := range bt.children[block.ParentHash] {
				if child.Hash == block.Hash {
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("block %x not in parent %x children list",
					block.Hash, parent.Hash)
			}
		}
	}

	return nil
}

// String returns a string representation of the tree for debugging.
func (bt *BlockTree) String() string {
	return fmt.Sprintf("BlockTree{Blocks: %d, Forks: %d, Committed: %s}",
		bt.GetBlockCount(), bt.GetForkCount(), bt.highestCommitted.String())
}
