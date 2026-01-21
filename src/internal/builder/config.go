package builder

const (
	// MaxBlockSizeBytes is the maximum size of a block in bytes (128 MB).
	MaxBlockSizeBytes = 128 * 1024 * 1024

	// BatchOverheadBytes is the estimated overhead for batch serialization.
	// Accounts for JSON wrapper: {"transactions":[...]} plus comma separators.
	// We use a conservative estimate to ensure we don't exceed the block size limit.
	BatchOverheadBytes = 1024
)
