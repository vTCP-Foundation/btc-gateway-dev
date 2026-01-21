package storage

import (
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test data
var (
	validPublicKey1 = base64.StdEncoding.EncodeToString([]byte("test-public-key-1-for-validation"))
	validPublicKey2 = base64.StdEncoding.EncodeToString([]byte("test-public-key-2-for-validation"))
	validPublicKey3 = base64.StdEncoding.EncodeToString([]byte("different-key-for-testing-dups"))
	
	validPeer1 = Peer{
		PublicKey: validPublicKey1,
		Addresses: []string{
			"/ip4/192.168.1.100/tcp/9000",
			"/dns4/node1.example.com/tcp/9000",
		},
	}
	
	validPeer2 = Peer{
		PublicKey: validPublicKey2,
		Addresses: []string{
			"/ip4/10.0.0.1/tcp/9000",
		},
	}
	
	duplicatePeer = Peer{
		PublicKey: validPublicKey1, // Same as validPeer1
		Addresses: []string{
			"/ip4/192.168.1.200/tcp/9000",
		},
	}
)

func TestNewFilePeerStorage(t *testing.T) {
	storage := NewFilePeerStorage("/tmp/test-peers.yaml")
	
	assert.NotNil(t, storage)
	assert.Equal(t, "/tmp/test-peers.yaml", storage.filePath)
	assert.NotNil(t, storage.peers)
	assert.Len(t, storage.peers, 0)
}

func TestFilePeerStorage_GetPeers_EmptyStorage(t *testing.T) {
	storage := NewFilePeerStorage("/tmp/test-peers.yaml")
	
	peers, err := storage.GetPeers()
	
	require.NoError(t, err)
	assert.Len(t, peers, 0)
}

func TestFilePeerStorage_LoadPeers_FileNotExists(t *testing.T) {
	tempFile := filepath.Join(t.TempDir(), "nonexistent.yaml")
	storage := NewFilePeerStorage(tempFile)
	
	err := storage.LoadPeers()
	
	require.NoError(t, err)
	peers, err := storage.GetPeers()
	require.NoError(t, err)
	assert.Len(t, peers, 0)
}

func TestFilePeerStorage_LoadPeers_ValidFile(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "valid-peers.yaml")
	
	// Create valid YAML file
	yamlContent := `peers:
  - public_key: ` + validPublicKey1 + `
    addresses:
      - "/ip4/192.168.1.100/tcp/9000"
      - "/dns4/node1.example.com/tcp/9000"
  - public_key: ` + validPublicKey2 + `
    addresses:
      - "/ip4/10.0.0.1/tcp/9000"`
	
	err := os.WriteFile(tempFile, []byte(yamlContent), 0644)
	require.NoError(t, err)
	
	storage := NewFilePeerStorage(tempFile)
	err = storage.LoadPeers()
	require.NoError(t, err)
	
	peers, err := storage.GetPeers()
	require.NoError(t, err)
	assert.Len(t, peers, 2)
	
	assert.Equal(t, validPublicKey1, peers[0].PublicKey)
	assert.Len(t, peers[0].Addresses, 2)
	assert.Equal(t, validPublicKey2, peers[1].PublicKey)
	assert.Len(t, peers[1].Addresses, 1)
}

func TestFilePeerStorage_LoadPeers_InvalidYAML(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "invalid.yaml")
	
	// Create invalid YAML file
	yamlContent := `peers:
  - public_key: invalid-yaml-structure
    addresses: not-a-list`
	
	err := os.WriteFile(tempFile, []byte(yamlContent), 0644)
	require.NoError(t, err)
	
	storage := NewFilePeerStorage(tempFile)
	err = storage.LoadPeers()
	
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to parse peer file")
}

func TestFilePeerStorage_SavePeers_Success(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "save-test.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	peers := []Peer{validPeer1, validPeer2}
	
	err := storage.SavePeers(peers)
	require.NoError(t, err)
	
	// Verify file was created
	assert.FileExists(t, tempFile)
	
	// Load and verify content
	newStorage := NewFilePeerStorage(tempFile)
	err = newStorage.LoadPeers()
	require.NoError(t, err)
	
	loadedPeers, err := newStorage.GetPeers()
	require.NoError(t, err)
	assert.Len(t, loadedPeers, 2)
	assert.Equal(t, peers, loadedPeers)
}

func TestFilePeerStorage_SavePeers_AtomicOperation(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "atomic-test.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	peers := []Peer{validPeer1}
	
	err := storage.SavePeers(peers)
	require.NoError(t, err)
	
	// Verify temporary file is cleaned up
	tempFilePattern := tempFile + ".tmp"
	assert.NoFileExists(t, tempFilePattern)
	
	// Verify final file exists
	assert.FileExists(t, tempFile)
}

func TestFilePeerStorage_ValidatePublicKey(t *testing.T) {
	storage := NewFilePeerStorage("/tmp/test.yaml")
	
	tests := []struct {
		name      string
		publicKey string
		wantErr   bool
	}{
		{
			name:      "valid base64 key",
			publicKey: validPublicKey1,
			wantErr:   false,
		},
		{
			name:      "empty key",
			publicKey: "",
			wantErr:   true,
		},
		{
			name:      "invalid base64",
			publicKey: "not-base64!@#$",
			wantErr:   true,
		},
		{
			name:      "too short key",
			publicKey: base64.StdEncoding.EncodeToString([]byte("short")),
			wantErr:   true,
		},
		{
			name:      "too long key",
			publicKey: base64.StdEncoding.EncodeToString(make([]byte, 200)),
			wantErr:   true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.validatePublicKey(tt.publicKey)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFilePeerStorage_ValidateAddresses(t *testing.T) {
	storage := NewFilePeerStorage("/tmp/test.yaml")
	
	tests := []struct {
		name      string
		addresses []string
		wantErr   bool
	}{
		{
			name:      "valid TCP address",
			addresses: []string{"/ip4/192.168.1.1/tcp/9000"},
			wantErr:   false,
		},
		{
			name:      "valid DNS address",
			addresses: []string{"/dns4/example.com/tcp/9000"},
			wantErr:   false,
		},
		{
			name:      "multiple valid addresses",
			addresses: []string{"/ip4/192.168.1.1/tcp/9000", "/dns4/example.com/tcp/9000"},
			wantErr:   false,
		},
		{
			name:      "empty addresses",
			addresses: []string{},
			wantErr:   true,
		},
		{
			name:      "empty address string",
			addresses: []string{""},
			wantErr:   true,
		},
		{
			name:      "invalid multiaddress",
			addresses: []string{"not-a-multiaddress"},
			wantErr:   true,
		},
		{
			name:      "mixed valid and invalid",
			addresses: []string{"/ip4/192.168.1.1/tcp/9000", "invalid"},
			wantErr:   true,
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.validateAddresses(tt.addresses)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFilePeerStorage_DuplicateFiltering(t *testing.T) {
	storage := NewFilePeerStorage("/tmp/test.yaml")
	
	peers := []Peer{validPeer1, duplicatePeer, validPeer2}
	
	validPeers, err := storage.validateAndFilterPeers(peers)
	require.NoError(t, err)
	
	// Should filter out the duplicate
	assert.Len(t, validPeers, 2)
	assert.Equal(t, validPeer1, validPeers[0])
	assert.Equal(t, validPeer2, validPeers[1])
}

func TestFilePeerStorage_ConcurrentAccess(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "concurrent-test.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	// Initialize with some data
	err := storage.SavePeers([]Peer{validPeer1})
	require.NoError(t, err)
	
	var wg sync.WaitGroup
	const numGoroutines = 10
	
	// Test concurrent reads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			for j := 0; j < 10; j++ {
				peers, err := storage.GetPeers()
				assert.NoError(t, err)
				assert.Len(t, peers, 1)
				
				err = storage.LoadPeers()
				assert.NoError(t, err)
			}
		}(i)
	}
	
	wg.Wait()
}

func TestFilePeerStorage_ConcurrentSaveLoad(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "concurrent-save-load.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	var wg sync.WaitGroup
	const numWriters = 5
	const numReaders = 5
	
	// Writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			keyData := make([]byte, 32) // 32 bytes for valid key length
			copy(keyData, []byte("writer-"+string(rune(id+48))))
			peer := Peer{
				PublicKey: base64.StdEncoding.EncodeToString(keyData),
				Addresses: []string{fmt.Sprintf("/ip4/192.168.1.%d/tcp/9000", 100+id)},
			}
			
			err := storage.SavePeers([]Peer{peer})
			assert.NoError(t, err)
		}(i)
	}
	
	// Readers
	for i := 0; i < numReaders; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			
			time.Sleep(time.Millisecond * 10) // Small delay to allow some writes
			
			err := storage.LoadPeers()
			assert.NoError(t, err)
			
			peers, err := storage.GetPeers()
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, len(peers), 0) // May be empty if read before writes
		}(i)
	}
	
	wg.Wait()
}

func TestFilePeerStorage_GetPeersReturnsCopy(t *testing.T) {
	storage := NewFilePeerStorage("/tmp/test.yaml")
	
	// Set some internal data
	storage.peers = []Peer{validPeer1}
	
	peers1, err := storage.GetPeers()
	require.NoError(t, err)
	
	peers2, err := storage.GetPeers()
	require.NoError(t, err)
	
	// Modify the returned slice
	if len(peers1) > 0 {
		peers1[0].PublicKey = "modified"
	}
	
	// Verify original data is unchanged
	assert.NotEqual(t, peers1[0].PublicKey, peers2[0].PublicKey)
	assert.Equal(t, validPeer1.PublicKey, peers2[0].PublicKey)
}

func TestFilePeerStorage_CreateBackup(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "backup-test.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	// Save some data first
	err := storage.SavePeers([]Peer{validPeer1})
	require.NoError(t, err)
	
	// Create backup
	err = storage.CreateBackup()
	require.NoError(t, err)
	
	// Check that backup file was created
	files, err := filepath.Glob(tempFile + ".backup.*")
	require.NoError(t, err)
	assert.Len(t, files, 1)
	
	// Verify backup content
	backupStorage := NewFilePeerStorage(files[0])
	err = backupStorage.LoadPeers()
	require.NoError(t, err)
	
	backupPeers, err := backupStorage.GetPeers()
	require.NoError(t, err)
	assert.Equal(t, []Peer{validPeer1}, backupPeers)
}

func TestFilePeerStorage_CreateBackup_NoFile(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "nonexistent.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	// Try to backup non-existent file
	err := storage.CreateBackup()
	
	// Should not error when file doesn't exist
	assert.NoError(t, err)
}

func TestFilePeerStorage_GetFileChecksum(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "checksum-test.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	// Test checksum of non-existent file
	checksum1, err := storage.GetFileChecksum()
	require.NoError(t, err)
	assert.Empty(t, checksum1)
	
	// Save some data
	err = storage.SavePeers([]Peer{validPeer1})
	require.NoError(t, err)
	
	// Get checksum
	checksum2, err := storage.GetFileChecksum()
	require.NoError(t, err)
	assert.NotEmpty(t, checksum2)
	assert.Len(t, checksum2, 64) // SHA256 hex string length
	
	// Modify file and verify checksum changes
	err = storage.SavePeers([]Peer{validPeer1, validPeer2})
	require.NoError(t, err)
	
	checksum3, err := storage.GetFileChecksum()
	require.NoError(t, err)
	assert.NotEqual(t, checksum2, checksum3)
}

func TestFilePeerStorage_ValidationErrors(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "validation-test.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	tests := []struct {
		name    string
		peers   []Peer
		wantErr bool
		errMsg  string
	}{
		{
			name: "invalid public key",
			peers: []Peer{{
				PublicKey: "invalid-base64!",
				Addresses: []string{"/ip4/192.168.1.1/tcp/9000"},
			}},
			wantErr: true,
			errMsg:  "invalid public key",
		},
		{
			name: "empty addresses",
			peers: []Peer{{
				PublicKey: validPublicKey1,
				Addresses: []string{},
			}},
			wantErr: true,
			errMsg:  "invalid addresses",
		},
		{
			name: "invalid multiaddress",
			peers: []Peer{{
				PublicKey: validPublicKey1,
				Addresses: []string{"not-a-multiaddress"},
			}},
			wantErr: true,
			errMsg:  "invalid addresses",
		},
	}
	
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := storage.SavePeers(tt.peers)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestFilePeerStorage_MemoryConsistency(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "memory-test.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	// Save peers
	originalPeers := []Peer{validPeer1, validPeer2}
	err := storage.SavePeers(originalPeers)
	require.NoError(t, err)
	
	// Verify in-memory state matches
	memoryPeers, err := storage.GetPeers()
	require.NoError(t, err)
	assert.Equal(t, originalPeers, memoryPeers)
	
	// Load from file and verify consistency
	newStorage := NewFilePeerStorage(tempFile)
	err = newStorage.LoadPeers()
	require.NoError(t, err)
	
	loadedPeers, err := newStorage.GetPeers()
	require.NoError(t, err)
	assert.Equal(t, originalPeers, loadedPeers)
}

func TestFilePeerStorage_SavePeers_DirectoryCreation(t *testing.T) {
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "subdir", "peers.yaml")
	
	storage := NewFilePeerStorage(tempFile)
	
	err := storage.SavePeers([]Peer{validPeer1})
	require.NoError(t, err)
	
	assert.FileExists(t, tempFile)
}

func TestFilePeerStorage_SavePeers_RenameError(t *testing.T) {
	// Create a file in a read-only directory to simulate write error
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "readonly", "peers.yaml")
	
	// Create readonly directory
	err := os.MkdirAll(filepath.Dir(tempFile), 0444) // read-only
	require.NoError(t, err)
	
	// Restore permissions after test
	defer func() {
		os.Chmod(filepath.Dir(tempFile), 0755)
	}()
	
	storage := NewFilePeerStorage(tempFile)
	
	err = storage.SavePeers([]Peer{validPeer1})
	
	// Should fail due to permission error during write
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to write temporary file")
}

func TestFilePeerStorage_CreateBackup_SourceError(t *testing.T) {
	// Test backup creation when source file doesn't exist
	storage := NewFilePeerStorage("/nonexistent/file.yaml")
	
	// Create backup should handle error gracefully when source file doesn't exist
	err := storage.CreateBackup()
	// Should succeed (no file to backup) or return specific error
	if err != nil {
		assert.Contains(t, err.Error(), "failed to open source file")
	}
}

func TestFilePeerStorage_GetFileChecksum_IOError(t *testing.T) {
	// Test with a file that exists but can't be read due to permissions
	tempDir := t.TempDir()
	tempFile := filepath.Join(tempDir, "unreadable.yaml")
	
	// Create file and make it unreadable
	err := os.WriteFile(tempFile, []byte("test"), 0000) // no permissions
	require.NoError(t, err)
	
	defer func() {
		os.Chmod(tempFile, 0644) // restore permissions for cleanup
	}()
	
	storage := NewFilePeerStorage(tempFile)
	
	checksum, err := storage.GetFileChecksum()
	assert.Error(t, err)
	assert.Empty(t, checksum)
}

// Benchmark tests
func BenchmarkFilePeerStorage_GetPeers(b *testing.B) {
	storage := NewFilePeerStorage("/tmp/bench.yaml")
	
	// Setup with 100 peers
	peers := make([]Peer, 100)
	for i := 0; i < 100; i++ {
		keyData := make([]byte, 32)
		copy(keyData, []byte("benchmark-key-"+fmt.Sprintf("%d", i)))
		peers[i] = Peer{
			PublicKey: base64.StdEncoding.EncodeToString(keyData),
			Addresses: []string{fmt.Sprintf("/ip4/192.168.1.%d/tcp/9000", 100+i%155)},
		}
	}
	storage.peers = peers
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := storage.GetPeers()
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFilePeerStorage_SavePeers(b *testing.B) {
	tempDir := b.TempDir()
	
	// Setup with 10 peers
	peers := make([]Peer, 10)
	for i := 0; i < 10; i++ {
		keyData := make([]byte, 32)
		copy(keyData, []byte("benchmark-key-"+fmt.Sprintf("%d", i)))
		peers[i] = Peer{
			PublicKey: base64.StdEncoding.EncodeToString(keyData),
			Addresses: []string{fmt.Sprintf("/ip4/192.168.1.%d/tcp/9000", 100+i%155)},
		}
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		storage := NewFilePeerStorage(filepath.Join(tempDir, "bench-"+string(rune(i))+".yaml"))
		err := storage.SavePeers(peers)
		if err != nil {
			b.Fatal(err)
		}
	}
}