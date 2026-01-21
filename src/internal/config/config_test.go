package config

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"btc-gateway/internal/keys"
	"btc-gateway/internal/types"
)

func TestManager_LoadConfig(t *testing.T) {
	keyManager := keys.NewKeyManager()
	manager := NewManager(keyManager)

	t.Run("creates default config when file doesn't exist", func(t *testing.T) {
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "test_config.yaml")

		cfg, err := manager.LoadConfig(configPath)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if cfg == nil {
			t.Fatal("Expected config to be loaded")
		}

		// Verify file was created
		if _, err := os.Stat(configPath); os.IsNotExist(err) {
			t.Fatal("Expected config file to be created")
		}

		// Verify private key was generated
		if cfg.Node.PrivateKey == "" {
			t.Fatal("Expected private key to be generated")
		}
	})

	t.Run("loads existing valid config", func(t *testing.T) {
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "test_config.yaml")

		// Generate a valid private key for testing
		testKey, err := keyManager.GeneratePrivateKey()
		if err != nil {
			t.Fatalf("Failed to generate test key: %v", err)
		}

		// Create a valid config file
		validConfig := fmt.Sprintf(`
node:
  private_key: "%s"

network:
  addresses:
    - "/ip4/0.0.0.0/tcp/9000"

peers:
  connection_timeout: "10s"

logging:
  level: "info"
  format: "json"
`, testKey)

		err = os.WriteFile(configPath, []byte(validConfig), 0644)
		if err != nil {
			t.Fatalf("Failed to create test config file: %v", err)
		}

		cfg, err := manager.LoadConfig(configPath)
		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if cfg.Node.PrivateKey == "" {
			t.Error("Expected private key to be loaded")
		}
		if len(cfg.Network.Addresses) == 0 {
			t.Error("Expected network addresses to be loaded")
		}
	})

	t.Run("fails on invalid YAML", func(t *testing.T) {
		tempDir := t.TempDir()
		configPath := filepath.Join(tempDir, "test_config.yaml")

		invalidYAML := `
node:
  private_key: "test"
invalid_yaml: [
`

		err := os.WriteFile(configPath, []byte(invalidYAML), 0644)
		if err != nil {
			t.Fatalf("Failed to create test config file: %v", err)
		}

		_, err = manager.LoadConfig(configPath)
		if err == nil {
			t.Fatal("Expected error for invalid YAML")
		}
	})
}

func TestManager_ValidateConfig(t *testing.T) {
	keyManager := keys.NewKeyManager()
	manager := NewManager(keyManager)

	t.Run("validates valid config", func(t *testing.T) {
		// Generate a valid test key
		testKey, err := keyManager.GeneratePrivateKey()
		if err != nil {
			t.Fatalf("Failed to generate test key: %v", err)
		}

		cfg := &types.Config{
			Node: types.NodeConfig{
				PrivateKey: testKey,
			},
			Network: types.NetworkConfig{
				Addresses: []string{"/ip4/0.0.0.0/tcp/9000"},
			},
			Peers: types.PeersConfig{
				ConnectionTimeout: 10 * time.Second,
			},
			Logging: types.LoggingConfig{
				Level:  "info",
				Format: "json",
			},
		}

		err = manager.ValidateConfig(cfg)
		if err != nil {
			t.Fatalf("Expected valid config to pass validation, got %v", err)
		}
	})

	t.Run("rejects nil config", func(t *testing.T) {
		err := manager.ValidateConfig(nil)
		if err == nil {
			t.Fatal("Expected error for nil config")
		}
	})

	t.Run("rejects empty network addresses", func(t *testing.T) {
		cfg := types.DefaultConfig()
		cfg.Network.Addresses = []string{}

		err := manager.ValidateConfig(cfg)
		if err == nil {
			t.Fatal("Expected error for empty network addresses")
		}
	})

	t.Run("rejects invalid log level", func(t *testing.T) {
		cfg := types.DefaultConfig()
		cfg.Logging.Level = "invalid"

		err := manager.ValidateConfig(cfg)
		if err == nil {
			t.Fatal("Expected error for invalid log level")
		}
	})
}

func TestValidateMultiaddr(t *testing.T) {
	tests := []struct {
		name    string
		addr    string
		wantErr bool
	}{
		{"valid IPv4", "/ip4/192.168.1.1/tcp/9000", false},
		{"valid IPv6", "/ip6/::1/tcp/9000", false},
		{"valid DNS4", "/dns4/example.com/tcp/9000", false},
		{"valid DNS6", "/dns6/example.com/tcp/9000", false},
		{"empty address", "", true},
		{"no leading slash", "ip4/192.168.1.1/tcp/9000", true},
		{"invalid format", "/invalid/format", true},
		{"missing protocol", "/ip4/192.168.1.1", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateMultiaddr(tt.addr)
			if (err != nil) != tt.wantErr {
				t.Errorf("validateMultiaddr() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
