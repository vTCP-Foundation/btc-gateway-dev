package keys

import (
	"encoding/base64"
	"testing"
)

func TestKeyManager_GeneratePrivateKey(t *testing.T) {
	km := NewKeyManager()

	privateKey, err := km.GeneratePrivateKey()
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	if privateKey == "" {
		t.Fatal("Expected non-empty private key")
	}

	// Verify it's valid base64
	_, err = base64.StdEncoding.DecodeString(privateKey)
	if err != nil {
		t.Fatalf("Generated key is not valid base64: %v", err)
	}

	// Generate another key and verify they're different
	privateKey2, err := km.GeneratePrivateKey()
	if err != nil {
		t.Fatalf("Failed to generate second private key: %v", err)
	}

	if privateKey == privateKey2 {
		t.Fatal("Generated keys should be different")
	}
}

func TestKeyManager_ValidatePrivateKey(t *testing.T) {
	km := NewKeyManager()

	t.Run("accepts empty key", func(t *testing.T) {
		err := km.ValidatePrivateKey("")
		if err != nil {
			t.Fatalf("Expected empty key to be valid, got %v", err)
		}
	})

	t.Run("accepts valid key", func(t *testing.T) {
		// Generate a valid key first
		validKey, err := km.GeneratePrivateKey()
		if err != nil {
			t.Fatalf("Failed to generate test key: %v", err)
		}

		err = km.ValidatePrivateKey(validKey)
		if err != nil {
			t.Fatalf("Expected valid key to pass validation, got %v", err)
		}
	})

	t.Run("rejects invalid base64", func(t *testing.T) {
		err := km.ValidatePrivateKey("not-base64!")
		if err == nil {
			t.Fatal("Expected error for invalid base64")
		}
	})

	t.Run("rejects wrong length", func(t *testing.T) {
		// Valid base64 but wrong length
		shortKey := base64.StdEncoding.EncodeToString([]byte("short"))
		err := km.ValidatePrivateKey(shortKey)
		if err == nil {
			t.Fatal("Expected error for wrong key length")
		}
	})
}

func TestKeyManager_GetPublicKey(t *testing.T) {
	km := NewKeyManager()

	// Generate a private key first
	privateKey, err := km.GeneratePrivateKey()
	if err != nil {
		t.Fatalf("Failed to generate private key: %v", err)
	}

	publicKey, err := km.GetPublicKey(privateKey)
	if err != nil {
		t.Fatalf("Failed to get public key: %v", err)
	}

	if publicKey == "" {
		t.Fatal("Expected non-empty public key")
	}

	// Verify it's valid base64
	_, err = base64.StdEncoding.DecodeString(publicKey)
	if err != nil {
		t.Fatalf("Public key is not valid base64: %v", err)
	}

	// Verify public key is different from private key
	if publicKey == privateKey {
		t.Fatal("Public key should be different from private key")
	}

	t.Run("fails on invalid private key", func(t *testing.T) {
		_, err := km.GetPublicKey("invalid")
		if err == nil {
			t.Fatal("Expected error for invalid private key")
		}
	})
}