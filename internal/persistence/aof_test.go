package persistence

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAOFPermissions(t *testing.T) {
	tempDir := t.TempDir()
	aofPath := filepath.Join(tempDir, "test.aof")

	aof, err := OpenAOF(aofPath, FsyncEverySec)
	if err != nil {
		t.Fatalf("Failed to open AOF: %v", err)
	}
	defer aof.Close()

	info, err := os.Stat(aofPath)
	if err != nil {
		t.Fatalf("Failed to stat AOF file: %v", err)
	}

	perm := info.Mode().Perm()
	if perm != 0600 {
		t.Fatalf("Expected AOF permissions to be 0600 (owner only), got %04o", perm)
	}
}
