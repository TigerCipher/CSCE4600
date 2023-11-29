package builtins

import (
	"os"
	"testing"
)

func TestMakeDirectory(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := "testdir"
	err := os.Mkdir(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.Remove(tempDir)

	// Test creating a new directory
	dirName := "newdir"
	err = MakeDirectory(dirName)
	if err != nil {
		t.Fatalf("Failed to create directory: %v", err)
	}

	// Check if the directory exists
	_, err = os.Stat(dirName)
	if err != nil {
		t.Fatalf("Failed to retrieve directory information: %v", err)
	}
}
