package builtins

import (
	"os"
	"testing"
)

func TestRemove(t *testing.T) {
	// Create a temporary file for testing
	tempFile := "testfile.txt"
	file, err := os.Create(tempFile)
	if err != nil {
		t.Fatalf("Failed to create temporary file: %v", err)
	}
	file.Close()
	defer os.Remove(tempFile)

	// Test removing the file
	err = Remove(tempFile)
	if err != nil {
		t.Fatalf("Failed to remove file: %v", err)
	}

	// Check if the file still exists
	_, err = os.Stat(tempFile)
	if !os.IsNotExist(err) {
		t.Fatalf("File was not removed: %v", err)
	}
}
