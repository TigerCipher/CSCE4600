package builtins

import (
	"os"
	"testing"
)

func TestListFiles(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := "testdir"
	err := os.Mkdir(tempDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create temporary directory: %v", err)
	}
	defer os.Remove(tempDir)

	// Create some test files
	testFiles := []string{"file1.txt", "file2.txt", "file3.txt"}
	for _, filename := range testFiles {
		file, err := os.Create(tempDir + "/" + filename)
		if err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
		file.Close()
		defer os.Remove(tempDir + "/" + filename)
	}

	// Test listing files in the directory
	err = ListFiles(tempDir)
	if err != nil {
		t.Fatalf("Failed to list files: %v", err)
	}

	// Test listing files in a non-existent directory
	err = ListFiles("nonexistentdir")
	if err == nil {
		t.Fatalf("Expected error when listing files in non-existent directory")
	}

	// Test listing files with reverse order
	err = ListFiles("-r", tempDir)
	if err != nil {
		t.Fatalf("Failed to list files with reverse order: %v", err)
	}

	// Test listing files with long format
	err = ListFiles("-l", tempDir)
	if err != nil {
		t.Fatalf("Failed to list files with long format: %v", err)
	}

	// Test listing files with hidden files shown
	err = ListFiles("-a", tempDir)
	if err != nil {
		t.Fatalf("Failed to list files with hidden files shown: %v", err)
	}

	// Test listing files sorted by size
	err = ListFiles("-S", tempDir)
	if err != nil {
		t.Fatalf("Failed to list files sorted by size: %v", err)
	}

	// Test listing files with human-readable sizes
	err = ListFiles("-h", "-l", tempDir)
	if err != nil {
		t.Fatalf("Failed to list files with human-readable sizes: %v", err)
	}

}
