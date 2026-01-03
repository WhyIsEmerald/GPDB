package db

import (
	"os"
	"path/filepath"
	"testing"
)

func setupTest(t *testing.T) (walPath, sstablePath, manifestPath string) {
	t.Helper()
	testDir, err := os.MkdirTemp("", "db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(testDir)
	})

	walPath = filepath.Join(testDir, "test.wal")
	sstablePath = filepath.Join(testDir, "sstables")
	manifestPath = filepath.Join(sstablePath, "manifest")
	if err := os.MkdirAll(sstablePath, os.ModePerm); err != nil {
		t.Fatalf("Failed to create sstables dir: %v", err)
	}
	return
}

func TestDB_Integration(t *testing.T) {
	walPath, sstablePath, manifestPath := setupTest(t)

	db, err := NewDB[string, string](3, sstablePath, walPath, manifestPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	// 1. Put and Get from memtable
	if err := db.Put("a", "apple"); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	val, err := db.Get("a")
	if err != nil || val != "apple" {
		t.Fatalf("Get failed: got val=%v, err=%v", val, err)
	}

	// 2. Delete from memtable
	if err := db.Delete("a"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	_, err = db.Get("a")
	if err != errNotFound {
		t.Fatalf("Expected errNotFound, got %v", err)
	}

	// 3. Flush to sstable and get
	db.Put("b", "banana")
	db.Put("c", "cherry") // Flushes here
	db.Put("d", "date")

	// Test get from sstable
	val, err = db.Get("b")
	if err != nil || val != "banana" {
		t.Fatalf("Get from sstable failed: got val=%v, err=%v", val, err)
	}

	// Test that tombstone was flushed
	_, err = db.Get("a")
	if err != errNotFound {
		t.Fatalf("Expected errNotFound for flushed tombstone, got %v", err)
	}

	// Before reopening, let's check how many sstables we expect.
	if len(db.sstables) != 1 {
		t.Fatalf("Expected 1 sstable, got %d", len(db.sstables))
	}

	// 4. Restart and check state
	reopenedDb, err := NewDB[string, string](3, sstablePath, walPath, manifestPath)
	if err != nil {
		t.Fatalf("Failed to create DB from WAL and manifest: %v", err)
	}

	// Check that the sstable was loaded from the manifest
	if len(reopenedDb.sstables) != 1 {
		t.Fatalf("Reopened DB should have loaded 1 sstable from manifest, got %d", len(reopenedDb.sstables))
	}

	// Check replayed value from WAL ('d' was in memtable, not flushed)
	val, err = reopenedDb.Get("d")
	if err != nil || val != "date" {
		t.Fatalf("Get from reopened DB failed for memtable value: got val=%v, err=%v", val, err)
	}

	// Check value from sstable in reopened DB
	val, err = reopenedDb.Get("b")
	if err != nil || val != "banana" {
		t.Fatalf("Get from reopened DB failed for sstable value: got val=%v, err=%v", val, err)
	}

	// Check replayed tombstone
	_, err = reopenedDb.Get("a")
	if err != errNotFound {
		t.Fatalf("Expected errNotFound for replayed tombstone, got %v", err)
	}
}
