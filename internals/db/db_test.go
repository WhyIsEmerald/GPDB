package db

import (
	"os"
	"path/filepath"
	"testing"
)

func setupTest(t *testing.T) (walPath, sstablePath string, cleanup func()) {
	t.Helper()
	testDir, err := os.MkdirTemp("", "db_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	walPath = filepath.Join(testDir, "test.wal")
	sstablePath = filepath.Join(testDir, "sstables")
	if err := os.MkdirAll(sstablePath, os.ModePerm); err != nil {
		t.Fatalf("Failed to create sstables dir: %v", err)
	}

	cleanup = func() {
		os.RemoveAll(testDir)
	}
	return
}

func TestDB_Integration(t *testing.T) {
	walPath, sstablePath, cleanup := setupTest(t)
	defer cleanup()

	db, err := NewDB[string, string](3, sstablePath, walPath)
	if err != nil {
		t.Fatalf("Failed to create DB: %v", err)
	}

	t.Run("Put and Get from memtable", func(t *testing.T) {
		if err := db.Put("a", "apple"); err != nil {
			t.Fatalf("Put failed: %v", err)
		}
		val, err := db.Get("a")
		if err != nil || val != "apple" {
			t.Fatalf("Get failed: got val=%v, err=%v", val, err)
		}
	})

	t.Run("Delete from memtable", func(t *testing.T) {
		if err := db.Delete("a"); err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
		_, err := db.Get("a")
		if err != errNotFound {
			t.Fatalf("Expected errNotFound, got %v", err)
		}
	})

	t.Run("Flush to sstable and get", func(t *testing.T) {
		db.Put("b", "banana")
		db.Put("c", "cherry") // Flushes here
		db.Put("d", "date")

		// Test get from sstable
		val, err := db.Get("b")
		if err != nil || val != "banana" {
			t.Fatalf("Get from sstable failed: got val=%v, err=%v", val, err)
		}

		// Test that tombstone was flushed
		_, err = db.Get("a")
		if err != errNotFound {
			t.Fatalf("Expected errNotFound for flushed tombstone, got %v", err)
		}
	})

	t.Run("WAL Replay", func(t *testing.T) {
		reopenedDb, err := NewDB[string, string](3, sstablePath, walPath)
		if err != nil {
			t.Fatalf("Failed to create DB from WAL: %v", err)
		}

		// Check replayed value
		val, err := reopenedDb.Get("b")
		if err != nil || val != "banana" {
			t.Fatalf("Get from reopened DB failed: got val=%v, err=%v", val, err)
		}

		// Check replayed tombstone
		_, err = reopenedDb.Get("a")
		if err != errNotFound {
			t.Fatalf("Expected errNotFound for replayed tombstone, got %v", err)
		}
	})
}
