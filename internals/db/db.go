package db

import (
	"fmt"
	"path/filepath"

	"github.com/WhyIsEmerald/GPDB/internals/generics"
)

type DB[K generics.Ordered, V any] struct {
	memtable        *MemTable[K, V]
	maxMemtableSize int
	memtableSize    int
	sstables        []*SSTable[K, V]
	sstableCounter  int
	sstablePath     string
	wal             *WAL[K, V]
	walPath         string
}

func NewDB[K generics.Ordered, V any](maxMemtableSize int, sstablePath, walPath string) (*DB[K, V], error) {
	memtable, err := ReplayWAL[K, V](walPath)
	if err != nil {
		return nil, err
	}
	wal, err := NewWAL[K, V](walPath)
	if err != nil {
		return nil, err
	}
	return &DB[K, V]{
		memtable:        memtable,
		maxMemtableSize: maxMemtableSize,
		memtableSize:    len(memtable.data),
		sstables:        make([]*SSTable[K, V], 0),
		sstablePath:     sstablePath,
		wal:             wal,
		walPath:         walPath,
	}, nil
}

func (db *DB[K, V]) Put(key K, value V) error {
	entry := generics.Entry[V]{Value: value, IsTombstone: false}
	if err := db.wal.Write(key, entry); err != nil {
		return err
	}

	if _, ok := db.memtable.data[key]; !ok {
		db.memtableSize++
	}
	db.memtable.data[key] = entry

	if db.memtableSize >= db.maxMemtableSize {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB[K, V]) Delete(key K) error {
	var zero V
	entry := generics.Entry[V]{Value: zero, IsTombstone: true}
	if err := db.wal.Write(key, entry); err != nil {
		return err
	}

	if _, ok := db.memtable.data[key]; !ok {
		db.memtableSize++
	}
	db.memtable.data[key] = entry

	if db.memtableSize >= db.maxMemtableSize {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB[K, V]) flushMemtable() error {
	sstableFileName := fmt.Sprintf("data-%d.sstable", db.sstableCounter)
	sstablePath := filepath.Join(db.sstablePath, sstableFileName)
	sstable, err := writeSSTable(db.memtable, sstablePath)
	if err != nil {
		return err
	}

	db.sstables = append(db.sstables, sstable)
	db.sstableCounter++
	db.memtable = NewMemTable[K, V]()
	db.memtableSize = 0

	return nil
}

func (db *DB[K, V]) Get(key K) (V, error) {
	if entry, ok := db.memtable.Get(key); ok {
		if entry.IsTombstone {
			var zero V
			return zero, errNotFound
		}
		return entry.Value, nil
	}

	for i := len(db.sstables) - 1; i >= 0; i-- {
		sstable := db.sstables[i]
		val, err := sstable.Get(key)
		if err != nil {
			if err == errNotFound {
				continue
			}
			var zero V
			return zero, err
		}
		return val, nil
	}

	var zero V
	return zero, errNotFound
}
