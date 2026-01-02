package db

import (
	"fmt"

	"github.com/WhyIsEmerald/GPDB/internals/generics"
)

type DB[K generics.Ordered, V any] struct {
	memtable        *MemTable[K, V]
	maxMemtableSize int
	memtableSize    int
	sstables        []*SSTable[K, V]
	sstableCounter  int
}

func NewDB[K generics.Ordered, V any](maxMemtableSize int) (*DB[K, V], error) {
	sstables := make([]*SSTable[K, V], 0)
	memtable := NewMemTable[K, V]()

	return &DB[K, V]{
		memtable:        memtable,
		maxMemtableSize: maxMemtableSize,
		sstables:        sstables,
	}, nil
}

func (db *DB[K, V]) Put(key K, value V) error {
	db.memtable.Put(key, value)
	db.memtableSize++

	if db.memtableSize >= db.maxMemtableSize {
		if err := db.flushMemtable(); err != nil {
			return err
		}
	}

	return nil
}

func (db *DB[K, V]) flushMemtable() error {
	sstablePath := fmt.Sprintf("data-%d.sstable", db.sstableCounter)
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
	if val, ok := db.memtable.Get(key); ok {
		return val, nil
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
