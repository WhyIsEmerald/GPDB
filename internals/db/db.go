package db

import "errors"

type DB[K comparable, V any] struct {
	memtable *MemTable[K, V]
}

func NewDB[K comparable, V any]() (*DB[K, V], error) {
	memtable := NewMemTable[K, V]()
	return &DB[K, V]{
		memtable: memtable,
	}, nil
}

func (db *DB[K, V]) Put(key K, value V) error {
	db.memtable.Put(key, value)
	return nil
}

func (db *DB[K, V]) Get(key K) (V, error) {
	if val, ok := db.memtable.Get(key); ok {
		return val, nil
	}
	var zero V
	return zero, errors.New("key not found")
}
