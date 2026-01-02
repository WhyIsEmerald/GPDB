package db

import "github.com/WhyIsEmerald/GPDB/internals/generics"

type MemTable[K generics.Ordered, V any] struct {
	data map[K]V
}

func NewMemTable[K generics.Ordered, V any]() *MemTable[K, V] {
	return &MemTable[K, V]{
		data: make(map[K]V),
	}
}

func (m *MemTable[K, V]) Put(key K, value V) {
	m.data[key] = value
}

func (m *MemTable[K, V]) Get(key K) (V, bool) {
	value, ok := m.data[key]
	var zero V
	if !ok {
		return zero, false
	}
	return value, true
}
