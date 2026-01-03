package db

import "github.com/WhyIsEmerald/GPDB/internals/generics"

type MemTable[K generics.Ordered, V any] struct {
	data map[K]generics.Entry[V]
}

func NewMemTable[K generics.Ordered, V any]() *MemTable[K, V] {
	return &MemTable[K, V]{
		data: make(map[K]generics.Entry[V]),
	}
}

func (m *MemTable[K, V]) Get(key K) (generics.Entry[V], bool) {
	e, ok := m.data[key]
	return e, ok
}
