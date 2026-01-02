package db

import "github.com/WhyIsEmerald/GPDB/internals/generics"

type entry[V any] struct {
	value       V
	isTombstone bool
}

type MemTable[K generics.Ordered, V any] struct {
	data map[K]entry[V]
}

func NewMemTable[K generics.Ordered, V any]() *MemTable[K, V] {
	return &MemTable[K, V]{
		data: make(map[K]entry[V]),
	}
}

func (m *MemTable[K, V]) Put(key K, value V) {
	m.data[key] = entry[V]{value: value, isTombstone: false}
}

func (m *MemTable[K, V]) Delete(key K) {
	var zero V
	m.data[key] = entry[V]{value: zero, isTombstone: true}
}

func (m *MemTable[K, V]) Get(key K) (entry[V], bool) {
	e, ok := m.data[key]
	return e, ok
}
