package db

import (
	"encoding/gob"
	"io"
	"os"
	"sort"

	"github.com/WhyIsEmerald/GPDB/internals/generics"
)

type SSTable[K generics.Ordered, V any] struct {
	path string
}

type Pair[K generics.Ordered, V any] struct {
	Key         K
	Value       V
	IsTombstone bool
}

func writeSSTable[K generics.Ordered, V any](memtable *MemTable[K, V], path string) (*SSTable[K, V], error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pairs := make([]Pair[K, V], 0, len(memtable.data))
	for k, e := range memtable.data {
		pairs = append(pairs, Pair[K, V]{Key: k, Value: e.Value, IsTombstone: e.IsTombstone})
	}

	sort.Slice(pairs, func(i, j int) bool {
		return generics.Compare(pairs[i].Key, pairs[j].Key) < 0
	})

	encoder := gob.NewEncoder(file)
	for _, pair := range pairs {
		if err := encoder.Encode(pair); err != nil {
			return nil, err
		}
	}

	return &SSTable[K, V]{path: path}, nil
}

func (s *SSTable[K, V]) Get(key K) (V, error) {
	file, err := os.Open(s.path)
	if err != nil {
		var zero V
		return zero, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)

	for {
		var pair Pair[K, V]
		if err := decoder.Decode(&pair); err != nil {
			if err == io.EOF {
				break
			}
			var zero V
			return zero, err
		}

		cmp := generics.Compare(pair.Key, key)
		if cmp == 0 {
			if pair.IsTombstone {
				var zero V
				return zero, errNotFound
			}
			return pair.Value, nil
		}

		if cmp > 0 {
			var zero V
			return zero, errNotFound
		}
	}

	var zero V
	return zero, errNotFound
}
