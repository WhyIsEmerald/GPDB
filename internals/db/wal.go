package db

import (
	"encoding/gob"
	"io"
	"os"

	"github.com/WhyIsEmerald/GPDB/internals/generics"
)

type WAL[K generics.Ordered, V any] struct {
	file    *os.File
	encoder *gob.Encoder
}

type WALEntry[K generics.Ordered, V any] struct {
	Key   K
	Entry generics.Entry[V]
}

func NewWAL[K generics.Ordered, V any](path string) (*WAL[K, V], error) {
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL[K, V]{
		file:    file,
		encoder: gob.NewEncoder(file),
	}, nil
}

func (wal *WAL[K, V]) Write(key K, entry generics.Entry[V]) error {
	walEntry := WALEntry[K, V]{Key: key, Entry: entry}
	return wal.encoder.Encode(&walEntry)
}

func ReplayWAL[K generics.Ordered, V any](path string) (*MemTable[K, V], error) {
	memtable := NewMemTable[K, V]()
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return memtable, nil
		}
		return nil, err
	}
	defer file.Close()

	decoder := gob.NewDecoder(file)
	for {
		var walEntry WALEntry[K, V]
		if err := decoder.Decode(&walEntry); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		memtable.data[walEntry.Key] = walEntry.Entry
	}

	return memtable, nil
}
