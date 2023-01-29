package features

import (
	"Projekat/structures"
	"fmt"
)

func DELETE(wal *structures.WAL, mem *structures.Memtable, cache *structures.LRUCache) bool {
	key := ""
	for key == "" {
		fmt.Print("Unesite ključ -> ")
		fmt.Scanln(&key)
	}

	timestamp := wal.Add(key, nil, 1)
	mem.Add(key, nil, 1, timestamp)

	elem := structures.Element{Key: key}
	suc := cache.Delete(elem)

	wal.Flush()
	return suc
}
