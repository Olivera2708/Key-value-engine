package features

import (
	"Projekat/structures"
	"fmt"
)

func PUT(wal *structures.WAL, mem *structures.Memtable, cache *structures.LRUCache, generation *int) {
	key := ""
	for key == "" {
		fmt.Print("Unesite ključ -> ")
		fmt.Scanln(&key)
	}

	var value []byte
	for len(value) == 0 {
		fmt.Print("Unesite vrednost -> ")
		fmt.Scanln(&value)
	}

	wal.Add(key, value, 0)
	mem.Add(key, value, 0) //0 znaci da je aktivan

	elem := structures.Element{Key: key, Element: value}
	cache.Add(elem)

	mem.Flush(generation)
	wal.Flush()
}
