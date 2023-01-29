package features

import (
	"Projekat/structures"
	"fmt"
)

func GET(mem *structures.Memtable, cache *structures.LRUCache, bloomf structures.BloomF) {
	key := ""
	for key == "" {
		fmt.Print("Unesite ključ -> ")
		fmt.Scanln(&key)
	}
	found, value := mem.Find(key)
	if found {
		fmt.Print("Pronadjen je i vrednost je ", value)
		return
	}
	found, elem := cache.Found(structures.Element{Key: key})
	if found {
		fmt.Println("Pronadjen je i vrednost je ", elem.Value)
		return
	}

	found = bloomf.Query(key)
	if !found {
		fmt.Println("Element sa trazenim kljucem nije pronadjen.")
		return
	}

}
