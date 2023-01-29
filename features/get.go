package features

import (
	"Projekat/structures"
	"fmt"
	"os"
)

func GET(mem *structures.Memtable, cache *structures.LRUCache, bloomf structures.BloomF) []byte {
	key := "4"
	// for key == "" {
	// 	fmt.Print("Unesite ključ -> ")
	// 	fmt.Scanln(&key)
	// }
	found, value := mem.Find(key)
	if found {
		return value
	}
	fmt.Println("Nema mem")
	found, elem := cache.Found(structures.Element{Key: key})
	if found {
		return elem.Value.(structures.Element).Element
	}
	fmt.Println("Nema cache")

	found = bloomf.Query(key)
	if !found {
		return nil
	}
	fmt.Println("Nema bloom")

	for i := 0; true; i++ {
		_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(i)+"-summary.db", os.O_RDONLY, 0666)
		if os.IsNotExist(err) {
			break
		}
		found, value = structures.ReadSummary("data/sstables/usertable-"+fmt.Sprint(i), key)
		return value
	}
	return nil
}
