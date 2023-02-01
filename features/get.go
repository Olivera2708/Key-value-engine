package features

import (
	"Projekat/structures"
	"fmt"
	"os"
)

func GET(mem *structures.Memtable, cache *structures.LRUCache, bloomf structures.BloomF, sstableType int, level int, summaryBlockingFactor int) []byte {
	key := ""
	for key == "" {
		fmt.Print("Unesite ključ -> ")
		fmt.Scanln(&key)
	}
	found, value := mem.Find(key)
	if found {
		cache.Add(structures.Element{Key: key, Element: value})
		return value
	}
	fmt.Println("Nema mem")
	found, elem := cache.Found(structures.Element{Key: key})
	if found {
		cache.Add(structures.Element{Key: key, Element: value})
		return elem.Value.(structures.Element).Element
	}
	fmt.Println("Nema cache")

	found = bloomf.Query(key)
	if !found {
		return nil
	}
	fmt.Println("Nema bloom")

	for lvl := 0; lvl < level; lvl++ {
		for i := 0; true; i++ {
			if sstableType == 2 {
				_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-summary.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				found, value = structures.ReadSummary("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), key)
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				found, value = structures.ReadSingleSummary("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", key, summaryBlockingFactor)
			}

			if found {
				cache.Add(structures.Element{Key: key, Element: value})
				return value
			}
		}
	}

	return nil
}
