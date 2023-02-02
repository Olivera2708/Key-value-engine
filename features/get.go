package features

import (
	"Projekat/structures"
	"fmt"
	"os"
)

func GET(mem *structures.Memtable, cache *structures.LRUCache, bloomf structures.BloomF, sstableType int, level int, summaryBlockingFactor int) {
	key := ""
	return_value := []byte{}
	for key == "" {
		fmt.Print("Unesite ključ -> ")
		fmt.Scanln(&key)
	}
	found, value := mem.Find(key)
	if found {
		cache.Add(structures.Element{Key: key, Element: value})
		if value != nil {
			fmt.Println("Pronađen je i vrednost je ", string(value))
			return
		}
	}
	fmt.Println("Nema mem")
	found, elem := cache.Found(structures.Element{Key: key})
	if found {
		cache.Add(structures.Element{Key: key, Element: value})
		return_value = elem.Value.(structures.Element).Element
		fmt.Println("Pronađen je i vrednost je ", string(return_value))
		return
	}
	fmt.Println("Nema cache")

	found = bloomf.Query(key)
	if !found {
		fmt.Println("Ne postoji vrednost sa datim ključem")
		return
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
				if found {
					fmt.Println("Pronađen je i vrednost je ", string(value))
					return
				}
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				found, value = structures.ReadSingleSummary("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", key, summaryBlockingFactor)
				if found {
					fmt.Println("Pronađen je i vrednost je ", string(value))
					return
				}
			}

			if found {
				cache.Add(structures.Element{Key: key, Element: value})
				fmt.Println("Pronađen je i vrednost je ", string(value))
				return
			}
		}
	}
	fmt.Println("Ne postoji vrednost sa datim ključem")
}
