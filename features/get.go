package features

import (
	"Projekat/structures"
	"fmt"
	"os"
	"strings"
)

func GET(mem *structures.Memtable, cache *structures.LRUCache, bloomf structures.BloomF, sstableType int, level int, summaryBlockingFactor int, generation int, wal *structures.WAL, precentage int) {
	key := ""
	return_value := []byte{}
	for key == "" {
		fmt.Print("Unesite ključ -> ")
		fmt.Scanln(&key)
	}
	found, value, new_key := mem.Find(key)
	if found {
		cache.Add(structures.Element{Key: new_key, Element: value})
		if value != nil {
			WriteFound(new_key, value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
			return
		}
	}
	fmt.Println("Nema mem")
	found, elem := cache.Found(structures.Element{Key: key})
	if found {
		cache.Add(structures.Element{Key: key, Element: value})
		return_value = elem.Value.(structures.Element).Element
		WriteFound(elem.Value.(structures.Element).Key, return_value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
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
				found, value, new_key = structures.ReadSummary("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), key)
				if found {
					cache.Add(structures.Element{Key: key, Element: value})
					WriteFound(new_key, value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
					return
				}
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				found, value, new_key = structures.ReadSingleSummary("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", key, summaryBlockingFactor)
				if found {
					cache.Add(structures.Element{Key: key, Element: value})
					WriteFound(new_key, value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
					return
				}
			}
		}
	}
	fmt.Println("Ne postoji vrednost sa datim ključem")
}

func WriteFound(key string, value []byte, wal *structures.WAL, mem *structures.Memtable, generation *int, sstableType int, percentage int, summaryBlockingFactor int) {
	write_value := []byte{}
	if strings.Contains(key, "-bloom") {
		fmt.Println("Pronađen je i vrednost je tipa bloom filter")
		bf := bloomMenu(key, value)
		write_value = bf.SerializeBloom()
	} else if strings.Contains(key, "-cms") {
		fmt.Println("Pronađen je i vrednost je tipa count min sketch")
		cms := CMSMenu(key, value)
		write_value = cms.SerializeCMS()
	} else if strings.Contains(key, "-simHash") {
		fmt.Println("Pronađen je i vrednost je tipa sim hash")
		sh := SHMenu(key, value)
		write_value = sh.SerializeSimHash()
	} else if strings.Contains(key, "-hll") {
		fmt.Println("Pronađen je i vrednost je tipa hyper log log")
		hll := HLLMenu(key, value)
		write_value = hll.SerializeHLL()
	} else {
		fmt.Println("Pronađen je i vrednost je tipa string -> ", string(value))
		return
	}
	WriteUpdated(key, write_value, wal, mem, generation, sstableType, percentage, summaryBlockingFactor)
}

func WriteUpdated(key string, value []byte, wal *structures.WAL, mem *structures.Memtable, generation *int, sstableType int, percentage int, summaryBlockingFactor int) {
	timestamp := wal.Add(key, value, 0)
	mem.Add(key, value, 0, timestamp) //0 znaci da je aktivan
	mem.Flush(generation, sstableType, percentage, summaryBlockingFactor)
	wal.Flush()
}

func bloomMenu(key string, value []byte) *structures.BloomF {
	bf := structures.DeserializeBloom(value)
	a := ""
	fmt.Println("\n-----------------------------------------------")
	fmt.Println("|                BLOOM FILTER                 |")
	fmt.Println("|                                             |")
	fmt.Println("| 1. Dodavanje (ADD)                          |")
	fmt.Println("| 2. Provera (QUERY)                          |")
	fmt.Println("|                                             |")
	fmt.Println("|                       Za izlaz ukucajte 'x' |")
	fmt.Println("-----------------------------------------------")
	for {
		fmt.Print("\nIzaberite opciju -> ")
		fmt.Scanln(&a)

		if a == "x" {
			break
		} else if a == "1" {
			fmt.Print("Unesite vrednost -> ")
			input := ""
			fmt.Scan(&input)
			bf.Add(input)
			fmt.Println("Vrednost uneta")
		} else if a == "2" {
			fmt.Print("Unesite vrednost -> ")
			input := ""
			fmt.Scan(&input)
			found := bf.Query(input)
			if found {
				fmt.Println("Vrednost verovatno pronađena")
			} else {
				fmt.Println("Vrednost nije pronađena")
			}
		}
	}
	return bf
}

func CMSMenu(key string, value []byte) *structures.CMS {
	cms := structures.DeserializeCMS(value)
	a := ""
	fmt.Println("\n-----------------------------------------------")
	fmt.Println("|             COUNT MIN SKETCH                |")
	fmt.Println("|                                             |")
	fmt.Println("| 1. Dodavanje (ADD)                          |")
	fmt.Println("| 2. Provera (QUERY)                          |")
	fmt.Println("|                                             |")
	fmt.Println("|                       Za izlaz ukucajte 'x' |")
	fmt.Println("-----------------------------------------------")
	for {
		fmt.Print("\nIzaberite opciju -> ")
		fmt.Scanln(&a)

		if a == "x" {
			break
		} else if a == "1" {
			fmt.Print("Unesite vrednost -> ")
			input := ""
			fmt.Scan(&input)
			cms.Add(input)
			fmt.Println("Vrednost uneta")
		} else if a == "2" {
			fmt.Print("Unesite vrednost -> ")
			input := ""
			fmt.Scan(&input)
			ret_val := cms.Query(input)
			fmt.Println("Broj ponavljanja vrednosti je -> " + fmt.Sprint(ret_val))
		}
	}
	return cms
}

func HLLMenu(key string, value []byte) *structures.HLL {
	hll := structures.DeserializeHLL(value)
	a := ""
	fmt.Println("\n-----------------------------------------------")
	fmt.Println("|               HYPER LOG LOG                 |")
	fmt.Println("|                                             |")
	fmt.Println("| 1. Dodavanje (ADD)                          |")
	fmt.Println("| 2. Procena (ESTIMATE)                       |")
	fmt.Println("|                                             |")
	fmt.Println("|                       Za izlaz ukucajte 'x' |")
	fmt.Println("-----------------------------------------------")
	for {
		fmt.Print("\nIzaberite opciju -> ")
		fmt.Scanln(&a)

		if a == "x" {
			break
		} else if a == "1" {
			fmt.Print("Unesite vrednost -> ")
			input := ""
			fmt.Scan(&input)
			hll.Add(input)
			fmt.Println("Vrednost uneta")
		} else if a == "2" {
			ret_val := hll.Estimate()
			fmt.Println("Broj različitih vrednosti u strukturi -> " + fmt.Sprint(ret_val))
		}
	}
	return hll
}

// ovo sve mora da se sredi
func SHMenu(key string, value []byte) *structures.SimHash {
	sh := structures.DeserializeSimHash(value)
	// a := ""
	fmt.Println("\n-----------------------------------------------")
	fmt.Println("|                  SIM HASH                   |")
	fmt.Println("|                                             |")
	fmt.Println("| 1. Dodavanje (ADD)                          |")
	fmt.Println("| 2. Procena (ESTIMATE)                       |")
	fmt.Println("|                                             |")
	fmt.Println("|                       Za izlaz ukucajte 'x' |")
	fmt.Println("-----------------------------------------------")
	// for {
	// 	fmt.Print("\nIzaberite opciju -> ")
	// 	fmt.Scanln(&a)

	// 	if a == "x" {
	// 		break
	// 	} else if a == "1" {
	// 		fmt.Print("Unesite vrednost -> ")
	// 		input := ""
	// 		fmt.Scan(&input)
	// 		sh.Add(input)
	// 		fmt.Println("Vrednost uneta")
	// 	} else if a == "2" {
	// 		ret_val := hll.Estimate()
	// 		fmt.Println("Broj različitih vrednosti u strukturi -> " + fmt.Sprint(ret_val))
	// 	}
	// }
	return sh
}
