package features

import (
	"Projekat/global"
	"Projekat/structures"
	"bufio"
	"fmt"
	"os"
	"strings"
)

func GET(mem *structures.Memtable, cache *structures.LRUCache, bloomf structures.BloomF, sstableType int, summaryBlockingFactor int, generation int, wal *structures.WAL, precentage int) {
	key := ""
	return_value := []byte{}
	for key == "" {
		fmt.Print("Unesite ključ -> ")
		fmt.Scan(&key)
	}
	found, value, new_key := mem.Find(key)
	if found {
		if len(value) > 0 {
			cache.Add(structures.Element{Key: new_key, Elem: value})
			WriteFound(new_key, value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
			return
		} else {
			fmt.Println("Ne postoji vrednost sa datim ključem")
			return
		}
	}
	found, elem := cache.Found(structures.Element{Key: key})
	if found {
		return_value = elem.Value.(structures.Element).Elem
		new_key = key
		if elem.Value.(structures.Element).Type != "" {
			new_key += "-" + elem.Value.(structures.Element).Type
		}
		WriteFound(new_key, return_value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
		return
	}

	found = bloomf.Query(key)
	if !found {
		fmt.Println("Ne postoji vrednost sa datim ključem")
		return
	}

	for lvl := 0; lvl < global.LSMTreeLevel; lvl++ {
		generation := 0
		for j := 0; true; j++ {
			var file *os.File
			var err error
			if sstableType == 2 {
				file, err = os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)
			} else {
				file, err = os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)
			}

			if os.IsNotExist(err) {
				break
			}
			generation += 1
			file.Close()
		}
		for i := generation - 1; i >= 0; i-- {
			if sstableType == 2 {
				_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-summary.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				found, value, new_key = structures.ReadSummary("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), key)
				if found && len(value) > 0 {
					cache.Add(structures.Element{Key: key, Elem: value})
					WriteFound(new_key, value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
					return
				}
				if found && len(value) == 0 {
					fmt.Println("Ne postoji vrednost sa datim ključem")
					return
				}
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				found, value, new_key = structures.ReadSingleSummary("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", key, summaryBlockingFactor)
				if found && len(value) > 0 {
					cache.Add(structures.Element{Key: key, Elem: value})
					WriteFound(new_key, value, wal, mem, &generation, sstableType, precentage, summaryBlockingFactor)
					return
				}
				if found && len(value) == 0 {
					fmt.Println("Ne postoji vrednost sa datim ključem")
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
		fmt.Scan(&a)

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
		fmt.Scan(&a)

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
		fmt.Scan(&a)

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

func SHMenu(key string, value []byte) *structures.SimHash {
	sh := structures.DeserializeSimHash(value)
	a := ""
	fmt.Println("\n-----------------------------------------------")
	fmt.Println("|                  SIM HASH                   |")
	fmt.Println("|                                             |")
	fmt.Println("| 1. Dodavanje (ADD)                          |")
	fmt.Println("| 2. Hemingova udaljenost                     |")
	fmt.Println("|                                             |")
	fmt.Println("|                       Za izlaz ukucajte 'x' |")
	fmt.Println("-----------------------------------------------")
	for {
		fmt.Print("\nIzaberite opciju -> ")
		fmt.Scan(&a)

		if a == "x" {
			break
		} else if a == "1" {
			fmt.Print("Unesite ključ -> ")
			input_key := ""
			fmt.Scanln(&input_key)
			fmt.Print("Unesite tekst -> ")
			input_val := ""
			scanner := bufio.NewScanner(os.Stdin)
			if scanner.Scan() {
				input_val = scanner.Text()
			}
			sh.Add(input_key, input_val)
			fmt.Println("Vrednost uneta")
		} else if a == "2" {
			fmt.Print("Unesite prvi ključ -> ")
			input_key1 := ""
			fmt.Scan(&input_key1)
			fmt.Print("Unesite drugi ključ -> ")
			input_key2 := ""
			fmt.Scan(&input_key2)
			ret_val := sh.Compare(input_key1, input_key2)
			if ret_val == -1 {
				fmt.Println("Nije pronađen element sa zadatim ključem")
			} else {
				fmt.Println("Hemingova udaljenost -> " + fmt.Sprint(ret_val))
			}
		}
	}
	return sh
}
