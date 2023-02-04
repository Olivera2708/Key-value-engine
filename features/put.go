package features

import (
	"Projekat/structures"
	"fmt"
	"strings"
)

func PUT(wal *structures.WAL, mem *structures.Memtable, cache *structures.LRUCache, generation *int, bloomf structures.BloomF, sstableType int, percentage int, summaryBlockingFactor int, HLLp int, CMSp float64, CMSd float64, BFn int, BFp float64) {

	key := ""
	for true {
		fmt.Print("Unesite ključ -> ")
		fmt.Scanln(&key)
		if !strings.Contains(key, "-") && strings.ReplaceAll(key, " ", "") != "" {
			break
		} else {
			fmt.Println("Neispravan ključ")
		}
	}

	//ispis sta zeli da doda
	fmt.Println("-----------------------------------------------")
	fmt.Println("|               OPCIJE DODAVANJA              |")
	fmt.Println("|                                             |")
	fmt.Println("| 1. String                                   |")
	fmt.Println("| 2. HyperLogLog                              |")
	fmt.Println("| 3. Count min sketch                         |")
	fmt.Println("| 4. Bloom Filter                             |")
	fmt.Println("| 5. SimHash                                  |")
	fmt.Println("-----------------------------------------------")
	num := 0
	for true {
		fmt.Print("Unesite jedan od ponuđenih brojeva -> ")
		fmt.Scan(&num)
		if num > 0 && num < 6 {
			break
		}
	}

	bloomf.Add(key) //bitno dodati pre nastavka
	var value []byte

	elem := structures.Element{}
	if num == 1 {
		for len(value) == 0 {
			fmt.Print("Unesite vrednost -> ")
			fmt.Scan(&value)
			elem = structures.Element{Key: key, Elem: value, Type: ""}
		}
	} else if num == 2 { // ne radi
		hll := structures.CreateHLL(uint8(HLLp))
		key += "-hll"
		value = hll.SerializeHLL()
		elem = structures.Element{Key: key, Elem: value, Type: "hll"}
	} else if num == 3 { //radi
		cms := structures.CreateCMS(CMSp, CMSd)
		key += "-cms"
		value = cms.SerializeCMS()
		elem = structures.Element{Key: key, Elem: value, Type: "cms"}
	} else if num == 4 { //radi
		bloomf := structures.CreateBloomFilter(uint(BFn), BFp)
		key += "-bloom"
		value = bloomf.SerializeBloom()
		elem = structures.Element{Key: key, Elem: value, Type: "bloom"}
	} else { //ne radi i malo nema smisla
		simHash := structures.CreateSimHash()
		key += "-simHash"
		value = simHash.SerializeSimHash()
		elem = structures.Element{Key: key, Elem: value, Type: "simHash"}
	}

	timestamp := wal.Add(key, value, 0)
	mem.Add(key, value, 0, timestamp) //0 znaci da je aktivan

	cache.Add(elem)

	wal.Low_water_mark = mem.Flush(generation, sstableType, percentage, summaryBlockingFactor)
	wal.Flush()
	bloomf.WriteGlobal()
}
