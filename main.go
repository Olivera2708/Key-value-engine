package main

import (
	"Projekat/features"
	"Projekat/global"
	"Projekat/structures"
	"container/list"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"time"
)

func tocken_bucket(time_flush int, number int, all_el *list.List) bool {
	num_el := 0
	time_now := time.Now().Unix()

	e := all_el.Front()
	for e != nil {
		if time_now-e.Value.(int64) >= int64(time_flush) {
			next := e.Next()
			all_el.Remove(e)
			e = next
		} else {
			num_el++
			e = e.Next()
		}
	}
	if num_el < number {
		all_el.PushBack(time_now)
		return true
	}
	return false
}

func main() {
	generation := 0
	WALSegmentationFactor := 5.0
	global.SkipListMaxHeight = 5
	memTableMaxCap := 5.0
	memTableFlush := 80.0
	global.MemTableDataType = 1 // skipList -> 1, BStablo -> 2
	cacheSize := 14.0
	SSTableType := 2.0 // single -> 1, multiple -> 2
	summaryBlockingFactor := 20.0
	global.LSMTreeLevel = 4
	global.LSMAlgorithm = 1 //size -> 1, lleveled -> 2
	global.LSMMinimum = 3
	TokenTime := 10.0
	TokenNumber := 5.0
	global.ResultsNumber = 5
	HLLp := 8.0      // broj vodecih bajtova
	CMSp := 0.1      // preciznost
	CMSd := 0.01     // tacnost
	global.BFn = 20  // broj elemenata
	global.BFp = 0.1 //preciznost
	global.BTreeN = 2
	configFile, err := ioutil.ReadFile("config/config.json")
	if err == nil {

		var payload map[string]map[string]float64
		err = json.Unmarshal(configFile, &payload)
		if err != nil {
			log.Fatal(err)
		}

		WALSegmentationFactor = payload["WAL"]["WALSegmentationFactor"]
		global.SkipListMaxHeight = int(payload["SkipList"]["skipListMaxHeight"])
		memTableMaxCap = payload["MemTable"]["memTableMaxCap"]
		memTableFlush = payload["MemTable"]["memTableFlush"]
		global.MemTableDataType = int(payload["MemTable"]["memTableType"])
		cacheSize = payload["LRUCache"]["cacheSize"]
		SSTableType = payload["SSTable"]["SSTableType"]
		summaryBlockingFactor = payload["SSTable"]["summaryBlockingFactor"]
		global.LSMTreeLevel = int(payload["LSMTree"]["LSMTreeLevel"])
		global.LSMAlgorithm = int(payload["LSMTree"]["LSMAlgorithm"])
		global.LSMMinimum = int(payload["LSMTree"]["LSMMinimum"])
		TokenTime = payload["TokenBucket"]["TokenTime"]
		TokenNumber = payload["TokenBucket"]["TokenNumber"]
		global.ResultsNumber = int(payload["Pagination"]["ResultsNumber"])
		HLLp = payload["HLL"]["p"]
		CMSp = float64(payload["CMS"]["p"])
		CMSd = float64(payload["CMS"]["d"])
		global.BFn = uint(payload["BloomFilter"]["n"])
		global.BFp = float64(payload["BloomFilter"]["p"])
		global.BTreeN = int(payload["BTree"]["n"])
	}

	//brojanje generacija
	for i := 0; true; i++ {
		if SSTableType == 2 {
			file, err := os.OpenFile("data/sstables/usertable-0-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
			if os.IsNotExist(err) {
				generation = i
				break
			}
			file.Close()
		} else {
			file, err := os.OpenFile("data/singlesstables/usertable-0-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
			if os.IsNotExist(err) {
				generation = i
				break
			}
			file.Close()
		}
	}

	//inicijalizacija
	wal := structures.CreateWAL(uint(WALSegmentationFactor))
	mem := structures.CreateMemtable(global.SkipListMaxHeight, uint(memTableMaxCap), 0)
	wal.ReadAll(*mem, generation, int(SSTableType))
	bloom := structures.ReadAll()
	cache := structures.CreateLRU(int(cacheSize))
	//ovo sve treba iz config fajla da se cita

	var a string = ""
	TokenList := list.New()

	for {
		fmt.Println("\n-----------------------------------------------")
		fmt.Println("|                   OPCIJE                    |")
		fmt.Println("|                                             |")
		fmt.Println("| 1. Dodavanje (PUT)                          |")
		fmt.Println("| 2. Dobavljenje (GET)                        |")
		fmt.Println("| 3. Brisanje (DELETE)                        |")
		fmt.Println("| 4. Pretraga (LIST)                          |")
		fmt.Println("| 5. Traženje (RANGE SCAN)                    |")
		fmt.Println("| 6. Kompakcija (LSM)                         |")
		fmt.Println("|                                             |")
		fmt.Println("|                       Za izlaz ukucajte 'x' |")
		fmt.Println("-----------------------------------------------")
		fmt.Print("\nIzaberite opciju -> ")
		//////////////////////////////////////////////////////////////////
		fmt.Scan(&a)
		// a = "5"
		//////////////////////////////////////////////////////////////////
		if tocken_bucket(int(TokenTime), int(TokenNumber), TokenList) {
			switch a {
			case "x":
				return
			case "1":
				features.PUT(wal, mem, cache, &generation, *bloom, int(SSTableType), int(memTableFlush), int(summaryBlockingFactor), int(HLLp), CMSp, CMSd)
			case "2":
				features.GET(mem, cache, *bloom, int(SSTableType), int(summaryBlockingFactor), int(generation), wal, int(memTableFlush))
			case "3":
				features.DELETE(wal, mem, cache)
				fmt.Println("Uspešno obrisan")
			case "4":
				features.LIST(mem, int(SSTableType), int(summaryBlockingFactor))
			case "5":
				features.RANGE_SCAN(mem, int(SSTableType), int(summaryBlockingFactor))
			case "6":
				generation = features.LSM(int(SSTableType), int(summaryBlockingFactor))
			default:
				fmt.Println("Pogrešan unos")
			}
		} else {
			fmt.Println("Previše operacija u kratkom vremenskom roku")
		}

		a = ""
	}
}
