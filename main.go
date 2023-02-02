package main

import (
	"Projekat/features"
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
	WALLLowWaterMark := 1.0
	WALSegmentationFactor := 5.0
	skipListMaxHeight := 5.0
	memTableMaxCap := 5.0
	memTableFlush := 80.0
	//memTableType := 1 // skipList -> 1, BStablo -> 2
	cacheSize := 14.0
	SSTableType := 2.0 // single -> 1, multiple -> 2
	summaryBlockingFactor := 20.0
	LSMTreeLevel := 4.0
	LSMAlgorithm := 1.0 //size -> 1, lleveled -> 2
	TokenTime := 10.0
	TokenNumber := 5.0
	ResultsNumber := 5.0
	HLLp := 8.0  // broj vodecih bajtova
	CMSp := 0.1  // preciznost
	CMSd := 0.01 // tacnost
	BFn := 20.0  // broj elemenata
	BFp := 0.1   //preciznost
	configFile, err := ioutil.ReadFile("config/config.json")
	if err == nil {

		var payload map[string]map[string]float64
		err = json.Unmarshal(configFile, &payload)
		if err != nil {
			log.Fatal(err)
		}

		WALLLowWaterMark = payload["WAL"]["WALLowWaterMark"]
		WALSegmentationFactor = payload["WAL"]["WALSegmentationFactor"]
		skipListMaxHeight = payload["SkipList"]["skipListMaxHeight"]
		memTableMaxCap = payload["MemTable"]["memTableMaxCap"]
		memTableFlush = payload["MemTable"]["memTableFlush"]
		//memTableType = payload["MemTable"]["memTableType"]
		cacheSize = payload["LRUCache"]["cacheSize"]
		SSTableType = payload["SSTable"]["SSTableType"]
		summaryBlockingFactor = payload["SSTable"]["summaryBlockingFactor"]
		LSMTreeLevel = payload["LSMTree"]["LSMTreeLevel"]
		LSMAlgorithm = payload["LSMTree"]["LSMAlgorithm"]
		TokenTime = payload["TokenBucket"]["TokenTime"]
		TokenNumber = payload["TokenBucket"]["TokenNumber"]
		ResultsNumber = payload["Pagination"]["ResultsNumber"]
		HLLp = payload["HLL"]["p"]
		CMSp = float64(payload["CMS"]["p"])
		CMSd = float64(payload["CMS"]["d"])
		BFn = payload["BloomFilter"]["n"]
		BFp = float64(payload["BloomFilter"]["p"])
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
	wal := structures.CreateWAL(uint(WALSegmentationFactor), int(WALLLowWaterMark))
	mem := structures.CreateMemtable(int(skipListMaxHeight), uint(memTableMaxCap), 0)
	wal.ReadAll(*mem, generation, int(SSTableType))
	bloom := structures.ReadAll()
	cache := structures.CreateLRU(int(cacheSize))
	//ovo sve treba iz config fajla da se cita

	// fmt.Println("-----------------------------------------------")
	// fmt.Println("|                   OPCIJE                    |")
	// fmt.Println("|                                             |")
	// fmt.Println("| 1. Dodavanje (PUT)                          |")
	// fmt.Println("| 2. Dobavljenje (GET)                        |")
	// fmt.Println("| 3. Brisanje (DELETE)                        |")
	// fmt.Println("| 4. Pretraga (LIST)                          |")
	// fmt.Println("| 5. Traženje (RANGE SCAN)                    |")
	// fmt.Println("| 6. Kompakcija (LSM)                         |")
	// fmt.Println("|                                             |")
	// fmt.Println("|                       Za izlaz ukucajte 'x' |")
	// fmt.Println("-----------------------------------------------")

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
		fmt.Scanln(&a)
		// a = "5"

		if tocken_bucket(int(TokenTime), int(TokenNumber), TokenList) {
			switch a {
			case "x":
				return
			case "1":
				features.PUT(wal, mem, cache, &generation, *bloom, int(SSTableType), int(memTableFlush), int(summaryBlockingFactor), int(HLLp), CMSp, CMSd, int(BFn), BFp)
			case "2":
				features.GET(mem, cache, *bloom, int(SSTableType), int(LSMTreeLevel), int(summaryBlockingFactor))
			case "3":
				features.DELETE(wal, mem, cache)
				fmt.Println("Uspešno obrisan")
			case "4":
				features.LIST(mem, int(LSMTreeLevel), int(SSTableType), int(summaryBlockingFactor), int(ResultsNumber))
			case "5":
				features.RANGE_SCAN(mem, int(LSMTreeLevel), int(SSTableType), int(summaryBlockingFactor), int(ResultsNumber))
			case "6":
				if generation > 1 {
					generation = features.LSM(int(SSTableType), int(LSMAlgorithm), int(LSMTreeLevel), int(summaryBlockingFactor))
				}

			case "test":
				fmt.Println(LSMAlgorithm)
				fmt.Println(LSMTreeLevel)
				fmt.Println(SSTableType)
			default:
				fmt.Println("Pogrešan unos")
			}
		} else {
			fmt.Println("Previše operacija u kratkom vremenskom roku")
		}

		a = ""
	}
}
