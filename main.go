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
	WALLLowWaterMark := 1
	WALSegmentationFactor := 5
	skipListMaxHeight := 5
	memTableMaxCap := 5
	memTableFlush := 80
	//memTableType := 1 // skipList -> 1, BStablo -> 2
	cacheSize := 14
	SSTableType := 2 // single -> 1, multiple -> 2
	summaryBlockingFactor := 20
	LSMTreeLevel := 4
	LSMAlgorithm := 1 //size -> 1, lleveled -> 2
	TokenTime := 10
	TokenNumber := 5
	ResultsNumber := 5
	configFile, err := ioutil.ReadFile("config/config.json")
	if err == nil {

		var payload map[string]map[string]int
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
	wal := structures.CreateWAL(uint(WALSegmentationFactor), WALLLowWaterMark)
	mem := structures.CreateMemtable(skipListMaxHeight, uint(memTableMaxCap), 0)
	wal.ReadAll(*mem, generation, SSTableType)
	bloom := structures.ReadAll()
	cache := structures.CreateLRU(cacheSize)
	//ovo sve treba iz config fajla da se cita

	fmt.Println("-----------------------------------------------")
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

	var a string = ""
	TokenList := list.New()

	for {
		fmt.Print("\nIzaberite opciju -> ")
		fmt.Scanln(&a)

		if tocken_bucket(TokenTime, TokenNumber, TokenList) {
			switch a {
			case "x":
				return
			case "1":
				features.PUT(wal, mem, cache, &generation, *bloom, SSTableType, memTableFlush, summaryBlockingFactor)
			case "2":
				value := features.GET(mem, cache, *bloom, SSTableType, LSMTreeLevel, summaryBlockingFactor)
				if value != nil {
					fmt.Println("Pronađen je i vrednost je ", string(value))
				} else {
					fmt.Println("Element sa traženim ključem nije pronađen.")
				}
			case "3":
				features.DELETE(wal, mem, cache)
				fmt.Println("Uspešno obrisan")
			case "4":
				features.LIST(mem, LSMTreeLevel, SSTableType, summaryBlockingFactor, ResultsNumber)
			case "5":
				features.RANGE_SCAN(mem, LSMTreeLevel, SSTableType, summaryBlockingFactor, ResultsNumber)
			case "6":
				if generation > 1 {
					generation = features.LSM(SSTableType, LSMAlgorithm, LSMTreeLevel, summaryBlockingFactor)
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
