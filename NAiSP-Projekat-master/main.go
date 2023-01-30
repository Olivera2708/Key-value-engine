package main

import (
	"Projekat/features"
	"Projekat/structures"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"os"
)

func main() {

	generation := 0
	WALLLowWaterMark := 1
	WALSegmentationFactor := 5
	skipListMaxHeight := 5
	memTableMaxCap := 5
	//memTableFlush := 80
	//memTableType := 1 // skipList -> 1, BStablo -> 2
	cacheSize := 14
	SSTableType := 2 // single -> 1, multiple -> 2
	//summaryBlockingFactor := 20
	//LSMTreeLevel := 4
	//LSMAlgorithm := 1
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
		//memTableFlush = payload["MemTable"]["memTableFlush"]
		//memTableType = payload["MemTable"]["memTableType"]
		cacheSize = payload["LRUCache"]["cacheSize"]
		SSTableType = payload["SSTable"]["SSTableType"]
		//summaryBlockingFactor = payload["SSTable"]["summaryBlockingFactor"]
		//LSMTreeLevel = payload["LSMTree"]["LSMTreeLevel"]
		//LSMAlgorithm = payload["LSMTree"]["LSMAlgoritam"]

	}

	for i := 0; true; i++ {
		if SSTableType == 2 {
			_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
			if os.IsNotExist(err) {
				generation = i
				break
			}
		} else {
			_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
			if os.IsNotExist(err) {
				generation = i
				break
			}
		}

	}

	//fmt.Println(payload["SSTable"]["SSTableType"])

	//inicijalizacija

	wal := structures.CreateWAL(uint(WALSegmentationFactor), WALLLowWaterMark)
	mem := structures.CreateMemtable(skipListMaxHeight, uint(memTableMaxCap), 0)
	wal.ReadAll(*mem)
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
	fmt.Println("|                                             |")
	fmt.Println("|                       Za izlaz ukucajte 'x' |")
	fmt.Println("-----------------------------------------------")

	var a string = ""

	for true {
		fmt.Print("\nIzaberite opciju -> ")
		fmt.Scanln(&a)
		// a = "3"

		switch a {
		case "x":
			return
		case "1":
			features.PUT(wal, mem, cache, &generation, *bloom, SSTableType)
		case "2":
			value := features.GET(mem, cache, *bloom, SSTableType)
			if value != nil {
				fmt.Println("Pronađen je i vrednost je ", string(value))
			} else {
				fmt.Println("Element sa traženim ključem nije pronađen.")
			}
		case "3":
			if features.DELETE(wal, mem, cache) {
				fmt.Println("Uspešno obrisan")
			} else {
				fmt.Println("Ne postoji element sa zadatim ključem")
			}
		default:
			fmt.Println("Pogrešan unos")
		}

		a = ""
	}
}
