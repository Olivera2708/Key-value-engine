package features

import (
	"Projekat/structures"
	"fmt"
	"os"
	"strconv"
)

func LIST(mem *structures.Memtable, level int, sstableType int, summaryBlockingFactor int, ResultsNumber int) {
	prefix := ""

	for true {
		fmt.Print("Unesite prefiks -> ")
		// prefix = "a"
		fmt.Scan(&prefix)
		if len(prefix) != 0 {
			break
		}
	}

	//u memtable
	// all_data := [][]byte{}
	all_keys, all_data := mem.FindAllPrefix(prefix)
	// all_keys := []string{}

	//sstable
	for lvl := 0; lvl < level; lvl++ {
		for i := 0; true; i++ {
			if sstableType == 2 {
				_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-summary.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				keys, values := structures.FindAllPrefixMultiple("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), prefix)
				all_data = append(all_data, values...)
				all_keys = append(all_keys, keys...)
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				all_keys = append(all_keys, structures.FindAllPrefixSingle("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", prefix, summaryBlockingFactor)...)
			}
		}
	}

	//ispis
	if len(all_data) == 0 {
		fmt.Println("Nema rezultata")
	} else {
		writerPrefix(all_keys, all_data, ResultsNumber)
	}
}

func writerPrefix(all_keys []string, all_data [][]byte, ResultsNumber int) {
	pageNumber := ""
	currentPage := -1
	for true {
		fmt.Print("Unesite broj strane, 'p' za prethodnu stranu, 's' za sledeću ili 'x' za izlazak -> ")
		fmt.Scan(&pageNumber)
		// pageNumber = "1"
		num, err := strconv.Atoi(pageNumber)
		if err != nil {
			if pageNumber == "x" {
				break
			}
			if currentPage != -1 {
				if pageNumber == "p" && currentPage > 1 {
					currentPage--
					writeAllPrefixData(all_keys, all_data, ResultsNumber, currentPage)
				} else if pageNumber == "s" && currentPage*ResultsNumber < len(all_data) {
					currentPage++
					writeAllPrefixData(all_keys, all_data, ResultsNumber, currentPage)
				} else {
					fmt.Println("Trazena strana ne postoji")
				}
			} else {
				fmt.Println("Potrebno je uneti broj")
			}
		} else if num < 1 || (num-1)*ResultsNumber >= len(all_data) {
			fmt.Println("Neispravan broj strana")
		} else {
			currentPage = num
			writeAllPrefixData(all_keys, all_data, ResultsNumber, currentPage)
		}
	}
}

func writeAllPrefixData(all_keys []string, all_data [][]byte, ResultsNumber int, pageNumber int) {
	start := (pageNumber - 1) * ResultsNumber

	for i := start; i < start+ResultsNumber; i++ {
		if i >= len(all_data) {
			break
		}
		fmt.Println(fmt.Sprint(i+1) + ". \t" + "ključ: " + all_keys[i] + "\n\tvrednost: " + string(all_data[i])) //ispisuje kao string
	}
}
