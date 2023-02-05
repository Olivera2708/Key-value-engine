package features

import (
	"Projekat/global"
	"Projekat/structures"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

func LIST(mem *structures.Memtable, sstableType int, summaryBlockingFactor int) {
	prefix := ""

	for {
		fmt.Print("Unesite prefiks -> ")
		// prefix = "2"
		fmt.Scan(&prefix)
		if len(prefix) != 0 {
			break
		}
	}
	n := global.ResultsNumber
	all_data := [][]byte{}
	all_keys := []string{}
	var paths []string
	var positions1 []uint64

	//u memtable
	btree_ind := -1
	mem_key := mem.FindAllPrefix(prefix, &btree_ind)
	// _, node, _, _ := mem.Data.Found(mem_key)

	//sstable
	for lvl := 0; lvl < global.LSMTreeLevel; lvl++ {
		for i := 0; true; i++ {
			if sstableType == 2 {
				_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-summary.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				path, position := structures.FindAllPrefixMultiple("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), prefix)
				if path != "" {
					paths = append(paths, path)
					positions1 = append(positions1, position)
				}
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				path, position := structures.FindAllPrefixSingle("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), prefix, summaryBlockingFactor)
				if path != "" {
					paths = append(paths, path)
					positions1 = append(positions1, position)
				}
			}
		}
	}
	currentPage := -1

	for {
		all_data = [][]byte{}
		all_keys = []string{}
		_, node, _, _ := mem.Data.Found(mem_key)

		positions := make([]uint64, len(positions1))
		copy(positions, positions1)
		pageNumber := ""

		fmt.Print("Unesite broj strane, 'p' za prethodnu stranu, 's' za sledeću ili 'x' za izlazak -> ")
		fmt.Scan(&pageNumber)
		// pageNumber = "1"

		num, err := strconv.Atoi(pageNumber)
		if err != nil {
			if pageNumber == "x" {
				break
			}
			if pageNumber == "p" && currentPage > 1 {
				currentPage--
			} else if pageNumber == "s" && currentPage > 0 { // && currentPage*global.ResultsNumber < len(all_data)
				currentPage++
			} else {
				fmt.Println("Trazena strana ne postoji")
				continue
			}

		} else if num < 1 { // || (num-1)*global.ResultsNumber >= len(all_data)
			fmt.Println("Neispravan broj strana")
			continue
		} else {
			currentPage = num
		}

		var best string
		var best_val []byte
		var bestStat int
		//ispis
		files := make([]os.File, 0)
		for j := 0; j < len(paths); j++ {
			file, err := os.Open(paths[j] + "-data.db")
			if err != nil {
				log.Fatal(err)
			}
			file.Seek(0, 0)
			files = append(files, *file)
		}

		var isMem bool
		var indices []int
		var offsets []int64
		var bestTime uint64
		var tomb int

		for i := 0; true; i++ {
			if global.MemTableDataType == 1 {
				if node != nil && strings.HasPrefix(node.Key, prefix) {
					best = node.Key
					best_val = node.Value
					bestTime = node.Timestamp
					bestStat = node.Status
					isMem = true //sta ako u mem nema prefiksa???
				}
			} else {
				best = mem.Data.FindAllPrefix(prefix, &btree_ind)
				if best != "" {
					best_val, bestTime, bestStat = mem.Data.FindTreeNode(best)
					isMem = true
				} else {
					isMem = false
				}
			}
			counter := 0
			for j := 0; j < len(files); j++ {
				var key string
				var val []byte
				var timeS []byte
				if sstableType == 2 {
					key, val, timeS, tomb = structures.FindPrefixSSTableMultiple(prefix, positions[j], &files[j])
				} else {
					key, val, timeS, tomb = structures.FindPrefixSSTableSingle(prefix, positions[j], &files[j])
				}
				offset, err := files[j].Seek(0, io.SeekCurrent)
				if err != nil {
					log.Fatal(err)
				}
				if strings.Split(key, "-")[0] == "" {
					counter++
					continue
				}
				timestamp := binary.LittleEndian.Uint64(timeS)
				if strings.Split(key, "-")[0] < strings.Split(best, "-")[0] || strings.Split(best, "-")[0] == "" {
					best = key
					best_val = val
					bestStat = tomb
					indices = make([]int, 1)
					offsets = make([]int64, 1)
					indices[0] = j
					offsets[0] = offset
					isMem = false
				} else if strings.Split(key, "-")[0] == strings.Split(best, "-")[0] {
					indices = append(indices, j)
					offsets = append(offsets, offset)
					if bestTime < timestamp {
						best = key
						best_val = val
						bestTime = timestamp
						bestStat = tomb
					}
				}
			}
			if (counter == len(files) && !isMem) || len(all_data) == n {
				break
			}
			if global.MemTableDataType == 1 {
				if isMem && node != nil {
					node = node.Next[0]
					if node == nil {
						isMem = false
					}
				}
			} else {
				if isMem {
					btree_ind++
				}
			}
			for k := 0; k < len(indices); k++ {
				positions[indices[k]] = uint64(offsets[k])
			}
			if best == "" || bestStat == 1 {
				i--
				best = ""
				continue
			}

			if i >= n*(currentPage-1) && i < n*(currentPage) && bestStat == 0 {
				all_keys = append(all_keys, best)
				all_data = append(all_data, best_val)
			}
			best = ""
		}
		for j := 0; j < len(files); j++ {
			files[j].Close()
		}
		if len(all_data) == 0 {
			fmt.Println("Nema rezultata")
			if pageNumber == "p" {
				currentPage = 1
			} else if pageNumber == "s" {
				currentPage--
			} else {
				currentPage = -1
			}
		} else {
			writeAllPrefixData(all_keys, all_data, currentPage)
		}
	}
}

func writeAllPrefixData(all_keys []string, all_data [][]byte, pageNumber int) {

	for i := 0; i < len(all_data); i++ {
		if i >= len(all_data) {
			break
		}
		write_value := string(all_data[i])
		write_key := all_keys[i]
		if strings.Contains(all_keys[i], "-bloom") {
			write_key = strings.ReplaceAll(write_key, "-bloom", "")
			write_value = "Bloom filter"
		} else if strings.Contains(all_keys[i], "-cms") {
			write_key = strings.ReplaceAll(write_key, "-cms", "")
			write_value = "Count min sketch"
		} else if strings.Contains(all_keys[i], "-hll") {
			write_key = strings.ReplaceAll(write_key, "-hll", "")
			write_value = "Hyper Log Log"
		} else if strings.Contains(all_keys[i], "-simHash") {
			write_key = strings.ReplaceAll(write_key, "-simHash", "")
			write_value = "Sim Hash"
		}
		fmt.Println(fmt.Sprint(i+1+global.ResultsNumber*(pageNumber-1)) + ". \t" + "ključ: " + write_key + "\n\tvrednost: " + write_value)
	}
}
