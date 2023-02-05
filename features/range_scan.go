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

func RANGE_SCAN(mem *structures.Memtable, sstableType int, summaryBlockingFactor int) {
	min_prefix := ""
	max_prefix := ""

	for {
		fmt.Print("Unesite minimalni prefiks -> ")
		fmt.Scan(&min_prefix)
		// min_prefix = "1"
		if len(min_prefix) != 0 {
			break
		}
	}

	for {
		fmt.Print("Unesite maksimalni prefiks -> ")
		fmt.Scan(&max_prefix)
		// max_prefix = "9"
		if min_prefix <= max_prefix && len(max_prefix) > 0 {
			break
		}
	}

	max_prefix = strings.Split(max_prefix, "-")[0]
	min_prefix = strings.Split(min_prefix, "-")[0]

	n := global.ResultsNumber
	all_data := [][]byte{}
	all_keys := []string{}
	var paths []string
	var positions1 []uint64
	//u memtable
	btree_ind := -1
	mem_key := mem.FindAllPrefixRange(min_prefix, max_prefix, &btree_ind)
	btree_ind--
	//sstable
	for lvl := 0; lvl < global.LSMTreeLevel; lvl++ {
		for i := 0; true; i++ {
			if sstableType == 2 {
				_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-summary.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				path, position := structures.FindAllPrefixRangeMultiple("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), min_prefix, max_prefix)
				if path != "" {
					paths = append(paths, path)
					positions1 = append(positions1, position)
				}
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				path, position := structures.FindAllPrefixRangeSingle("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), min_prefix, max_prefix, summaryBlockingFactor)
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

		num, err := strconv.Atoi(pageNumber)
		if err != nil {
			if pageNumber == "x" {
				break
			}
			if pageNumber == "p" && currentPage > 1 {
				currentPage--
			} else if pageNumber == "s" && currentPage > 0 {
				currentPage++
			} else {
				fmt.Println("Trazena strana ne postoji")
				continue
			}

		} else if num < 1 {
			fmt.Println("Neispravan broj strana")
			continue
		} else {
			currentPage = num
		}

		var best string
		var best_val []byte
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
		var bestTime uint64
		var bestStat int
		btree_ind = 0

		for i := 0; true; i++ {
			var offsets []int64
			var indices []int
			if global.MemTableDataType == 1 {
				if node != nil && (node.Key >= min_prefix && node.Key <= max_prefix) {
					best = node.Key
					best_val = node.Value
					bestTime = node.Timestamp
					bestStat = node.Status
					isMem = true //sta ako u mem nema prefiksa???
				}
			} else {
				best = mem.Data.FindAllPrefixRange(min_prefix, max_prefix, &btree_ind)
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
				var tomb int

				if sstableType == 2 {
					key, val, timeS, tomb = structures.FindPrefixSSTableRangeMultiple(min_prefix, max_prefix, positions[j], &files[j])
				} else {
					key, val, timeS, tomb = structures.FindPrefixSSTableRangeSingle(min_prefix, max_prefix, positions[j], &files[j])
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
					indices = make([]int, 1)
					offsets = make([]int64, 1)
					bestStat = tomb
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
				if !isMem && btree_ind > -1 {
					btree_ind--
				}
			}
			for k := 0; k < len(indices); k++ {
				positions[indices[k]] = uint64(offsets[k])
			}

			// fmt.Println(best)
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
