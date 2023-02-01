package features

import (
	"Projekat/structures"
	"fmt"
	"os"
)

func RANGE_SCAN(mem *structures.Memtable, level int, sstableType int, summaryBlockingFactor int, ResultsNumber int) {
	min_prefix := ""
	max_prefix := ""

	for true {
		fmt.Print("Unesite minimalni prefiks -> ")
		fmt.Scan(&min_prefix)
		if len(min_prefix) != 0 {
			break
		}
	}

	for true {
		fmt.Print("Unesite maksimalni prefiks -> ")
		fmt.Scan(&max_prefix)
		if min_prefix <= max_prefix && len(max_prefix) > 0 {
			break
		}
	}

	//u memtable
	all_data := mem.FindAllPrefixRange(min_prefix, max_prefix)

	//sstable
	for lvl := 0; lvl < level; lvl++ {
		for i := 0; true; i++ {
			if sstableType == 2 {
				_, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-summary.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				all_data = append(all_data, structures.FindAllPrefixRangeMultiple("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i), min_prefix, max_prefix)...)
			} else {
				_, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
				if os.IsNotExist(err) {
					break
				}
				all_data = append(all_data, structures.FindAllPrefixRangeSingle("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", min_prefix, max_prefix, summaryBlockingFactor)...)
			}
		}
	}

	writerPrefix(all_data, ResultsNumber)
}
