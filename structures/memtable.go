package structures

import (
	"Projekat/global"
	"fmt"
	"os"
)

type Memtable struct {
	Data         MemtableData
	capacity     uint
	max_capacity uint
}

func CreateMemtable(height int, max_cap uint, stat int) *Memtable {
	if global.MemTableDataType == 1 {
		data := CreateSkipList(height-1, 1, stat)
		memtable := Memtable{data, 0, max_cap}
		return &memtable
	} else {
		data := CreateBTree("", nil, 0, 0)
		memtable := Memtable{data, 0, max_cap}
		return &memtable
	}
}

func (memtable *Memtable) Add(key string, value []byte, stat int, timestamp uint64) {
	new := memtable.Data.Add(key, value, stat, timestamp)
	if new {
		memtable.capacity++
	}
}

// func (memtable *Memtable) Update(key string, value []byte, stat int) bool {
// 	element := memtable.data.Update(key, value, stat)
// 	return element
// }

// func (memtable *Memtable) Remove(key string) bool {
// 	element := memtable.data.Delete(key)
// 	return element
// }

func (Memtable *Memtable) FindAllPrefix(prefix string, j *int) string {
	return Memtable.Data.FindAllPrefix(prefix, j)
}

func (Memtable *Memtable) FindAllPrefixRange(min_prefix string, max_prefix string, j *int) string {
	return Memtable.Data.FindAllPrefixRange(min_prefix, max_prefix, j)
}

func (Memtable *Memtable) Find(key string) (found bool, value []byte, all_key string) {
	found, skiplist, val, new_key := Memtable.Data.Found(key)
	if found {
		if global.MemTableDataType == 1 {
			if skiplist.Status == 1 {
				return true, nil, ""
			} else {
				return true, skiplist.Value, skiplist.Key
			}
		} else {
			return true, val, new_key
		}
	}
	return false, nil, ""
}

func (memtable *Memtable) Flush(generation *int, sstableType int, percentage int, summaryBlockingFactor int) int {
	if float64(memtable.capacity)/float64(memtable.max_capacity)*100 >= float64(percentage) { //ovde treba videti odakle se uzima granica popunjenosti
		data := memtable.Data.GetData()

		//dodati da broji za generaciju
		generation := 0
		for j := 0; true; j++ {
			var file *os.File
			var err error
			if sstableType == 2 {
				file, err = os.OpenFile("data/sstables/usertable-0-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)
			} else {
				file, err = os.OpenFile("data/singlesstables/usertable-0-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)
			}

			if os.IsNotExist(err) {
				break
			}
			generation += 1
			file.Close()
		}

		if sstableType == 2 {
			if global.LSMAlgorithm == 1 {
				CreateSSTable(data, generation, summaryBlockingFactor)
			} else {
				//features.LSM(sstableType, summaryBlockingFactor)

			}

		} else {
			if global.LSMAlgorithm == 1 {
				CreateSingleSSTable(data, generation, summaryBlockingFactor)
			} else {
				//features.LSM(sstableType, summaryBlockingFactor)
			}

		}
		generation++
		memtable.capacity = 0
		if global.MemTableDataType == 1 {
			memtable.Data = CreateSkipList(global.SkipListMaxHeight, 1, 0) //obrisali -1 za maxh
		} else {
			memtable.Data = CreateBTree("", nil, 0, 0)
		}
		return 1
	}
	return 0
}
