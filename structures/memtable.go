package structures

import "Projekat/global"

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

func (Memtable *Memtable) FindAllPrefix(prefix string) string {
	return Memtable.Data.FindAllPrefix(prefix)
}

func (Memtable *Memtable) FindAllPrefixRange(min_prefix string, max_prefix string) ([]string, [][]byte) {
	return Memtable.Data.FindAllPrefixRange(min_prefix, max_prefix)
}

func (Memtable *Memtable) Find(key string) (found bool, value []byte, all_key string) {
	found, skiplist, val, new_key := Memtable.Data.Found(key)
	if found {
		if global.MemTableDataType == 1 {
			if skiplist.Status == 1 {
				return false, nil, ""
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
		if sstableType == 2 {
			CreateSSTable(data, *generation, summaryBlockingFactor)
		} else {
			CreateSingleSSTable(data, *generation, summaryBlockingFactor)
		}
		*generation++
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
