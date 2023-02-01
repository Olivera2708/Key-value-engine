package structures

type Memtable struct {
	data         SkipList
	capacity     uint
	max_capacity uint
}

func CreateMemtable(height int, max_cap uint, stat int) *Memtable {
	skip_list := CreateSkipList(height-1, 1, stat)
	memtable := Memtable{*skip_list, 0, max_cap}
	return &memtable
}

func (memtable *Memtable) Add(key string, value []byte, stat int, timestamp uint64) {
	new := memtable.data.Add(key, value, stat, timestamp)
	if new {
		memtable.capacity++
	}
}

func (memtable *Memtable) Update(key string, value []byte, stat int) bool {
	element := memtable.data.Update(key, value, stat)
	return element
}

// func (memtable *Memtable) Remove(key string) bool {
// 	element := memtable.data.Delete(key)
// 	return element
// }

func (Memtable *Memtable) FindAllPrefix(prefix string) []string {
	return Memtable.data.FindAllPrefix(prefix)
}

func (Memtable *Memtable) FindAllPrefixRange(min_prefix string, max_prefix string) []string {
	return Memtable.data.FindAllPrefixRange(min_prefix, max_prefix)
}

func (Memtable *Memtable) Find(key string) (found bool, value []byte) {
	found, element := Memtable.data.Found(key)
	if found {
		if element.status == 1 {
			return false, nil
		} else {
			return true, element.value
		}
	}
	return false, nil
}

func (memtable *Memtable) Flush(generation *int, sstableType int, percentage int, summaryBlockingFactor int) {
	if float64(memtable.capacity)/float64(memtable.max_capacity)*100 >= float64(percentage) { //ovde treba videti odakle se uzima granica popunjenosti
		data := memtable.data.GetData()
		if sstableType == 2 {
			CreateSSTable(data, *generation, summaryBlockingFactor)
		} else {
			CreateSingleSSTable(data, *generation, summaryBlockingFactor)
		}
		*generation++
		memtable.capacity = 0
		memtable.data = *CreateSkipList(memtable.data.maxHeight, 1, 0) //obrisali -1 za maxh
	}
}
