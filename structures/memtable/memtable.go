package structures

type Memtable struct {
	data         SkipList
	capacity     uint
	max_capacity uint
}

func CreateMemtable(height int, max_cap uint, stat uint) *Memtable {
	skip_list := Create(height)
	memtable := Memtable{*skip_list, 0, stat}
	return &memtable
}

func (memtable *Memtable) Add(key string, value []byte, stat uint) {
	memtable.data.Add(key, value, stat)
	memtable.capacity++
}

func (memtable *Memtable) Update(key string, value []byte, stat uint) bool {
	element := memtable.data.Update(key, value, stat)
	return element
}

func (memtable *Memtable) Remove(key string) bool {
	element := memtable.data.Delete(key)
	return element
}

func (Memtable *Memtable) Find(key string) (found bool, value []byte) {
	found, element := Memtable.data.Found(key)
	if found {
		if element.status == 0 {
			return false, nil
		} else {
			return true, element
		}
	}
	return false, nil
}

func (memtable *Memtable) Flush() {
	//kada popunjenost bude veca od 80% onda treba da se Flush
	if float64(memtable.capacity)/float64(memtable.max_capacity)*100 >= 80 {
		//kod da se prepise u sstable
	}
}
