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

func (memtable *Memtable) Add(key string, value []byte, stat int) {
	memtable.data.Add(key, value, stat)
	memtable.capacity++
}

func (memtable *Memtable) Update(key string, value []byte, stat int) bool {
	element := memtable.data.Update(key, value, stat)
	return element
}

// func (memtable *Memtable) Remove(key string) bool {
// 	element := memtable.data.Delete(key)
// 	return element
// }

func (Memtable *Memtable) Find(key string) (found bool, value []byte) {
	found, element := Memtable.data.Found(key)
	if found {
		if element.status == 0 {
			return false, nil
		} else {
			return true, element.value
		}
	}
	return false, nil
}

func (memtable *Memtable) Flush(generation *int) {
	if float64(memtable.capacity)/float64(memtable.max_capacity)*100 >= 80 { //ovde treba videti odakle se uzima granica popunjenosti
		CreateSSTable(memtable, *generation)
		*generation++
		memtable.capacity = 0
		skip_list := CreateSkipList(memtable.data.maxHeight-1, 0, 0)
		memtable.data = *skip_list
	}
}
