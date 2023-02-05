package structures

import (
	"Projekat/global"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"strings"
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
				LeveledMultiMem(memtable, summaryBlockingFactor)
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

func LeveledMultiMem(memtable *Memtable, summaryBlockingFactor int) {
	memData := memtable.Data.GetData()
	startIndex := memData[0][0]
	endIndex := memData[len(memData)-1][0]

	mapa := make(map[int]bool)

	files := make([]os.File, 0)
	i := 0
	for j := 0; true; j++ {
		file, err := os.Open("data/sstables/usertable-0-" + fmt.Sprint(j) + "-summary.db")
		if err != nil {
			break
		}

		len1Bytes := make([]byte, 8, 8)
		len2Bytes := make([]byte, 8, 8)
		file.Read(len1Bytes)
		len1 := binary.LittleEndian.Uint64(len1Bytes)
		key1 := make([]byte, int(len1), int(len1))
		file.Read(key1)

		file.Read(len2Bytes)
		len2 := binary.LittleEndian.Uint64(len2Bytes)
		key2 := make([]byte, int(len2), int(len2))
		file.Read(key2)
		file.Close()

		if !((strings.Split(string(startIndex), "-")[0] > strings.Split(string(key2), "-")[0]) || (strings.Split(string(endIndex), "-")[0] < strings.Split(string(key1), "-")[0])) {
			file, _ = os.Open("data/sstables/usertable-0-" + fmt.Sprint(j) + "-data.db")
			files = append(files, *file)
			mapa[i] = true
			i += 1
		}

	}
	newGen := 0
	for i := 0; true; i++ {
		file, err := os.OpenFile("data/sstables/usertable-0-"+fmt.Sprint(i)+"-data.db", os.O_WRONLY, 0666)
		if os.IsNotExist(err) {
			newGen = i
			break
		}
		file.Close()
	}

	if len(files) > 0 {

		keys := make([]string, 0)
		values := make([][]byte, 0)
		positions := make([]int, 0)
		currentPos := 0
		isMem := false
		memCounter := 0

		recMin := make(map[string][]byte)
		newRec := make(map[string][]byte)
		empty := true
		newMin := 0

		newFile, _ := os.Create("data/sstables/usertable-0-" + fmt.Sprint(newGen) + "-data.db")

		for true {
			isMem = true
			newMin = -1
			minimums := make([]int, 0)

			if memCounter < len(memData) {

				recMin = make(map[string][]byte)
				recMin["key"] = memData[memCounter][0]
				recMin["value"] = memData[memCounter][1]
				recMin["tombstone"] = memData[memCounter][2]
				recMin["timestamp"] = memData[memCounter][3]
				key_size := len(memData[memCounter][0])
				keySizeBytes := make([]byte, 8, 8)
				binary.LittleEndian.PutUint64(keySizeBytes, uint64(key_size))
				recMin["key_size"] = keySizeBytes

				val_size := len(memData[memCounter][1])
				valSizeBytes := make([]byte, 8, 8)
				binary.LittleEndian.PutUint64(valSizeBytes, uint64(val_size))
				recMin["val_size"] = valSizeBytes

				record := append(recMin["timestamp"], recMin["tombstone"]...)
				record = append(record, keySizeBytes...)
				record = append(record, valSizeBytes...)
				record = append(record, recMin["key"]...)
				record = append(record, recMin["value"]...)

				crc := crc32.ChecksumIEEE(record)
				crcBytes := make([]byte, 4, 4)
				binary.LittleEndian.PutUint32(crcBytes, uint32(crc))
				recMin["crc"] = crcBytes

			} else {
				for i := 0; i < len(files); i++ {
					if mapa[i] {
						recMin, empty = ReadNextRecord(&files[i])
						if !empty {
							isMem = false
							newMin = i
							minimums = append(minimums, newMin)
							minimums = append(minimums, len(recMin["key"]))
							minimums = append(minimums, len(recMin["value"]))
							break
						} else {
							files[i].Close()
							mapa[i] = false
						}
					}
				}

			}

			if newMin == -1 && memCounter == len(memData) {
				break
			}

			// ako je newMin = len - 1 ? Trebalo bi da samo ne uđe u uslovu i < len(files)

			for i := newMin + 1; i < len(files); i++ {
				if mapa[i] {
					newRec, empty = ReadNextRecord(&files[i])

					if empty {
						files[i].Close()
						mapa[i] = false
						continue
					}

					if strings.Split(string(newRec["key"]), "-")[0] < strings.Split(string(recMin["key"]), "-")[0] {
						m := 0

						isMem = false

						for m < len(minimums) {
							files[minimums[m]].Seek(-29-int64(minimums[m+1])-int64(minimums[m+2]), 1)
							m += 3
						}

						newMin = i

						minimums := make([]int, 0)
						minimums = append(minimums, newMin)
						minimums = append(minimums, len(newRec["key"]))
						minimums = append(minimums, len(newRec["value"]))

						recMin = newRec

					} else if strings.Split(string(newRec["key"]), "-")[0] > strings.Split(string(recMin["key"]), "-")[0] {

						files[i].Seek(-29-int64(len(newRec["key"]))-int64(len(newRec["value"])), 1)

					} else {

						minimums = append(minimums, i)
						minimums = append(minimums, len(newRec["key"]))
						minimums = append(minimums, len(newRec["value"]))
						tNew := binary.LittleEndian.Uint64(newRec["timestamp"])
						tMin := binary.LittleEndian.Uint64(recMin["timestamp"])
						if tNew > tMin {
							recMin = newRec
						}
					}
				}
			}

			if isMem {
				memCounter++
			}

			if recMin["tombstone"][0] == 0 {

				rec := append(recMin["crc"], recMin["timestamp"]...)
				rec = append(rec, recMin["tombstone"]...)
				rec = append(rec, recMin["key_size"]...)
				rec = append(rec, recMin["val_size"]...)
				rec = append(rec, recMin["key"]...)
				rec = append(rec, recMin["value"]...)
				newFile.Write(rec)
				keys = append(keys, string(recMin["key"]))
				values = append(values, recMin["value"])
				positions = append(positions, currentPos)
				currentPos += 29 + len(recMin["key"]) + len(recMin["value"])

			}

		}

		for f := 0; f < len(files); f++ {
			path := files[f].Name()
			path = path[:len(path)-8]
			os.Remove(path + "-data.db")
			os.Remove(path + "-index.db")
			os.Remove(path + "-summary.db")
			os.Remove(path + "-filter.db")
			os.Remove(path + "-TOC.txt")
			os.Remove(path + "-Metadata.txt")

		}

		newFile.Close()

		CreateIndex(keys, positions, "data/sstables/usertable-0-"+fmt.Sprint(newGen), summaryBlockingFactor)

		bf := CreateBloomFilter(uint(len(keys)), 2)
		for i := 0; i < len(keys); i++ {
			bf.Add(keys[i])
		}
		bf.Write("data/sstables/usertable-0-" + fmt.Sprint(newGen))
		CreateTOC("data/sstables/usertable-0-" + fmt.Sprint(newGen))

		merkle := CreateMerkleTree(values)
		WriteMerkleInFile(merkle, "data/sstables/usertable-0-"+fmt.Sprint(newGen))

		j := 0
		for i := 0; i <= newGen; i++ {
			file, err := os.OpenFile("data/sstables/usertable-0-"+fmt.Sprint(i)+"-data.db", os.O_WRONLY, 0666)
			if os.IsNotExist(err) {
				continue
			}
			file.Close()
			if j != i {
				os.Rename("data/sstables/usertable-0-"+fmt.Sprint(i)+"-data.db",
					"data/sstables/usertable-0-"+fmt.Sprint(j)+"-data.db")
				os.Rename("data/sstables/usertable-0-"+fmt.Sprint(i)+"-index.db",
					"data/sstables/usertable-0-"+fmt.Sprint(j)+"-index.db")
				os.Rename("data/sstables/usertable-0-"+fmt.Sprint(i)+"-summary.db",
					"data/sstables/usertable-0-"+fmt.Sprint(j)+"-summary.db")
				os.Rename("data/sstables/usertable-0-"+fmt.Sprint(i)+"-filter.db",
					"data/sstables/usertable-0-"+fmt.Sprint(j)+"-filter.db")
				os.Rename("data/sstables/usertable-0-"+fmt.Sprint(i)+"-TOC.txt",
					"data/sstables/usertable-0-"+fmt.Sprint(j)+"-TOC.txt")
				os.Rename("data/sstables/usertable-0-"+fmt.Sprint(i)+"-Metadata.txt",
					"data/sstables/usertable-0-"+fmt.Sprint(j)+"-Metadata.txt")
			}

			j++
		}
	} else {
		CreateSSTable(memData, newGen, summaryBlockingFactor)
	}

}
