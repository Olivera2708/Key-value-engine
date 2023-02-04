package features

import (
	"Projekat/structures"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"os"
	"strings"
)

func LSM(sstype int, algorithm int, level int, summaryBlockingFactor int) int {
	if algorithm == 1 {
		if sstype == 1 {

			//fmt.Println("sts")
			SizeTieredSingle(level, summaryBlockingFactor)
		} else {
			//fmt.Println("stm")

			SizeTieredMulti(level, summaryBlockingFactor)
		}
	} else {
		if sstype == 1 {
			//fmt.Println("ls")
			LeveledSingle(level, summaryBlockingFactor)
		} else {
			//fmt.Println("lm")
			LeveledMulti(level, summaryBlockingFactor)
		}
	}
	return 0
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SizeTieredSingle(level int, summaryBlockingFactor int) {

	files := make([]os.File, 0)
	mapa := make(map[int]bool)
	lengths := make([]uint64, 0)

	for i := 0; true; i++ {

		file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)

		if os.IsNotExist(err) {
			break
		}
		lenBytes := make([]byte, 8, 8)
		file.Read(lenBytes)
		len := binary.LittleEndian.Uint64(lenBytes)
		fmt.Println(len)
		lengths = append(lengths, len)
		file.Seek(32, 0)

		files = append(files, *file)

		mapa[i] = true
	}

	if len(files) < 2 {
		return
	}

	newGen := 0
	for i := 0; true; i++ {
		file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(level+1)+"-"+fmt.Sprint(i)+"-data.db", os.O_WRONLY, 0666)
		if os.IsNotExist(err) {
			newGen = i
			break
		}
		file.Close()
	}

	keys := make([]string, 0)
	positions := make([]int, 0)
	currentPos := 32
	lengthCounter := make([]uint64, len(lengths))

	recMin := make(map[string][]byte)
	newRec := make(map[string][]byte)
	newMin := 0
	newFile, _ := os.Create("data/singlesstables/usertable-" + fmt.Sprint(level+1) + "-" + fmt.Sprint(newGen) + "-data.db")
	initialZeros := make([]byte, 32)
	newFile.Write(initialZeros)
	for true {
		newMin = -1

		minimums := make([]int, 0)
		for i := 0; i < len(files); i++ {
			if mapa[i] {
				if lengthCounter[i] < lengths[i] {
					recMin, _ = structures.ReadNextRecord(&files[i])
					lengthCounter[i]++
					fmt.Print(lengthCounter)
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

		if newMin == -1 {
			break
		}

		// ako je newMin = len - 1 ? Trebalo bi da samo ne uđe u uslovu i < len(files)

		for i := newMin + 1; i < len(files); i++ {
			if mapa[i] {
				if lengthCounter[i] == lengths[i] {
					files[i].Close()
					mapa[i] = false
					continue
				} else {
					newRec, _ = structures.ReadNextRecord(&files[i])
					lengthCounter[i]++

					if strings.Split(string(newRec["key"]), "-")[0] < strings.Split(string(recMin["key"]), "-")[0] {
						m := 0
						for m < len(minimums) {
							files[minimums[m]].Seek(-29-int64(minimums[m+1])-int64(minimums[m+2]), 1)
							lengthCounter[m]--
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
						lengthCounter[i]--

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
			positions = append(positions, currentPos)
			currentPos += 29 + len(recMin["key"]) + len(recMin["value"])
		}

	}

	for f := 0; f < len(files); f++ {
		path := files[f].Name()
		path = path[:len(path)-8]
		os.Remove(path + "-data.db")

	}

	newFile.Seek(0, 0)
	newLenBytes := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(newLenBytes, uint64(len(keys)))
	newFile.Write(newLenBytes)

	newFile.Seek(8, 0)

	posInd := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(posInd, uint64(currentPos))
	newFile.Write(posInd)

	newFile.Seek(0, 2)

	positionsSum := make([]int, len(keys))
	for i := 0; i < len(keys); i++ {

		positionsSum[i] = currentPos
		keySize := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(keySize, uint64(len(keys[i])))

		pos1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(pos1, uint64(positions[i]))

		newFile.Write(keySize)
		newFile.Write([]byte(keys[i]))
		newFile.Write(pos1)
		currentPos += 16 + len(keys[i])
	}

	newFile.Seek(16, 0)
	posSum := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(posSum, uint64(currentPos))
	newFile.Write(posSum)
	newFile.Seek(0, 2)

	len1SumBytes := make([]byte, 8, 8)
	len2SumBytes := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(len1SumBytes, uint64(len(keys[0])))
	binary.LittleEndian.PutUint64(len2SumBytes, uint64(len(keys[len(keys)-1])))

	newFile.Write(len1SumBytes)
	newFile.Write([]byte(keys[0]))

	newFile.Write(len2SumBytes)
	newFile.Write([]byte(keys[len(keys)-1]))

	currentPos += 16 + len(keys[0]) + len(keys[len(keys)-1])

	for i := 0; i < len(positionsSum); i += 1 {
		if i%summaryBlockingFactor == 0 {

			keySize1 := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(keySize1, uint64(len(keys[i])))

			key1 := []byte(keys[i])

			posSum1 := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(posSum1, uint64(positionsSum[i]))

			newFile.Write(keySize1)
			newFile.Write(key1)
			newFile.Write(posSum1)

			currentPos += 16 + len([]byte(keys[i]))
		}
	}

	newFile.Seek(24, 0)
	posBF := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(posBF, uint64(currentPos))

	newFile.Write(posBF)
	newFile.Seek(0, 2)

	bf := structures.CreateBloomFilter(uint(len(keys)), 2) //mozda p treba decimalno
	for i := 0; i < len(keys); i++ {
		bf.Add(keys[i])
	}

	encoder := gob.NewEncoder(newFile)
	err := encoder.Encode(bf)
	if err != nil {
		panic(err)
	}

	newFile.Close()

	SizeTieredSingle(level+1, summaryBlockingFactor)

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SizeTieredMulti(level int, summaryBlockingFactor int) {

	files := make([]os.File, 0)
	mapa := make(map[int]bool)

	for i := 0; true; i++ {
		file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)

		if os.IsNotExist(err) {
			break
		}
		//test
		fmt.Println("data/sstables/usertable-" + fmt.Sprint(level) + "-" + fmt.Sprint(i) + "-data.db")
		files = append(files, *file)
		mapa[i] = true
	}

	if len(files) < 2 {
		return
	}

	newGen := 0
	for i := 0; true; i++ {
		file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(level+1)+"-"+fmt.Sprint(i)+"-data.db", os.O_WRONLY, 0666)
		if os.IsNotExist(err) {
			newGen = i
			break
		}
		file.Close()
	}

	keys := make([]string, 0)
	positions := make([]int, 0)
	currentPos := 0

	recMin := make(map[string][]byte)
	newRec := make(map[string][]byte)
	empty := true
	newMin := 0
	newFile, _ := os.Create("data/sstables/usertable-" + fmt.Sprint(level+1) + "-" + fmt.Sprint(newGen) + "-data.db")
	for true {
		newMin = -1

		minimums := make([]int, 0)
		for i := 0; i < len(files); i++ {
			if mapa[i] {
				recMin, empty = structures.ReadNextRecord(&files[i])
				if !empty {
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

		if newMin == -1 {
			break
		}

		// ako je newMin = len - 1 ? Trebalo bi da samo ne uđe u uslovu i < len(files)

		for i := newMin + 1; i < len(files); i++ {
			if mapa[i] {
				newRec, empty = structures.ReadNextRecord(&files[i])

				if empty {
					files[i].Close()
					mapa[i] = false
					continue
				}

				if strings.Split(string(newRec["key"]), "-")[0] < strings.Split(string(recMin["key"]), "-")[0] {
					m := 0
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

		if recMin["tombstone"][0] == 0 {
			rec := append(recMin["crc"], recMin["timestamp"]...)
			rec = append(rec, recMin["tombstone"]...)
			rec = append(rec, recMin["key_size"]...)
			rec = append(rec, recMin["val_size"]...)
			rec = append(rec, recMin["key"]...)
			rec = append(rec, recMin["value"]...)
			newFile.Write(rec)
			keys = append(keys, string(recMin["key"]))
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

	}

	newFile.Close()

	structures.CreateIndex(keys, positions, "data/sstables/usertable-"+fmt.Sprint(level+1)+"-"+fmt.Sprint(newGen), summaryBlockingFactor)

	bf := structures.CreateBloomFilter(uint(len(keys)), 2)
	for i := 0; i < len(keys); i++ {
		bf.Add(keys[i])
	}
	bf.Write("data/sstables/usertable-" + fmt.Sprint(level+1) + "-" + fmt.Sprint(newGen))
	structures.CreateTOC("data/sstables/usertable-" + fmt.Sprint(level+1) + "-" + fmt.Sprint(newGen))

	SizeTieredMulti(level+1, summaryBlockingFactor)

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func LeveledSingle(level int, summaryBlockingFactor int) {}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func LeveledMulti(level int, summaryBlockingFactor int) {}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

// func SizeTieredSingle(level int, summaryBlockingFactor int) {
// 	var jos bool

// 	for lv := 0; lv < level; lv++ {
// 		generation := 0

// 		for i := 0; true; i++ {
// 			file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lv+1)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
// 			if os.IsNotExist(err) {
// 				generation = i
// 				break
// 			}
// 			file.Close()
// 		}

// 		k := -1

// 		terminate_list := make([]int, 1)

// 		initial := make([]byte, 32)

// 		file1, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lv)+"-0-data.db", os.O_RDWR, 0666)
// 		if os.IsNotExist(err) {
// 			jos = false
// 			k = -1
// 			continue
// 		}

// 		terminate_list[0] = 0
// 		keys := make([]string, 0)
// 		positions := make([]int, 0)
// 		currentPos := 32

// 		for jos = true; jos; {

// 			for i := 1; ; i++ {

// 				keys = make([]string, 0)
// 				positions = make([]int, 0)
// 				currentPos = 32

// 				file2, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lv)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDWR, 0666)
// 				if os.IsNotExist(err) {
// 					jos = false
// 					k = i - 1
// 					file2.Close()
// 					file1.Close()
// 					break
// 				}

// 				if jos {
// 					term := make([]int, 1)
// 					term[0] = i
// 					terminate_list = append(terminate_list, term...)
// 					//postoje dva fajla lv nivoa koje treba spojiti

// 					//novi fajl ima lvl: lv+1, indeks: i

// 					fmt.Println(generation)

// 					new_file, err := os.OpenFile("data/singlesstables/usertable-pomocna-"+fmt.Sprint(i)+"-data.db", os.O_CREATE|os.O_WRONLY, 0666)
// 					if err != nil {
// 						log.Fatal(err)
// 					}
// 					new_file.Write(initial)
// 					newLen := 0

// 					len1Bytes := make([]byte, 8)
// 					len2Bytes := make([]byte, 8)
// 					file1.Read(len1Bytes)
// 					file2.Read(len2Bytes)

// 					len1 := binary.LittleEndian.Uint64(len1Bytes)
// 					len2 := binary.LittleEndian.Uint64(len2Bytes)

// 					file1.Seek(32, 0)
// 					file2.Seek(32, 0)

// 					readRecords1 := 0
// 					readRecords2 := 0

// 					rec1, _ := structures.ReadNextRecord(file1)
// 					rec2, _ := structures.ReadNextRecord(file2)

// 					for {

// 						if readRecords1 >= int(len1) {
// 							for readRecords2 < int(len2) {
// 								if rec2["tombstone"][0] == 1 {
// 									continue
// 								}
// 								rec := append(rec2["crc"], rec2["timestamp"]...)
// 								rec = append(rec, rec2["tombstone"]...)
// 								rec = append(rec, rec2["key_size"]...)
// 								rec = append(rec, rec2["val_size"]...)
// 								rec = append(rec, rec2["key"]...)
// 								rec = append(rec, rec2["value"]...)
// 								new_file.Write(rec)
// 								newLen++

// 								keys = append(keys, string(rec2["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec2["key"])) + int(len(rec2["value"]))

// 								rec2, _ = structures.ReadNextRecord(file2)
// 								readRecords2++
// 							}
// 							break

// 						} else if readRecords2 >= int(len2) {
// 							for readRecords1 < int(len1) {
// 								if rec1["tombstone"][0] == 1 {
// 									continue
// 								}
// 								rec := append(rec1["crc"], rec1["timestamp"]...)
// 								rec = append(rec, rec1["tombstone"]...)
// 								rec = append(rec, rec1["key_size"]...)
// 								rec = append(rec, rec1["val_size"]...)
// 								rec = append(rec, rec1["key"]...)
// 								rec = append(rec, rec1["value"]...)
// 								new_file.Write(rec)
// 								newLen++

// 								keys = append(keys, string(rec1["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec1["key"])) + int(len(rec1["value"]))

// 								rec1, _ = structures.ReadNextRecord(file1)
// 								readRecords1++
// 							}
// 							break

// 						} else {
// 							if string(rec1["key"]) == string(rec2["key"]) { //jednaki kljucevi
// 								t1 := binary.LittleEndian.Uint64(rec1["timestamp"])
// 								t2 := binary.LittleEndian.Uint64(rec2["timestamp"])
// 								if t1 <= t2 && rec2["tombstone"][0] == 0 {
// 									rec := append(rec2["crc"], rec2["timestamp"]...)
// 									rec = append(rec, rec2["tombstone"]...)
// 									rec = append(rec, rec2["key_size"]...)
// 									rec = append(rec, rec2["val_size"]...)
// 									rec = append(rec, rec2["key"]...)
// 									rec = append(rec, rec2["value"]...)
// 									new_file.Write(rec)
// 									newLen++

// 									keys = append(keys, string(rec2["key"]))
// 									positions = append(positions, currentPos)
// 									currentPos += 29 + int(len(rec2["key"])) + int(len(rec2["value"]))

// 								} else if t1 > t2 && rec1["tombstone"][0] == 0 {
// 									rec := append(rec1["crc"], rec1["timestamp"]...)
// 									rec = append(rec, rec1["tombstone"]...)
// 									rec = append(rec, rec1["key_size"]...)
// 									rec = append(rec, rec1["val_size"]...)
// 									rec = append(rec, rec1["key"]...)
// 									rec = append(rec, rec1["value"]...)
// 									new_file.Write(rec)
// 									newLen++

// 									keys = append(keys, string(rec1["key"]))
// 									positions = append(positions, currentPos)
// 									currentPos += 29 + int(len(rec1["key"])) + int(len(rec1["value"]))

// 								}
// 								rec1, _ = structures.ReadNextRecord(file1)
// 								readRecords1++
// 								rec2, _ = structures.ReadNextRecord(file2)
// 								readRecords2++
// 								continue
// 							}

// 							if rec1["tombstone"][0] == 1 {
// 								rec1, _ = structures.ReadNextRecord(file1)
// 								readRecords1++
// 								continue
// 							}
// 							if rec2["tombstone"][0] == 1 {
// 								rec2, _ = structures.ReadNextRecord(file2)
// 								readRecords2++
// 								continue
// 							}

// 							if string(rec1["key"]) < string(rec2["key"]) {
// 								rec := append(rec1["crc"], rec1["timestamp"]...)
// 								rec = append(rec, rec1["tombstone"]...)
// 								rec = append(rec, rec1["key_size"]...)
// 								rec = append(rec, rec1["val_size"]...)
// 								rec = append(rec, rec1["key"]...)
// 								rec = append(rec, rec1["value"]...)
// 								new_file.Write(rec)
// 								newLen++

// 								keys = append(keys, string(rec1["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec1["key"])) + int(len(rec1["value"]))

// 								rec1, _ = structures.ReadNextRecord(file1)
// 								readRecords1++
// 							} else if string(rec1["key"]) > string(rec2["key"]) {
// 								rec := append(rec2["crc"], rec2["timestamp"]...)
// 								rec = append(rec, rec2["tombstone"]...)
// 								rec = append(rec, rec2["key_size"]...)
// 								rec = append(rec, rec2["val_size"]...)
// 								rec = append(rec, rec2["key"]...)
// 								rec = append(rec, rec2["value"]...)
// 								new_file.Write(rec)
// 								newLen++

// 								keys = append(keys, string(rec2["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec2["key"])) + int(len(rec2["value"]))

// 								rec2, _ = structures.ReadNextRecord(file2)
// 								readRecords1++
// 							}
// 						}

// 					}
// 					new_file.Seek(0, 0)
// 					newLenBytes := make([]byte, 8, 8)
// 					binary.LittleEndian.PutUint64(newLenBytes, uint64(newLen))
// 					new_file.Write(newLenBytes)
// 					new_file.Close()
// 					err1 := file1.Close()
// 					if err1 != nil {
// 						log.Fatal(err1)
// 					}
// 					file2.Close()

// 					file1, err = os.OpenFile("data/singlesstables/usertable-pomocna-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
// 					if err != nil {
// 						log.Fatal(err)
// 					}

// 				}

// 			}

// 		}

// 		os.Rename("data/singlesstables/usertable-pomocna-"+fmt.Sprint(k)+"-data.db",
// 			"data/singlesstables/usertable-"+fmt.Sprint(lv+1)+"-"+fmt.Sprint(generation)+"-data.db")

// 		fileFinal, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lv+1)+"-"+fmt.Sprint(generation)+"-data.db", os.O_WRONLY, 0666)
// 		if err != nil {
// 			log.Fatal(err)
// 		}

// 		fileFinal.Seek(8, 0)

// 		posInd := make([]byte, 8, 8)
// 		binary.LittleEndian.PutUint64(posInd, uint64(currentPos))
// 		fileFinal.Write(posInd)

// 		fileFinal.Seek(0, 2)

// 		positionsSum := make([]int, len(keys))
// 		for i := 0; i < len(keys); i++ {

// 			positionsSum[i] = currentPos
// 			keySize := make([]byte, 8, 8)
// 			binary.LittleEndian.PutUint64(keySize, uint64(len(keys[i])))

// 			pos1 := make([]byte, 8, 8)
// 			binary.LittleEndian.PutUint64(pos1, uint64(positions[i]))

// 			fileFinal.Write(keySize)
// 			fileFinal.Write([]byte(keys[i]))
// 			fileFinal.Write(pos1)
// 			currentPos += 16 + len(keys[i])
// 		}

// 		fileFinal.Seek(116, 0)
// 		posSum := make([]byte, 8, 8)
// 		binary.LittleEndian.PutUint64(posSum, uint64(currentPos))
// 		fileFinal.Write(posSum)
// 		fileFinal.Seek(0, 2)

// 		len1SumBytes := make([]byte, 8, 8)
// 		len2SumBytes := make([]byte, 8, 8)
// 		binary.LittleEndian.PutUint64(len1SumBytes, uint64(len(keys[0])))
// 		binary.LittleEndian.PutUint64(len2SumBytes, uint64(len(keys[len(keys)-1])))

// 		fileFinal.Write(len1SumBytes)
// 		fileFinal.Write([]byte(keys[0]))

// 		fileFinal.Write(len2SumBytes)
// 		fileFinal.Write([]byte(keys[len(keys)-1]))

// 		currentPos += 16 + len(keys[0]) + len(keys[len(keys)-1])

// 		for i := 0; i < len(positionsSum); i += 1 {
// 			if i%summaryBlockingFactor == 0 {

// 				keySize1 := make([]byte, 8, 8)
// 				binary.LittleEndian.PutUint64(keySize1, uint64(len(keys[i])))

// 				key1 := []byte(keys[i])

// 				posSum1 := make([]byte, 8, 8)
// 				binary.LittleEndian.PutUint64(posSum1, uint64(positionsSum[i]))

// 				fileFinal.Write(keySize1)
// 				fileFinal.Write(key1)
// 				fileFinal.Write(posSum1)

// 				currentPos += 16 + len([]byte(keys[i]))
// 			}
// 		}

// 		fileFinal.Seek(24, 0)
// 		posBF := make([]byte, 8, 8)
// 		binary.LittleEndian.PutUint64(posBF, uint64(currentPos))

// 		fileFinal.Write(posBF)
// 		fileFinal.Seek(0, 2)

// 		bf := structures.CreateBloomFilter(uint(len(keys)), 2) //mozda p treba decimalno
// 		for i := 0; i < len(keys); i++ {
// 			bf.Add(keys[i])
// 		}

// 		fileFinal.Close()

// 		encoder := gob.NewEncoder(fileFinal)
// 		err = encoder.Encode(bf)
// 		if err != nil {
// 			panic(err)
// 		}

// 		if len(terminate_list) > 1 {
// 			for q := 0; q < len(terminate_list); q++ {
// 				err := os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-data.db")
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 			}

// 			for pom := 1; pom < k; pom++ {

// 				err := os.Remove("data/singlesstables/usertable-pomocna-" + fmt.Sprint(pom) + "-data.db")
// 				if os.IsNotExist(err) {
// 					break
// 				}
// 			}
// 		}

// 	}

// }

///////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////
///////////////////////////////////////////////////////////////////////////////////

// func SizeTieredMulti(level int, summaryBlockingFactor int) {

// 	var jos bool

// 	for lv := 0; lv < level; lv++ {
// 		generation := 0

// 		for i := 0; true; i++ {
// 			file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lv+1)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
// 			if os.IsNotExist(err) {
// 				generation = i
// 				break
// 			}
// 			file.Close()
// 		}

// 		k := -1

// 		terminate_list := make([]int, 1)

// 		file1, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lv)+"-0-data.db", os.O_RDWR, 0666)
// 		if os.IsNotExist(err) {
// 			jos = false
// 			k = -1
// 			continue
// 		}

// 		terminate_list[0] = 0
// 		keys := make([]string, 0)
// 		positions := make([]int, 0)
// 		currentPos := 0

// 		for jos = true; jos; {

// 			for i := 1; ; i++ {
// 				keys = make([]string, 0)
// 				positions = make([]int, 0)
// 				currentPos = 0

// 				file2, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lv)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDWR, 0666)
// 				if os.IsNotExist(err) {
// 					jos = false
// 					k = i - 1
// 					file2.Close()
// 					file1.Close()
// 					break
// 				}

// 				if jos {
// 					term := make([]int, 1)
// 					term[0] = i
// 					terminate_list = append(terminate_list, term...)
// 					//postoje dva fajla lv nivoa koje treba spojiti

// 					//novi fajl ima lvl: lv+1, indeks: i

// 					fmt.Println(generation)

// 					new_file, err := os.OpenFile("data/sstables/usertable-pomocna-"+fmt.Sprint(i)+"-data.db", os.O_CREATE|os.O_WRONLY, 0666)
// 					if err != nil {
// 						log.Fatal(err)
// 					}

// 					empty1 := false
// 					empty2 := false

// 					rec1, empty1 := structures.ReadNextRecord(file1)
// 					rec2, empty2 := structures.ReadNextRecord(file2)

// 					for {

// 						if empty1 {
// 							for !empty2 {
// 								if rec2["tombstone"][0] == 1 {
// 									continue
// 								}
// 								rec := append(rec2["crc"], rec2["timestamp"]...)
// 								rec = append(rec, rec2["tombstone"]...)
// 								rec = append(rec, rec2["key_size"]...)
// 								rec = append(rec, rec2["val_size"]...)
// 								rec = append(rec, rec2["key"]...)
// 								rec = append(rec, rec2["value"]...)
// 								new_file.Write(rec)

// 								keys = append(keys, string(rec2["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec2["key"])) + int(len(rec2["value"]))

// 								rec2, empty2 = structures.ReadNextRecord(file2)
// 							}
// 							break

// 						} else if empty2 {
// 							for !empty1 {
// 								if rec1["tombstone"][0] == 1 {
// 									continue
// 								}
// 								rec := append(rec1["crc"], rec1["timestamp"]...)
// 								rec = append(rec, rec1["tombstone"]...)
// 								rec = append(rec, rec1["key_size"]...)
// 								rec = append(rec, rec1["val_size"]...)
// 								rec = append(rec, rec1["key"]...)
// 								rec = append(rec, rec1["value"]...)
// 								new_file.Write(rec)

// 								keys = append(keys, string(rec1["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec1["key"])) + int(len(rec1["value"]))

// 								rec1, empty1 = structures.ReadNextRecord(file1)
// 							}
// 							break

// 						} else {
// 							if string(rec1["key"]) == string(rec2["key"]) { //jednaki kljucevi
// 								t1 := binary.LittleEndian.Uint64(rec1["timestamp"])
// 								t2 := binary.LittleEndian.Uint64(rec2["timestamp"])
// 								if t1 <= t2 && rec2["tombstone"][0] == 0 {
// 									rec := append(rec2["crc"], rec2["timestamp"]...)
// 									rec = append(rec, rec2["tombstone"]...)
// 									rec = append(rec, rec2["key_size"]...)
// 									rec = append(rec, rec2["val_size"]...)
// 									rec = append(rec, rec2["key"]...)
// 									rec = append(rec, rec2["value"]...)
// 									new_file.Write(rec)

// 									keys = append(keys, string(rec2["key"]))
// 									positions = append(positions, currentPos)
// 									currentPos += 29 + int(len(rec2["key"])) + int(len(rec2["value"]))

// 								} else if t1 > t2 && rec1["tombstone"][0] == 0 {
// 									rec := append(rec1["crc"], rec1["timestamp"]...)
// 									rec = append(rec, rec1["tombstone"]...)
// 									rec = append(rec, rec1["key_size"]...)
// 									rec = append(rec, rec1["val_size"]...)
// 									rec = append(rec, rec1["key"]...)
// 									rec = append(rec, rec1["value"]...)
// 									new_file.Write(rec)

// 									keys = append(keys, string(rec1["key"]))
// 									positions = append(positions, currentPos)
// 									currentPos += 29 + int(len(rec1["key"])) + int(len(rec1["value"]))

// 								}
// 								rec1, empty1 = structures.ReadNextRecord(file1)
// 								rec2, empty2 = structures.ReadNextRecord(file2)
// 								continue
// 							}

// 							if rec1["tombstone"][0] == 1 {
// 								rec1, empty1 = structures.ReadNextRecord(file1)
// 								continue
// 							}
// 							if rec2["tombstone"][0] == 1 {
// 								rec2, empty2 = structures.ReadNextRecord(file2)
// 								continue
// 							}

// 							if string(rec1["key"]) < string(rec2["key"]) {
// 								rec := append(rec1["crc"], rec1["timestamp"]...)
// 								rec = append(rec, rec1["tombstone"]...)
// 								rec = append(rec, rec1["key_size"]...)
// 								rec = append(rec, rec1["val_size"]...)
// 								rec = append(rec, rec1["key"]...)
// 								rec = append(rec, rec1["value"]...)
// 								new_file.Write(rec)

// 								keys = append(keys, string(rec1["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec1["key"])) + int(len(rec1["value"]))

// 								rec1, empty1 = structures.ReadNextRecord(file1)
// 							} else if string(rec1["key"]) > string(rec2["key"]) {
// 								rec := append(rec2["crc"], rec2["timestamp"]...)
// 								rec = append(rec, rec2["tombstone"]...)
// 								rec = append(rec, rec2["key_size"]...)
// 								rec = append(rec, rec2["val_size"]...)
// 								rec = append(rec, rec2["key"]...)
// 								rec = append(rec, rec2["value"]...)
// 								new_file.Write(rec)

// 								keys = append(keys, string(rec2["key"]))
// 								positions = append(positions, currentPos)
// 								currentPos += 29 + int(len(rec2["key"])) + int(len(rec2["value"]))

// 								rec2, empty2 = structures.ReadNextRecord(file2)
// 							}
// 						}

// 					}
// 					new_file.Close()
// 					err1 := file1.Close()
// 					if err1 != nil {
// 						log.Fatal(err1)
// 					}
// 					file2.Close()

// 					file1, err = os.OpenFile("data/sstables/usertable-pomocna-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)
// 					if err != nil {
// 						log.Fatal(err)
// 					}

// 				}

// 			}

// 		}

// 		os.Rename("data/sstables/usertable-pomocna-"+fmt.Sprint(k)+"-data.db",
// 			"data/sstables/usertable-"+fmt.Sprint(lv+1)+"-"+fmt.Sprint(generation)+"-data.db")

// 		path := "data/sstables/usertable-" + fmt.Sprint(lv+1) + "-" + fmt.Sprint(generation)

// 		bf := structures.CreateBloomFilter(uint(len(keys)), 2)
// 		for i := 0; i < len(keys); i++ {
// 			bf.Add(keys[i])
// 		}
// 		bf.Write(path)
// 		structures.CreateIndex(keys, positions, path, summaryBlockingFactor)

// 		if len(terminate_list) > 1 {
// 			for q := 0; q < len(terminate_list); q++ {
// 				err := os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-data.db")
// 				if err != nil {
// 					log.Fatal(err)
// 				}

// 				err = os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-index.db")
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				err = os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-summary.db")
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				err = os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-filter.db")
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 				err = os.Remove("data/sstables/usertable-" + fmt.Sprint(lv) + "-" + fmt.Sprint(terminate_list[q]) + "-TOC.txt")
// 				if err != nil {
// 					log.Fatal(err)
// 				}
// 			}

// 			for pom := 1; pom < k; pom++ {

// 				err := os.Remove("data/sstables/usertable-pomocna-" + fmt.Sprint(pom) + "-data.db")
// 				if os.IsNotExist(err) {
// 					break
// 				}
// 			}
// 		}

// 	}
// }
