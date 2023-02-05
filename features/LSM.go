package features

import (
	"Projekat/global"
	"Projekat/structures"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"math"
	"os"
	"strings"
)

func LSM(sstype int, summaryBlockingFactor int) int {
	if global.LSMAlgorithm == 1 {
		if sstype == 1 {
			SizeTieredSingle(0, summaryBlockingFactor)
		} else {
			SizeTieredMulti(0, summaryBlockingFactor)
		}
	} else {
		if sstype == 1 {
			LeveledSingle(summaryBlockingFactor)
		} else {
			LeveledMulti(summaryBlockingFactor)
		}
	}
	return 0
}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SizeTieredSingle(level int, summaryBlockingFactor int) {
	if level > global.LSMTreeLevel {
		return
	}

	totalFiles := 0

	for j := 0; true; j++ {
		file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)

		if os.IsNotExist(err) {
			break
		}
		totalFiles += 1
		file.Close()
	}

	if totalFiles < global.LSMMinimum {
		return
	}

	for j := 0; j < int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum))); j++ {

		files := make([]os.File, 0)
		mapa := make(map[int]bool)
		lengths := make([]uint64, 0)

		for i := j * global.LSMMinimum; i < (j+1)*global.LSMMinimum; i++ {

			file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)

			if os.IsNotExist(err) {
				break
			}
			lenBytes := make([]byte, 8, 8)
			file.Read(lenBytes)
			len := binary.LittleEndian.Uint64(lenBytes)
			//fmt.Println(len)
			lengths = append(lengths, len)
			file.Seek(32, 0)

			files = append(files, *file)

			mapa[i-j*global.LSMMinimum] = true
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
		values := make([][]byte, 0)
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
						//fmt.Print(lengthCounter)
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
				values = append(values, recMin["value"])
				keys = append(keys, string(recMin["key"]))
				positions = append(positions, currentPos)
				currentPos += 29 + len(recMin["key"]) + len(recMin["value"])
			}

		}

		for f := 0; f < len(files); f++ {
			path := files[f].Name()
			path = path[:len(path)-8]
			os.Remove(path + "-data.db")
			os.Remove(path + "-Metadata.txt")

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

		merkle := structures.CreateMerkleTree(values)
		structures.WriteMerkleInFile(merkle, "data/singlesstables/usertable-"+fmt.Sprint(level+1)+"-"+fmt.Sprint(newGen))

	}

	for j := int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum))) * global.LSMMinimum; j < totalFiles; j++ {
		os.Rename("data/singlesstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-data.db",
			"data/singlesstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-data.db")
		os.Rename("data/singlesstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-Metadata.txt",
			"data/singlesstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-Metadata.txt")
	}

	SizeTieredSingle(level+1, summaryBlockingFactor)

}

/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
/////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

func SizeTieredMulti(level int, summaryBlockingFactor int) {

	if level > global.LSMTreeLevel {
		return
	}

	totalFiles := 0

	for j := 0; true; j++ {
		file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)

		if os.IsNotExist(err) {
			break
		}
		totalFiles += 1
		file.Close()
	}

	if totalFiles < global.LSMMinimum {
		return
	}

	for j := 0; j < int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum))); j++ {

		files := make([]os.File, 0)
		mapa := make(map[int]bool)

		for i := j * global.LSMMinimum; i < (j+1)*global.LSMMinimum; i++ {

			file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(i)+"-data.db", os.O_RDONLY, 0666)

			if os.IsNotExist(err) {
				break
			}
			//test
			// fmt.Println("data/sstables/usertable-" + fmt.Sprint(level) + "-" + fmt.Sprint(i) + "-data.db")
			files = append(files, *file)
			mapa[i-j*global.LSMMinimum] = true
		}

		if len(files) < global.LSMMinimum {
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
		values := make([][]byte, 0)
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

		structures.CreateIndex(keys, positions, "data/sstables/usertable-"+fmt.Sprint(level+1)+"-"+fmt.Sprint(newGen), summaryBlockingFactor)

		bf := structures.CreateBloomFilter(uint(len(keys)), 2)
		for i := 0; i < len(keys); i++ {
			bf.Add(keys[i])
		}
		bf.Write("data/sstables/usertable-" + fmt.Sprint(level+1) + "-" + fmt.Sprint(newGen))
		structures.CreateTOC("data/sstables/usertable-" + fmt.Sprint(level+1) + "-" + fmt.Sprint(newGen))

		merkle := structures.CreateMerkleTree(values)
		structures.WriteMerkleInFile(merkle, "data/sstables/usertable-"+fmt.Sprint(level+1)+"-"+fmt.Sprint(newGen))
	}

	for j := int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum))) * global.LSMMinimum; j < totalFiles; j++ {
		os.Rename("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-data.db",
			"data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-data.db")
		os.Rename("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-index.db",
			"data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-index.db")
		os.Rename("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-summary.db",
			"data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-summary.db")
		os.Rename("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-filter.db",
			"data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-filter.db")
		os.Rename("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-TOC.txt",
			"data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-TOC.txt")
		os.Rename("data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j)+"-Metadata.txt",
			"data/sstables/usertable-"+fmt.Sprint(level)+"-"+fmt.Sprint(j-int(math.Floor(float64(totalFiles)/float64(global.LSMMinimum)))*global.LSMMinimum)+"-Metadata.txt")
	}

	SizeTieredMulti(level+1, summaryBlockingFactor)

}

func LeveledMulti(summaryBlockingFactor int) {

	for lvl := 0; lvl < global.LSMTreeLevel-1; lvl++ {
		totalFiles := 0

		for j := 0; true; j++ {
			file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)

			if os.IsNotExist(err) {
				break
			}
			totalFiles += 1
			file.Close()
		}

		if totalFiles < global.LSMMinimum {
			return
		}

		file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-data.db", os.O_RDONLY, 0666)
		if os.IsNotExist(err) {
			os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-0-data.db",
				"data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-data.db")

			os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-0-index.db",
				"data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-index.db")

			os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-0-summary.db",
				"data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-summary.db")

			os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-0-filter.db",
				"data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-filter.db")

			os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-0-TOC.txt",
				"data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-TOC.txt")

			os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-0-Metadata.txt",
				"data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-Metadata.txt")

			for f := 1; f < totalFiles; f++ {
				os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-data.db",
					"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-data.db")

				os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-index.db",
					"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-index.db")

				os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-summary.db",
					"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-summary.db")

				os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-filter.db",
					"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-filter.db")

				os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-TOC.txt",
					"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-TOC.txt")

				os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-Metadata.txt",
					"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-Metadata.txt")

			}
			totalFiles--
		}
		file.Close()

		//(lvl+1, 0)
		primary_summ, _ := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-summary.db", os.O_RDONLY, 0666)
		len1Bytes := make([]byte, 8, 8)
		len2Bytes := make([]byte, 8, 8)
		primary_summ.Read(len1Bytes)
		len1 := binary.LittleEndian.Uint64(len1Bytes)
		startIndex := make([]byte, int(len1), int(len1))
		primary_summ.Read(startIndex)

		primary_summ.Read(len2Bytes)
		len2 := binary.LittleEndian.Uint64(len2Bytes)
		endIndex := make([]byte, int(len2), int(len2))
		primary_summ.Read(endIndex)
		primary_summ.Close()

		files := make([]os.File, 0)
		mapa := make(map[int]bool)

		for j := 0; j < totalFiles; j++ {
			i := 0
			file, _ := os.Open("data/sstables/usertable-" + fmt.Sprint(lvl) + "-" + fmt.Sprint(j) + "-summary.db")

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
				file, _ = os.Open("data/sstables/usertable-" + fmt.Sprint(lvl) + "-" + fmt.Sprint(j) + "-data.db")
				files = append(files, *file)
				mapa[i] = true
				i += 1
			}
		}

		if len(files) > 0 {
			primary_file, _ := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl+1)+"-0-data.db", os.O_RDONLY, 0666)

			newFile, _ := os.Create("data/sstables/pomocna-data.db")

			keys := make([]string, 0)
			values := make([][]byte, 0)
			positions := make([]int, 0)
			currentPos := 0

			recMin := make(map[string][]byte)
			newRec := make(map[string][]byte)
			empty := false
			emptyPrim := false
			newMin := 0
			minimums := make([]int, 0)

			for {
				newMin = -1
				minimums = make([]int, 0)

				if !emptyPrim {
					recMin, emptyPrim = structures.ReadNextRecord(primary_file)
					if emptyPrim {
						primary_file.Close()
					}
				}
				if emptyPrim {
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
				}

				if newMin == -1 && emptyPrim {
					break
				}

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
			path := primary_file.Name()
			path = path[:len(path)-8]
			os.Remove(path + "-data.db")
			os.Remove(path + "-index.db")
			os.Remove(path + "-summary.db")
			os.Remove(path + "-filter.db")
			os.Remove(path + "-TOC.txt")
			os.Remove(path + "-Metadata.txt")

			newFile.Close()

			os.Rename(newFile.Name(), path+"-data.db")

			structures.CreateIndex(keys, positions, path, summaryBlockingFactor)
			bf := structures.CreateBloomFilter(uint(len(keys)), 2)
			for i := 0; i < len(keys); i++ {
				bf.Add(keys[i])
			}
			bf.Write(path)
			structures.CreateTOC(path)

			merkle := structures.CreateMerkleTree(values)
			structures.WriteMerkleInFile(merkle, path)

			j := 0
			for i := 0; i <= totalFiles; i++ {
				file, err := os.OpenFile("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_WRONLY, 0666)
				if os.IsNotExist(err) {
					continue
				}
				file.Close()
				if j != i {
					os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db",
						"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-data.db")
					os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-index.db",
						"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-index.db")
					os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-summary.db",
						"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-summary.db")
					os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-filter.db",
						"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-filter.db")
					os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-TOC.txt",
						"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-TOC.txt")
					os.Rename("data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-Metadata.txt",
						"data/sstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-Metadata.txt")
				}

				j++
			}

		}

	}

}

// ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
func LeveledSingle(summaryBlockingFactor int) {

	for lvl := 0; lvl < global.LSMTreeLevel-1; lvl++ {
		totalFiles := 0

		for j := 0; true; j++ {
			file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-data.db", os.O_RDONLY, 0666)

			if os.IsNotExist(err) {
				break
			}
			totalFiles += 1
			file.Close()
		}

		if totalFiles < global.LSMMinimum {
			return
		}

		file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl+1)+"-0-data.db", os.O_RDONLY, 0666)
		if os.IsNotExist(err) {
			os.Rename("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-0-data.db",
				"data/singlesstables/usertable-"+fmt.Sprint(lvl+1)+"-0-data.db")

			os.Rename("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-0-Metadata.txt",
				"data/singlesstables/usertable-"+fmt.Sprint(lvl+1)+"-0-Metadata.txt")

			for f := 1; f < totalFiles; f++ {
				os.Rename("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-data.db",
					"data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-data.db")

				os.Rename("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f)+"-Metadata.txt",
					"data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(f-1)+"-Metadata.txt")

			}
			totalFiles--
		}
		file.Close()

		//(lvl+1, 0)
		primary_file, _ := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl+1)+"-0-data.db", os.O_RDONLY, 0666)

		lenPrimb := make([]byte, 8, 8)
		primary_file.Read(lenPrimb)
		lengthPrim := binary.LittleEndian.Uint64(lenPrimb)
		primCounter := 0

		primary_file.Seek(16, 0)
		posSumPrim := make([]byte, 8, 8)
		primary_file.Read(posSumPrim)
		posSumPrimInt := binary.LittleEndian.Uint64(posSumPrim)
		primary_file.Seek(int64(posSumPrimInt), 0)

		len1Bytes := make([]byte, 8, 8)
		len2Bytes := make([]byte, 8, 8)

		primary_file.Read(len1Bytes)
		len1 := binary.LittleEndian.Uint64(len1Bytes)
		startIndex := make([]byte, int(len1), int(len1))
		primary_file.Read(startIndex)

		primary_file.Read(len2Bytes)
		len2 := binary.LittleEndian.Uint64(len2Bytes)
		endIndex := make([]byte, int(len2), int(len2))
		primary_file.Read(endIndex)
		primary_file.Seek(32, 0)

		files := make([]os.File, 0)
		lengths := make([]uint64, 0)
		mapa := make(map[int]bool)
		i := 0

		for j := 0; j < totalFiles; j++ {
			file, _ := os.Open("data/singlesstables/usertable-" + fmt.Sprint(lvl) + "-" + fmt.Sprint(j) + "-data.db")

			file.Seek(16, 0)
			posSum := make([]byte, 8, 8)
			file.Read(posSum)
			posSumInt := binary.LittleEndian.Uint64(posSum)
			file.Seek(int64(posSumInt), 0)

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

			if !((strings.Split(string(startIndex), "-")[0] > strings.Split(string(key2), "-")[0]) || (strings.Split(string(endIndex), "-")[0] < strings.Split(string(key1), "-")[0])) {
				file.Seek(0, 0)
				lenBytes := make([]byte, 8, 8)
				file.Read(lenBytes)
				len := binary.LittleEndian.Uint64(lenBytes)
				lengths = append(lengths, len)
				file.Seek(32, 0)
				files = append(files, *file)
				mapa[i] = true
				i += 1
			} else {
				file.Close()
			}
		}

		if len(files) > 0 {

			newFile, _ := os.Create("data/singlesstables/pomocna-data.db")

			keys := make([]string, 0)
			values := make([][]byte, 0)
			positions := make([]int, 0)
			currentPos := 32
			lengthCounter := make([]uint64, len(lengths))
			// isPrim := false
			//primeKeyLen := 0
			//primeValLen := 0

			recMin := make(map[string][]byte)
			newRec := make(map[string][]byte)
			newMin := 0

			initialZeros := make([]byte, 32)
			newFile.Write(initialZeros)

			for {
				// isPrim = true
				newMin = -1
				minimums := make([]int, 0)

				if primCounter < int(lengthPrim) {
					recMin, _ = structures.ReadNextRecord(primary_file)
					primCounter++
					if primCounter == int(lengthPrim) {
						primary_file.Close()
					} //else {
					//primeKeyLen = int(len(recMin["key"]))
					//primeValLen = int(len(recMin["value"]))
					//}
				}
				if primCounter == int(lengthPrim) {
					for i := 0; i < len(files); i++ {
						if mapa[i] {
							if lengthCounter[i] < lengths[i] {
								recMin, _ = structures.ReadNextRecord(&files[i])
								lengthCounter[i]++
								// isPrim = false
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

				if newMin == -1 && primCounter == int(lengthPrim) {
					break
				}

				for i := newMin + 1; i < len(files); i++ {
					if mapa[i] {
						if lengthCounter[i] == lengths[i] {

							files[i].Close()
							mapa[i] = false
							continue
						} else {
							newRec, _ = structures.ReadNextRecord(&files[i])
							lengthCounter[i]++
						}

						if strings.Split(string(newRec["key"]), "-")[0] < strings.Split(string(recMin["key"]), "-")[0] {
							m := 0

							// isPrim = false

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
				/*
					if !isPrim {
						//primary_file.Seek(-29-int64(primeKeyLen)-int64(primeValLen), 1)
					}
				*/
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
				os.Remove(path + "-Metadata.txt")
			}
			path := primary_file.Name()
			path = path[:len(path)-8]
			os.Remove(path + "-data.db")
			os.Remove(path + "-Metadata.txt")

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

			bf := structures.CreateBloomFilter(uint(len(keys)), 2)
			for i := 0; i < len(keys); i++ {
				bf.Add(keys[i])
			}

			encoder := gob.NewEncoder(newFile)
			err := encoder.Encode(bf)
			if err != nil {
				panic(err)
			}

			newFile.Close()

			merkle := structures.CreateMerkleTree(values)
			structures.WriteMerkleInFile(merkle, path) //<<<<

			os.Rename(newFile.Name(), path+"-data.db")
			j := 0
			for i := 0; i <= totalFiles; i++ {
				file, err := os.OpenFile("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db", os.O_WRONLY, 0666)
				if os.IsNotExist(err) {
					continue
				}
				file.Close()
				if j != i {
					os.Rename("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-data.db",
						"data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-data.db")
					os.Rename("data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(i)+"-Metadata.txt",
						"data/singlesstables/usertable-"+fmt.Sprint(lvl)+"-"+fmt.Sprint(j)+"-Metadata.txt")
				}

				j++
			}

		}

	}

}
