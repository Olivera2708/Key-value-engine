package structures

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"os"
	"strconv"
)

const (
	SUMMARY_BLOCKING_FACTOR = 20 // promeniti da bude iz config fajla
)

type SSTable struct {
	path  string
	index *Index
}

func CreateSSTable(memtable *Memtable, generation int) *SSTable {
	path := "data/sstables/usertable-" + strconv.FormatInt(int64(generation), 10)

	outFile, err := os.Create(path + "-data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	fileWriter := bufio.NewWriter(outFile)

	currentPos := 0

	keys := make([]string, 0)
	// values := make([][]byte, 0)
	positions := make([]int, 0)

	for node := memtable.data.head.next[0]; node != nil; node = node.next[0] {
		key := node.key
		keys = append(keys, key)

		value := node.value
		// values = append(values, value)

		positions = append(positions, currentPos)

		timeStamp := node.timestamp
		timeStamp1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(timeStamp1, timeStamp)
		//copy(timeStamp1, timeStamp)

		tombstone := uint8(node.status)
		if tombstone > 0 {
			tombstone = 1
		}

		key1 := []byte(key)

		keySize := uint64(len(key1))
		keySize1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(keySize1, keySize)

		valueSize := uint64(len(value))
		valueSize1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(valueSize1, valueSize)

		tombstone1 := make([]byte, 1, 1)
		tombstone1[0] = tombstone

		record := append(timeStamp1, tombstone1...)
		record = append(record, keySize1...)
		record = append(record, valueSize1...)
		record = append(record, key1...)
		record = append(record, value...)

		crc := crc32.ChecksumIEEE(record)
		crc1 := make([]byte, 4, 4)

		binary.LittleEndian.PutUint32(crc1, crc)

		fileWriter.Write(crc1)
		fileWriter.Write(timeStamp1)
		fileWriter.WriteByte(uint8(tombstone))
		fileWriter.Write(keySize1)
		fileWriter.Write(valueSize1)
		fileWriter.Write(key1)
		fileWriter.Write(value)
		fileWriter.Flush()

		currentPos += 29 + int(len(key1)) + int(len(value))
	}

	bf := CreateBloomFilter(uint(len(keys)), 2) //mozda p treba decimalno
	for i := 0; i < len(keys); i++ {
		bf.Add(keys[i])
	}

	bf.Write(path)
	index := CreateIndex(keys, positions, path)
	sstable := SSTable{path: path, index: index}
	CreateTOC(&sstable)

	return &sstable
}

type Index struct {
	path    string
	summary *Summary
}

func CreateIndex(keys []string, positions []int, path string) *Index {
	indexPath := path + "-index.db"

	outFile, err := os.Create(indexPath)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	fileWriter := bufio.NewWriter(outFile)

	currentPos := 0
	keysSum := make([]string, len(keys)+2, len(keys)+2)
	positionsSum := make([]int, len(keys), len(keys))
	keySizesSum := make([]int, len(keys), len(keys))
	for i := 0; i < len(keys); i += 1 {

		keysSum[i] = keys[i]
		positionsSum[i] = currentPos

		pos1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(pos1, uint64(positions[i]))

		key1 := []byte(keys[i])

		keySizesSum[i] = len(key1)

		keySize := uint64(len(key1))
		keySize1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(keySize1, keySize)

		fileWriter.Write(keySize1)
		fileWriter.Write(key1)
		fileWriter.Write(pos1)
		fileWriter.Flush()

		currentPos += len([]byte(keys[i])) + 4
	}
	keysSum[len(keys)] = keys[0]
	keysSum[len(keys)+1] = keys[len(keys)-1]

	summary := CreateSummary(keySizesSum, keysSum, positionsSum, path)

	index := Index{path: indexPath, summary: summary}
	return &index
}

type Summary struct {
	path string
}

func CreateSummary(keySizesSum []int, keysSum []string, positionsSum []int, path string) *Summary {
	sumPath := path + "-summary.db"

	outFile, err := os.Create(sumPath)
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	fileWriter := bufio.NewWriter(outFile)
	len1 := make([]byte, 8, 8)
	len2 := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(len1, uint64(len(keysSum[len(keysSum)-2])))
	binary.LittleEndian.PutUint64(len2, uint64(len(keysSum[len(keysSum)-1])))
	fileWriter.Write(len1)
	fileWriter.Write([]byte(keysSum[len(keysSum)-2]))
	fileWriter.Write(len2)
	fileWriter.Write([]byte(keysSum[len(keysSum)-1]))

	for i := 0; i < len(positionsSum); i += 1 {
		if i%SUMMARY_BLOCKING_FACTOR == 0 {

			keySize1 := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(keySize1, uint64(keySizesSum[i]))

			key1 := []byte(keysSum[i])

			posSum1 := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(posSum1, uint64(positionsSum[i]))

			fileWriter.Write(keySize1)
			fileWriter.Write(key1)
			fileWriter.Write(posSum1)
			fileWriter.Flush()
		}
	}

	summary := Summary{path: sumPath}
	return &summary
}

func ReadSummary(path string, key string) (bool, []byte) {
	startLen := make([]byte, 8)
	endLen := make([]byte, 8)
	file, err := os.OpenFile(path+"-summary.db", os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Read(startLen)
	startL := binary.LittleEndian.Uint64(startLen)
	startIndex := make([]byte, startL)
	file.Read(startIndex)
	file.Read(endLen)
	endL := binary.LittleEndian.Uint64(endLen)
	endIndex := make([]byte, endL)
	file.Read(endIndex)
	if key >= string(startIndex) && key <= string(endIndex) {
		position := make([]byte, 8)
		for true {
			keyLen := make([]byte, 8)
			file.Read(keyLen)
			keyLenNum := binary.LittleEndian.Uint64(keyLen)
			key1 := make([]byte, keyLenNum)
			file.Read(key1)
			if string(key1) > key {
				file.Seek(-(int64(keyLenNum) + 16), 1)
				file.Read(position)
				pos := binary.LittleEndian.Uint64(position)
				found, value := ReadIndex(path, key, pos)
				return found, value
			} else if string(key1) == key {
				file.Read(position)
				pos := binary.LittleEndian.Uint64(position)
				found, value := ReadIndex(path, key, pos)
				return found, value
			}
			file.Read(position)
		}
	}
	return false, nil
}

func ReadIndex(path string, key string, position uint64) (bool, []byte) {
	fmt.Println("Indeks")
	file, err := os.OpenFile(path+"-index.db", os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(position), 0)
	position1 := make([]byte, 8)
	for true {
		keyLen := make([]byte, 8)
		file.Read(keyLen)
		keyLenNum := binary.LittleEndian.Uint64(keyLen)
		key1 := make([]byte, keyLenNum)
		file.Read(key1)
		if key == string(key1) {
			file.Read(position1)
			pos := binary.LittleEndian.Uint64(position1)
			value := ReadSSTable(path, key, pos)
			return true, value
		} else if key < string(key1) {
			return false, nil
		}
		file.Seek(8, 1)
	}
	return false, nil
}

func ReadSSTable(path, key string, position uint64) []byte {
	fmt.Println("SStable")
	file, err := os.OpenFile(path+"-data.db", os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(position)+13, 0)
	keyLen := make([]byte, 8, 8)
	file.Read(keyLen)
	keyLenNum := binary.LittleEndian.Uint64(keyLen)
	valLen := make([]byte, 8, 8)
	file.Read(valLen)
	valLenNum := binary.LittleEndian.Uint64(valLen)
	file.Seek(int64(keyLenNum), 1)
	value := make([]byte, valLenNum, valLenNum)
	file.Read(value)
	return value
}

func CreateTOC(sstable *SSTable) {
	path := sstable.path + "-TOC.txt"
	inFile, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer inFile.Close()

	fileWriter := bufio.NewWriter(inFile)

	fileWriter.WriteString(sstable.path + "\n")
	fileWriter.WriteString(sstable.path + "-data.db\n")
	fileWriter.WriteString(sstable.path + "-index.db\n")
	fileWriter.WriteString(sstable.path + "-summary.db\n")
	fileWriter.Flush()

	return
}
