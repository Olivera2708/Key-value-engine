package structures

import (
	"bufio"
	"encoding/binary"
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
	values := make([][]byte, 0)
	positions := make([]int, 0)

	for node := memtable.data.head.next[0]; node != nil; node = node.next[0] {

		key := node.key
		keys = append(keys, key)

		value := node.value
		values = append(values, value)

		positions = append(positions, currentPos)

		crc := crc32.ChecksumIEEE(value)
		crc1 := make([]byte, 4, 4)

		binary.LittleEndian.PutUint32(crc1, crc)

		//timeStamp := node.timeStamp
		timeStamp1 := make([]byte, 16, 16)
		//copy(timeStamp1, timeStamp)

		tombstone := uint32(node.status)
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

		fileWriter.Write(crc1)
		fileWriter.Write(timeStamp1)
		fileWriter.WriteByte(uint8(tombstone))
		fileWriter.Write(keySize1)
		fileWriter.Write(valueSize1)
		fileWriter.Write(key1)
		fileWriter.Write(value)

		currentPos += 37 + int(len(key1)) + int(len(value))

	}

	index := CreateIndex(keys, positions, path)
	sstable := SSTable{path: path, index: index}
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
		}
	}
	fileWriter.Write([]byte(keysSum[len(keysSum)-2]))
	fileWriter.Write([]byte(keysSum[len(keysSum)-1]))

	summary := Summary{path: sumPath}
	return &summary
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

	return
}
