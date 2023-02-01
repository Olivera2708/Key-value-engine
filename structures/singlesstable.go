package structures

import (
	"bufio"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"os"
	"strconv"
)

type SingleSSTable struct {
	path string
}

func CreateSingleSSTable(data [][][]byte, generation int, summaryBlockingFactor int) *SingleSSTable {
	path := "data/singlesstables/usertable-0-" + strconv.FormatInt(int64(generation), 10)
	outFile, err := os.Create(path + "-data.db")
	if err != nil {
		log.Fatal(err)
	}
	defer outFile.Close()

	fileWriter := bufio.NewWriter(outFile)

	currentPos := 32

	keys := make([]string, 0)
	// values := make([][]byte, 0)
	positions := make([]int, 0)

	initHeader := make([]byte, 32)
	fileWriter.Write(initHeader)
	fileWriter.Flush()

	for i := 0; i < len(data); i++ {
		key := string(data[i][0])
		keys = append(keys, key)

		value := data[i][1]
		// values = append(values, value)

		positions = append(positions, currentPos)

		timeStamp := data[i][3]
		//timeStamp1 := make([]byte, 8, 8)
		//binary.LittleEndian.PutUint64(timeStamp1, timeStamp)

		tombstone := data[i][2]

		key1 := []byte(key)

		keySize := uint64(len(key1))
		keySize1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(keySize1, keySize)

		valueSize := uint64(len(value))
		valueSize1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(valueSize1, valueSize)

		record := append(timeStamp, tombstone...)
		record = append(record, keySize1...)
		record = append(record, valueSize1...)
		record = append(record, key1...)
		record = append(record, value...)

		crc := crc32.ChecksumIEEE(record)
		crc1 := make([]byte, 4, 4)

		binary.LittleEndian.PutUint32(crc1, crc)

		fileWriter.Write(crc1)
		fileWriter.Write(timeStamp)
		fileWriter.Write(tombstone)
		fileWriter.Write(keySize1)
		fileWriter.Write(valueSize1)
		fileWriter.Write(key1)
		fileWriter.Write(value)
		fileWriter.Flush()

		currentPos += 29 + int(len(key1)) + int(len(value))
	}

	outFile.Seek(0, 0)
	lenBytes := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(lenBytes, uint64(len(keys)))
	fileWriter.Write(lenBytes)
	posIndex := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(posIndex, uint64(currentPos))
	fileWriter.Write(posIndex)
	fileWriter.Flush()
	outFile.Seek(int64(currentPos), 0)

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

		currentPos += len([]byte(keys[i])) + 16
	}

	keysSum[len(keys)] = keys[0]
	keysSum[len(keys)+1] = keys[len(keys)-1]

	outFile.Seek(16, 0)
	posSum := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(posSum, uint64(currentPos))
	fileWriter.Write(posSum)
	fileWriter.Flush()
	outFile.Seek(int64(currentPos), 0)

	len1 := make([]byte, 8, 8)
	len2 := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(len1, uint64(len(keysSum[len(keysSum)-2])))
	binary.LittleEndian.PutUint64(len2, uint64(len(keysSum[len(keysSum)-1])))
	fileWriter.Write(len1)
	fileWriter.Write([]byte(keysSum[len(keysSum)-2]))
	fileWriter.Write(len2)
	fileWriter.Write([]byte(keysSum[len(keysSum)-1]))
	currentPos += 16 + len([]byte(keysSum[len(keysSum)-2])) + len([]byte(keysSum[len(keysSum)-1]))

	for i := 0; i < len(positionsSum); i += 1 {
		if i%summaryBlockingFactor == 0 {

			keySize1 := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(keySize1, uint64(keySizesSum[i]))

			key1 := []byte(keysSum[i])

			posSum1 := make([]byte, 8, 8)
			binary.LittleEndian.PutUint64(posSum1, uint64(positionsSum[i]))

			currentPos += 16 + len([]byte(keysSum[i]))
			fileWriter.Write(keySize1)
			fileWriter.Write(key1)
			fileWriter.Write(posSum1)
			fileWriter.Flush()
		}
	}

	outFile.Seek(24, 0)
	posBF := make([]byte, 8, 8)
	binary.LittleEndian.PutUint64(posBF, uint64(currentPos))
	fileWriter.Write(posBF)
	fileWriter.Flush()
	outFile.Seek(int64(currentPos), 0)

	bf := CreateBloomFilter(uint(len(keys)), 2) //mozda p treba decimalno
	for i := 0; i < len(keys); i++ {
		bf.Add(keys[i])
	}

	encoder := gob.NewEncoder(outFile)
	err = encoder.Encode(bf)
	if err != nil {
		panic(err)
	}

	ssst := SingleSSTable{path}
	return &ssst
}

func ReadSingleSummary(path, key string, summaryBlockingFactor int) (bool, []byte) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0666)
	defer file.Close()
	lengthBytes := make([]byte, 8, 8)
	posIndBytes := make([]byte, 8, 8)
	posSumBytes := make([]byte, 8, 8)
	posBFBytes := make([]byte, 8, 8)
	file.Read(lengthBytes)
	file.Read(posIndBytes)
	file.Read(posSumBytes)
	file.Read(posBFBytes)
	length := binary.LittleEndian.Uint64(lengthBytes)
	posInd := binary.LittleEndian.Uint64(posIndBytes)
	posSum := binary.LittleEndian.Uint64(posSumBytes)
	posBF := binary.LittleEndian.Uint64(posBFBytes)
	file.Seek(int64(posBF), 0)

	decoder := gob.NewDecoder(file)
	var srs = new(BloomF)
	for {
		err = decoder.Decode(srs)
		if err != nil {
			break
		}
	}

	isHere := srs.Query(key)
	if !isHere {
		return false, nil
	}

	file.Seek(int64(posSum), 0)

	startLen := make([]byte, 8)
	endLen := make([]byte, 8)

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
		for i := 0; i < int(math.Ceil(float64(length)/float64(summaryBlockingFactor))); i++ {

			//fmt.Print(int(math.Ceil(float64(length) / float64(SUMMARY_BLOCKING_FACTOR))))

			keyLen := make([]byte, 8)
			file.Read(keyLen)
			keyLenNum := binary.LittleEndian.Uint64(keyLen)
			key1 := make([]byte, keyLenNum)
			file.Read(key1)
			if string(key1) > key {
				file.Seek(-(int64(keyLenNum) + 16), 1)
				file.Read(position)
				pos := binary.LittleEndian.Uint64(position)

				found, value := ReadSingleIndex(file, key, pos, posInd)

				return found, value
			} else if string(key1) == key {
				file.Read(position)
				pos := binary.LittleEndian.Uint64(position)

				found, value := ReadSingleIndex(file, key, pos, posInd)

				return found, value
			}
			// file.Seek(8, 1)
			file.Read(position)

			if i == int(math.Ceil(float64(length)/float64(summaryBlockingFactor)))-1 {
				pos := binary.LittleEndian.Uint64(position)
				found, value := ReadSingleIndex(file, key, pos, posInd)
				return found, value
			}
		}
	}
	return false, nil

}

func ReadSingleIndex(file *os.File, key string, position, posInd uint64) (bool, []byte) {
	fmt.Println("Indeks")
	file.Seek(int64(posInd), 0) // ups
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
			value := ReadSingleSSTable(file, key, pos)
			return true, value
		} else if key < string(key1) {
			return false, nil
		}
		file.Seek(8, 1)
	}
	return false, nil

}

func ReadSingleSSTable(file *os.File, key string, pos uint64) []byte {
	fmt.Println("Data")
	file.Seek(int64(pos)+13, 0)
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

func FindAllPrefixSingle(path string, prefix string, summaryBlockingFactor int) []string {
	return_data := []string{}

	return return_data
}

func FindAllPrefixRangeSingle(path string, min_prefix string, max_prefix string, summaryBlockingFactor int) []string {
	return_data := []string{}

	return return_data
}
