package structures

import (
	"bufio"
	"encoding/binary"
	"hash/crc32"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

type SSTable struct {
	path  string
	index *Index
}

func CreateSSTable(data [][][]byte, generation int, summaryBlockingFactor int) *SSTable {
	path := "data/sstables/usertable-0-" + strconv.FormatInt(int64(generation), 10)

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

	for i := 0; i < len(data); i++ {
		key := string(data[i][0])
		keys = append(keys, key)

		value := data[i][1]
		values = append(values, value)

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

	bf := CreateBloomFilter(uint(len(keys)), 0.1) //mozda p treba decimalno
	for i := 0; i < len(keys); i++ {
		bf.Add(strings.Split(keys[i], "-")[0])
	}

	bf.Write(path)
	index := CreateIndex(keys, positions, path, summaryBlockingFactor)
	sstable := SSTable{path: path, index: index}
	CreateTOC(path)

	merkle := CreateMerkleTree(values)
	WriteMerkleInFile(merkle, path)

	return &sstable
}

type Index struct {
	path    string
	summary *Summary
}

func CreateIndex(keys []string, positions []int, path string, summaryBlockingFactor int) *Index {
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

		currentPos += len([]byte(keys[i])) + 16
	}
	keysSum[len(keys)] = keys[0]
	keysSum[len(keys)+1] = keys[len(keys)-1]

	summary := CreateSummary(keySizesSum, keysSum, positionsSum, path, summaryBlockingFactor)

	index := Index{path: indexPath, summary: summary}
	return &index
}

type Summary struct {
	path string
}

func CreateSummary(keySizesSum []int, keysSum []string, positionsSum []int, path string, summaryBlockingFactor int) *Summary {
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
		if i%summaryBlockingFactor == 0 {

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

func CheckBloomF(path string, key string) bool {
	bf := Read(path)
	return bf.Query(key)
}

func ReadSummary(path string, key string) (bool, []byte, string) {
	if !CheckBloomF(path, key) {
		return false, nil, ""
	}

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
	if strings.Split(key, "-")[0] >= strings.Split(string(startIndex), "-")[0] && strings.Split(key, "-")[0] <= strings.Split(string(endIndex), "-")[0] {
		position := make([]byte, 8)
		for true {
			keyLen := make([]byte, 8)
			_, err = file.Read(keyLen)
			if err == io.EOF {
				pos := binary.LittleEndian.Uint64(position)
				found, value, new_key := ReadIndex(path, key, pos)
				return found, value, new_key
			}
			keyLenNum := binary.LittleEndian.Uint64(keyLen)
			key1 := make([]byte, keyLenNum)
			file.Read(key1)
			if strings.Split(string(key1), "-")[0] > strings.Split(key, "-")[0] {
				file.Seek(-(int64(keyLenNum) + 16), 1)
				file.Read(position)
				pos := binary.LittleEndian.Uint64(position)
				found, value, new_key := ReadIndex(path, key, pos)
				return found, value, new_key
			} else if strings.Split(string(key1), "-")[0] == strings.Split(key, "-")[0] {
				file.Read(position)
				pos := binary.LittleEndian.Uint64(position)
				found, value, new_key := ReadIndex(path, key, pos)
				return found, value, new_key
			}
			// file.Seek(8, 1)
			file.Read(position)
		}
	}
	return false, nil, ""
}

func ReadIndex(path string, key string, position uint64) (bool, []byte, string) {
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
		if strings.Split(key, "-")[0] == strings.Split(string(key1), "-")[0] {
			file.Read(position1)
			pos := binary.LittleEndian.Uint64(position1)
			value := ReadSSTable(path, string(key1), pos)
			return true, value, string(key1)
		} else if strings.Split(key, "-")[0] < strings.Split(string(key1), "-")[0] {
			return false, nil, ""
		}
		file.Seek(8, 1)
	}
	return false, nil, ""
}

func ReadSSTable(path, key string, position uint64) []byte {
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

func CreateTOC(path string) {
	inFile, err := os.Create(path + "-TOC.txt")
	if err != nil {
		panic(err)
	}
	defer inFile.Close()

	fileWriter := bufio.NewWriter(inFile)

	fileWriter.WriteString(path + "\n")
	fileWriter.WriteString(path + "-data.db\n")
	fileWriter.WriteString(path + "-index.db\n")
	fileWriter.WriteString(path + "-summary.db\n")
	fileWriter.WriteString(path + "-filter.db\n")
	fileWriter.Flush()

	return
}

func ReadNextRecord(file *os.File) (map[string][]byte, bool) {
	crcb := make([]byte, CRC_SIZE)
	timestamp := make([]byte, TIMESTAMP_SIZE)
	tomb := make([]byte, TOMBSTONE_SIZE)
	ksb := make([]byte, KEY_SIZE_SIZE)
	vsb := make([]byte, VALUE_SIZE_SIZE)

	_, err := file.Read(crcb)
	if err == io.EOF {
		return nil, true
	}
	file.Read(timestamp)
	file.Read(tomb)
	file.Read(ksb)
	key_size := binary.LittleEndian.Uint64(ksb)
	file.Read(vsb)
	val_size := binary.LittleEndian.Uint64(vsb)

	key := make([]byte, key_size)
	file.Read(key)
	val := make([]byte, val_size)
	file.Read(val)

	data := make(map[string][]byte)
	data["crc"] = crcb
	data["timestamp"] = timestamp
	data["tombstone"] = tomb
	data["key_size"] = ksb
	data["key"] = key
	data["val_size"] = vsb
	data["value"] = val

	return data, false
}

func FindAllPrefixMultiple(path string, key string) (string, uint64) {
	return FindPrefixSummaryMultiple(path, key)
}

func FindPrefixSummaryMultiple(path string, key string) (string, uint64) {
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

	if (strings.Split(key, "-")[0] >= string(startIndex) || strings.HasPrefix(string(startIndex), key)) && strings.Split(key, "-")[0] <= string(endIndex) {
		position := make([]byte, 8)
		keyLen := make([]byte, 8)
		_, err = file.Read(keyLen)
		keyLenNum := binary.LittleEndian.Uint64(keyLen)
		key1 := make([]byte, keyLenNum)
		file.Read(key1)
		file.Read(position)
		pos := binary.LittleEndian.Uint64(position)
		returnFile, returnPos := FindPrefixIndexMultiple(path, key, pos)
		return returnFile, returnPos
	}
	return "", 0
}

func FindPrefixIndexMultiple(path string, key string, position uint64) (string, uint64) {
	file, err := os.OpenFile(path+"-index.db", os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(position), 0)
	position1 := make([]byte, 8)
	for true {
		keyLen := make([]byte, 8)
		_, err = file.Read(keyLen)
		if err != nil {
			break
		}
		keyLenNum := binary.LittleEndian.Uint64(keyLen)
		key1 := make([]byte, keyLenNum)
		file.Read(key1)
		file.Read(position1)
		pos := binary.LittleEndian.Uint64(position1)

		if strings.HasPrefix(string(key1), key) {
			return path, pos
		} else if strings.Split(string(key1), "-")[0] > strings.Split(key, "-")[0] {
			break
		}
	}
	return "", 0
}

func FindPrefixSSTableMultiple(key string, position uint64, file *os.File) (string, []byte, []byte, int) {
	file.Seek(int64(position), 0)
	file.Seek(4, 1)
	timestamp := make([]byte, TIMESTAMP_SIZE)
	file.Read(timestamp)
	tombstone := make([]byte, TOMBSTONE_SIZE)
	file.Read(tombstone)
	keyLen := make([]byte, 8, 8)
	_, err := file.Read(keyLen)
	if err != nil {
		return "", []byte(""), []byte(""), -1
	}
	keyLenNum := binary.LittleEndian.Uint64(keyLen)
	valLen := make([]byte, 8, 8)
	file.Read(valLen)
	valLenNum := binary.LittleEndian.Uint64(valLen)
	key1 := make([]byte, keyLenNum, keyLenNum)
	file.Read(key1)
	value := make([]byte, valLenNum, valLenNum)
	file.Read(value)
	if strings.HasPrefix(string(key1), key) {
		return string(key1), value, timestamp, int(tombstone[0])
	} else if strings.Split(string(key1), "-")[0] > strings.Split(key, "-")[0] {
		return "", []byte(""), []byte(""), -1
	}
	return "", []byte(""), []byte(""), -1
}

func FindAllPrefixRangeMultiple(path string, min_prefix string, max_prefix string) (string, uint64) {
	return FindPrefixSummaryRangeMultiple(path, min_prefix, max_prefix)
}

func FindPrefixSummaryRangeMultiple(path string, min_prefix string, max_prefix string) (string, uint64) {
	// return_data := [][]byte{}
	// all_keys := []string{}

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

	if (min_prefix <= strings.Split(string(startIndex), "-")[0] && strings.Split(string(startIndex), "-")[0] <= max_prefix) || (min_prefix <= strings.Split(string(endIndex), "-")[0] && strings.Split(string(endIndex), "-")[0] <= max_prefix) || (min_prefix <= strings.Split(string(endIndex), "-")[0] && strings.Split(string(startIndex), "-")[0] <= max_prefix) {
		position := make([]byte, 8)
		keyLen := make([]byte, 8)
		_, err = file.Read(keyLen)
		keyLenNum := binary.LittleEndian.Uint64(keyLen)
		key1 := make([]byte, keyLenNum)
		file.Read(key1)
		file.Read(position)
		pos := binary.LittleEndian.Uint64(position)
		path1, pos1 := FindPrefixIndexRangeMultiple(path, min_prefix, max_prefix, pos)
		return path1, pos1
		// return_data = append(return_data, values...)
		// all_keys = append(all_keys, keys...)
	}
	return "", 0
}

func FindPrefixIndexRangeMultiple(path string, min_prefix string, max_prefix string, position uint64) (string, uint64) {
	// return_data := [][]byte{}
	// all_keys := []string{}
	file, err := os.OpenFile(path+"-index.db", os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	file.Seek(int64(position), 0)
	position1 := make([]byte, 8)
	for true {
		keyLen := make([]byte, 8)
		_, err = file.Read(keyLen)
		if err != nil {
			break
		}
		keyLenNum := binary.LittleEndian.Uint64(keyLen)
		key1 := make([]byte, keyLenNum)
		file.Read(key1)
		if strings.Split(string(key1), "-")[0] <= max_prefix && strings.Split(string(key1), "-")[0] >= min_prefix {
			file.Read(position1)
			pos := binary.LittleEndian.Uint64(position1)
			return path, pos

		} else if string(key1) > max_prefix {
			break
		}
		file.Seek(8, 1)
	}
	return "", 0
}

func FindPrefixSSTableRangeMultiple(min_prefix string, max_prefix string, position uint64, file *os.File) (string, []byte, []byte, int) {
	file.Seek(int64(position), 0)
	for {
		file.Seek(4, 1)
		timestamp := make([]byte, TIMESTAMP_SIZE)
		file.Read(timestamp)
		tombstone := make([]byte, TOMBSTONE_SIZE)
		file.Read(tombstone)
		keyLen := make([]byte, 8, 8)
		_, err := file.Read(keyLen)
		if err != nil {
			break
		}
		keyLenNum := binary.LittleEndian.Uint64(keyLen)
		valLen := make([]byte, 8, 8)
		file.Read(valLen)
		valLenNum := binary.LittleEndian.Uint64(valLen)
		key1 := make([]byte, keyLenNum, keyLenNum)
		file.Read(key1)
		value := make([]byte, valLenNum, valLenNum)
		file.Read(value)
		if strings.Split(string(key1), "-")[0] <= max_prefix && strings.Split(string(key1), "-")[0] >= min_prefix {
			return string(key1), value, timestamp, int(tombstone[0])
			// all_keys = append(all_keys, string(key1))
			// all_data = append(all_data, value)
		} else if strings.Split(string(key1), "-")[0] > max_prefix {
			break
		}
	}
	return "", []byte(""), []byte(""), -1
}
