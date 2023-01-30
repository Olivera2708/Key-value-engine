package structures

type SingleSSTable struct {
	path string
}

// func CreateSingleSSTable(memtable *Memtable, generation int) *SingleSSTable {
// 	path := "data/singlesstables/usertable-" + strconv.FormatInt(int64(generation), 10)
// 	outFile, err := os.Create(path + "-data.db")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer outFile.Close()

// 	fileWriter := bufio.NewWriter(outFile)

// 	currentPos := 0

// 	keys := make([]string, 0)
// 	// values := make([][]byte, 0)
// 	positions := make([]int, 0)

// 	initHeader := make([]byte, 32)
// 	fileWriter.Write(initHeader)

// 	for node := memtable.data.head.next[0]; node != nil; node = node.next[0] {
// 		key := node.key
// 		keys = append(keys, key)

// 		value := node.value
// 		// values = append(values, value)

// 		positions = append(positions, currentPos)

// 		timeStamp := node.timestamp
// 		timeStamp1 := make([]byte, 8, 8)
// 		binary.LittleEndian.PutUint64(timeStamp1, timeStamp)
// 		//copy(timeStamp1, timeStamp)

// 		tombstone := uint8(node.status)
// 		if tombstone > 0 {
// 			tombstone = 1
// 		}

// 		key1 := []byte(key)

// 		keySize := uint64(len(key1))
// 		keySize1 := make([]byte, 8, 8)
// 		binary.LittleEndian.PutUint64(keySize1, keySize)

// 		valueSize := uint64(len(value))
// 		valueSize1 := make([]byte, 8, 8)
// 		binary.LittleEndian.PutUint64(valueSize1, valueSize)

// 		tombstone1 := make([]byte, 1, 1)
// 		tombstone1[0] = tombstone

// 		record := append(timeStamp1, tombstone1...)
// 		record = append(record, keySize1...)
// 		record = append(record, valueSize1...)
// 		record = append(record, key1...)
// 		record = append(record, value...)

// 		crc := crc32.ChecksumIEEE(record)
// 		crc1 := make([]byte, 4, 4)

// 		binary.LittleEndian.PutUint32(crc1, crc)

// 		fileWriter.Write(crc1)
// 		fileWriter.Write(timeStamp1)
// 		fileWriter.WriteByte(uint8(tombstone))
// 		fileWriter.Write(keySize1)
// 		fileWriter.Write(valueSize1)
// 		fileWriter.Write(key1)
// 		fileWriter.Write(value)
// 		fileWriter.Flush()

// 		currentPos += 29 + int(len(key1)) + int(len(value))
// 	}

// 	bf := CreateBloomFilter(uint(len(keys)), 2) //mozda p treba decimalno
// 	for i := 0; i < len(keys); i++ {
// 		bf.Add(keys[i])
// 	}

// 	return &sstable
// }
