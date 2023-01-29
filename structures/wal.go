package structures

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"log"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
*/

const (
	DIRNAME         = "data/wal"
	CIFARA_U_NAZIVU = 5 //kod imenovanja segmenata

	CRC_SIZE        = 4
	TIMESTAMP_SIZE  = 8
	TOMBSTONE_SIZE  = 1
	KEY_SIZE_SIZE   = 8
	VALUE_SIZE_SIZE = 8

	CRC_START        = 0
	TIMESTAMP_START  = CRC_START + CRC_SIZE
	TOMBSTONE_START  = TIMESTAMP_START + TIMESTAMP_SIZE
	KEY_SIZE_START   = TOMBSTONE_START + TOMBSTONE_SIZE
	VALUE_SIZE_START = KEY_SIZE_START + KEY_SIZE_SIZE
	KEY_START        = VALUE_SIZE_START + VALUE_SIZE_SIZE
)

func CRC32(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

type WAL struct {
	segment_size   uint          //maksimalna velicina jednog segmenta
	segments       []fs.DirEntry //lista za fajlove
	low_water_mark int           //koliko segmenata se ne brise
}

func CreateWAL(ss uint, lwm int) *WAL {

	segments, err := os.ReadDir(DIRNAME)
	if len(segments) == 0 {

		//OVO NE RADI ZA PRAZAN DIR <<<<

		file, err := os.OpenFile(DIRNAME+"/wal_00001.log", os.O_CREATE, 0666)
		if err != nil {
			log.Fatal(err)
		}
		segments, err = os.ReadDir(DIRNAME)
		if err != nil {
			log.Fatal(err)
		}
		file.Close()
	} else if err != nil {
		log.Fatal(err)
	}

	return &WAL{ss, segments, lwm}
}

func NumberOfRecords(filename string) uint {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var k uint = 0
	jos := true
	ksb := make([]byte, KEY_SIZE_SIZE)
	vsb := make([]byte, VALUE_SIZE_SIZE)
	pom := make([]byte, 1)
	for jos {
		//provera da li je dosao do eof
		_, err_kraj := file.Read(pom)
		if err_kraj == io.EOF {
			jos = false
			break
		} else if err_kraj != nil {
			log.Fatal(err_kraj)
		}

		file.Seek(KEY_SIZE_START-1, 1)
		file.Read(ksb)
		key_size := binary.LittleEndian.Uint64(ksb)

		file.Read(vsb)
		val_size := binary.LittleEndian.Uint64(vsb)

		file.Seek(int64(key_size)+int64(val_size), 1)

		k++
	}
	return k
}

func (wal *WAL) Add(key string, valb []byte, flag byte) uint64 {

	//zapis pravis po formatu gore
	keyb := []byte(key)
	key_len := uint64(len(keyb))
	key_size := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(key_size, key_len)

	val_len := uint64(len(valb))
	val_size := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(val_size, val_len)

	//time := time.Now().Format("DD-MM-YYYY HH:mm")
	//timestamp := []byte(time)
	t := time.Now().Unix()
	timestamp := make([]byte, 8)
	binary.LittleEndian.PutUint64(timestamp, uint64(t))

	tombstone := make([]byte, 1)
	tombstone[0] = flag

	record := append(timestamp, tombstone...)
	record = append(record, key_size...)
	record = append(record, val_size...)
	record = append(record, keyb...)
	record = append(record, valb...)

	crc := CRC32(record)
	// fmt.Println(crc)
	data := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(data, uint32(crc))
	data = append(data, record...) // kreiran je zapis

	//ucitaj poslednji segment u memoriju (pp: sortirani su)
	n := len(wal.segments)
	s := wal.segments[n-1]

	//proveri da li ima slobodnog mesta
	if wal.segment_size == NumberOfRecords(DIRNAME+"/"+s.Name()) {
		rbr, err := strconv.ParseUint(s.Name()[4:9], 10, 32)
		if err != nil {
			log.Fatal(err)
		}
		rbr++
		broj := strconv.FormatUint(rbr, 10)
		for i := len(broj); i < CIFARA_U_NAZIVU; i++ {
			broj = "0" + broj
		}

		new_file_name := "wal_" + broj + ".log"
		s, err := os.OpenFile(DIRNAME+"/"+new_file_name, os.O_CREATE, 0666)
		s.Close()
		if err != nil {
			log.Fatal(err)
		}
		wal.segments, err = os.ReadDir(DIRNAME)
		if err != nil {
			log.Fatal(err)
		}
	}
	n = len(wal.segments)
	s = wal.segments[n-1]

	file, err := os.OpenFile(DIRNAME+"/"+s.Name(), os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()
	//dodas zapis na kraj poslednjeg segmenta i upises promene u fajlu
	file.Seek(0, 2)
	_, err = file.Write(data)
	if err != nil {
		log.Fatal(err)
	}
	return uint64(t)
}

func (wal *WAL) ReadAll(mem Memtable) {
	//ucitava redom segmente i ispisuje sadrzaj
	crcb := make([]byte, CRC_SIZE)
	time2 := make([]byte, TIMESTAMP_SIZE)
	tomb := make([]byte, TOMBSTONE_SIZE)
	ksb := make([]byte, KEY_SIZE_SIZE)
	vsb := make([]byte, VALUE_SIZE_SIZE)

	for _, seg := range wal.segments {
		file, err := os.OpenFile(DIRNAME+"/"+seg.Name(), os.O_RDONLY, 0666)

		if err != nil {
			log.Fatal(err)
		}
		defer file.Close()

		jos := true

		for jos {
			//provera da li je dosao do eof
			_, err_kraj := file.Read(crcb)
			if err_kraj == io.EOF {
				jos = false
				break
			} else if err_kraj != nil {
				log.Fatal(err_kraj)
			}
			crc := binary.LittleEndian.Uint32(crcb)
			// fmt.Println("CRC: ", crc)
			file.Read(time2)
			time1 := binary.LittleEndian.Uint64(time2)
			// fmt.Println(time.Unix(int64(time1), 0))
			file.Read(tomb)
			// fmt.Println(tomb)
			file.Read(ksb)
			key_size := binary.LittleEndian.Uint64(ksb)
			// fmt.Println(key_size)
			file.Read(vsb)
			val_size := binary.LittleEndian.Uint64(vsb)
			// fmt.Println(val_size)

			key := make([]byte, key_size)
			file.Read(key)
			val := make([]byte, val_size)
			file.Read(val)

			//
			// fmt.Println("Key: ", string(key))
			// fmt.Println("Val: ", string(val))
			// fmt.Println()
			//dodati sta se radi sa procitanim podacima
			//ispisuje samo kljuc i vrednost radi provere

			mem.Add(string(key), val, int(tomb[0]), time1)
			//provera crc?
			// data := make([]byte, TIMESTAMP_SIZE+TOMBSTONE_SIZE+
			// 	KEY_SIZE_SIZE+VALUE_SIZE_SIZE+key_size+val_size)

			data := append(time2, tomb...)
			data = append(data, ksb...)
			data = append(data, vsb...)
			data = append(data, key...)
			data = append(data, val...)

			// fmt.Println(crc)
			// fmt.Println(CRC32(data))
			if crc != CRC32(data) {
				fmt.Println("Korumpirani podaci")
			}
		}
	}
}

func (wal *WAL) Flush() {
	n := len(wal.segments)
	if n > wal.low_water_mark {
		new := make([]fs.DirEntry, wal.low_water_mark)

		j := 0
		for i := n - wal.low_water_mark; i < n; i++ {
			new[j] = wal.segments[i]
			j++
		}

		//obrise suvisne
		for i := 0; i < n-wal.low_water_mark; i++ {
			err := os.Remove(DIRNAME + "/" + wal.segments[i].Name())
			if err != nil {
				log.Fatal(err)
			}
		}

		//preimenuje preostale
		for i := 0; i < wal.low_water_mark; i++ {
			broj := strconv.FormatUint(uint64(i)+1, 10)
			for i := len(broj); i < CIFARA_U_NAZIVU; i++ {
				broj = "0" + broj
			}

			new_file_name := "wal_" + broj + ".log"
			err := os.Rename(DIRNAME+"/"+new[i].Name(), DIRNAME+"/"+new_file_name)
			if err != nil {
				log.Fatal(err)
			}
		}
		wal.segments = new
	}
}
