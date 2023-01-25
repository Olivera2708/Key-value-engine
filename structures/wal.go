package structures

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"time"
)

/*
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   |    CRC (4B)   | Timestamp (8B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
   +---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
   CRC = 32bit hash computed over the payload using CRC
   Key Size = Length of the Key data
   Tombstone = If this record was deleted and has a value
   Value Size = Length of the Value data
   Key = Key data
   Value = Value data
   Timestamp = Timestamp of the operation in seconds
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
	segments       []fs.FileInfo //lista za fajlove
	low_water_mark int           //koliko segmenata se ne brise
}

// mozda provera da li je direktorijum prazan, da onda napravi prvi segmnet koji ce biti prazan fajl?
func CreateWAL(ss uint, lwm int) *WAL {
	//iscita segmente=fajlove i da ubaci pokazivace u segments
	segments, err := ioutil.ReadDir(DIRNAME)
	if err != nil {
		log.Fatal(err)
	}

	return &WAL{ss, segments, lwm}
}

func IsFull(filename string) uint {
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

// drugi tipovi parametara?
func (wal *WAL) Add(key string, val string) {

	//zapis pravis po formatu gore
	keyb := []byte(key)
	key_len := uint64(len(keyb))
	key_size := make([]byte, KEY_SIZE_SIZE)
	binary.LittleEndian.PutUint64(key_size, key_len)

	valb := []byte(val)
	val_len := uint64(len(valb))
	val_size := make([]byte, VALUE_SIZE_SIZE)
	binary.LittleEndian.PutUint64(val_size, val_len)

	timestamp := []byte(time.Now().String()) // <<<<<------------
	//TREBA DA BUDE TACNO 8 BAJTOVA

	tombstone := make([]byte, 1)
	tombstone[0] = byte(1)

	record := append(timestamp, tombstone...)
	record = append(record, key_size...)
	record = append(record, val_size...)
	record = append(record, keyb...)
	record = append(record, valb...)

	crc := CRC32(record)
	data := make([]byte, CRC_SIZE)
	binary.LittleEndian.PutUint32(data, uint32(crc))
	data = append(data, record...) // kreiran je zapis

	//ucitaj poslednji segment u memoriju (pp: sortirani su)
	n := len(wal.segments)
	s := wal.segments[n-1]

	//proveri da li ima slobodnog mesta
	if wal.segment_size == IsFull(DIRNAME+"/"+s.Name()) {
		rbr, err := strconv.ParseUint(DIRNAME+"/"+s.Name()[4:10], 10, 32) //kako radi sjasing stringa ovde????
		if err != nil {
			log.Fatal(err)
		}
		rbr++
		broj := strconv.FormatUint(rbr, 10)
		for i := len(broj); i < CIFARA_U_NAZIVU; i++ {
			broj = "0" + broj
		}

		new_file_name := "wal_" + broj + ".log"
		s, err := os.OpenFile(DIRNAME+"/"+new_file_name, os.O_CREATE, 0666) //dozvole
		s.Close()
		if err != nil {
			log.Fatal(err)
		}
		ss := make([]fs.FileInfo, 1)
		ss[0], _ = os.Stat(DIRNAME + "/" + s.Name())
		wal.segments = append(wal.segments, ss...)
	}
	n = len(wal.segments)
	s = wal.segments[n-1]

	file, err := os.OpenFile(DIRNAME+"/"+s.Name(), os.O_RDWR, 0666) //dozvole?
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
}

func (wal *WAL) ReadAll() {
	//ucitava redom segmente i ispisuje sadrzaj
	crcb := make([]byte, CRC_SIZE)
	time := make([]byte, TIMESTAMP_SIZE)
	tomb := make([]byte, TOMBSTONE_SIZE)
	ksb := make([]byte, KEY_SIZE_SIZE)
	vsb := make([]byte, VALUE_SIZE_SIZE)

	for _, seg := range wal.segments {
		file, err := os.OpenFile(seg.Name(), os.O_RDONLY, 0666)
		defer file.Close()

		if err != nil {
			log.Fatal(err)
		}

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

			file.Read(time)
			file.Read(tomb)
			file.Read(ksb)
			key_size := binary.LittleEndian.Uint64(ksb)
			file.Read(vsb)
			val_size := binary.LittleEndian.Uint64(vsb)

			key := make([]byte, key_size)
			file.Read(key)
			val := make([]byte, val_size)
			file.Read(val)
			fmt.Println(key)
			fmt.Println(val)
			fmt.Println()
			//dodati sta se radi sa procitanim podacima
			//ispisuje samo kljuc i vrednost radi provere

			//provera crc?
			data := make([]byte, TIMESTAMP_SIZE+TOMBSTONE_SIZE+
				KEY_SIZE_SIZE+VALUE_SIZE_SIZE+key_size+val_size)

			if crc != CRC32(data) {
				fmt.Println("Korumpirani podaci")
			}
		}
	}
}

func (wal *WAL) Flush() {
	n := len(wal.segments)
	if n > wal.low_water_mark {
		new := make([]fs.FileInfo, wal.low_water_mark)

		j := 0
		for i := wal.low_water_mark - 1; i < n; i++ {
			new[j] = wal.segments[i]
			j++
		}

		//obrise suvisne
		for i := 0; i < wal.low_water_mark-1; i++ {
			err := os.Remove(wal.segments[i].Name())
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
			err := os.Rename(new[i].Name(), new_file_name)
			if err != nil {
				log.Fatal(err)
			}
		}
		wal.segments = new
	}
}
