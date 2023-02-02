package structures

import (
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"encoding/gob"
	"log"
	"math"
	"os"
	"time"
)

type BloomFilter interface {
	Add()
	Query()
}

type BloomF struct {
	M     uint
	K     uint
	P     float64
	Set   []byte
	HashF []HashWithSeed
}

// n je koliko ce biti elemenata, p tacnost
func CreateBloomFilter(n uint, p float64) *BloomF {
	m := CalculateM(int(n), p)
	k := CalculateK(int(n), m)
	hashF := CreateHashFunctions(k)
	bloomF := BloomF{m, k, p, make([]byte, m), hashF}
	return &bloomF
}

// dodavanje, tako sto upotrebimu svaku hash i postavimo u set na 1
func (bloomF *BloomF) Add(elem string) {
	for _, hashFunc := range bloomF.HashF {
		i := hashFunc.Hash([]byte(elem)) % uint64(bloomF.M)
		bloomF.Set[i] = 1
	}
}

// provera da li je za sve hash za dati string u setu 1
func (bloomF *BloomF) Query(elem string) bool {
	for _, hashFunc := range bloomF.HashF {
		i := hashFunc.Hash([]byte(elem)) % uint64(bloomF.M)
		if bloomF.Set[i] != 1 {
			return false
		}
	}
	return true
}

type HashWithSeed struct {
	Seed []byte
}

func (h HashWithSeed) Hash(data []byte) uint64 {
	fn := md5.New()
	fn.Write(append(data, h.Seed...))
	return binary.BigEndian.Uint64(fn.Sum(nil))
}

func CreateHashFunctions(k uint) []HashWithSeed {
	h := make([]HashWithSeed, k)
	ts := uint(time.Now().Unix())
	for i := uint(0); i < k; i++ {
		seed := make([]byte, 32)
		binary.BigEndian.PutUint32(seed, uint32(ts+i))
		hfn := HashWithSeed{Seed: seed}
		h[i] = hfn
	}
	return h
}

func CalculateM(expectedElements int, falsePositiveRate float64) uint {
	return uint(math.Ceil(float64(expectedElements) * math.Abs(math.Log(falsePositiveRate)) / math.Pow(math.Log(2), float64(2))))
}

func CalculateK(expectedElements int, m uint) uint {
	return uint(math.Ceil((float64(m) / float64(expectedElements)) * math.Log(2)))
}

func (bloomF *BloomF) Write(path string) {
	file, err := os.OpenFile(path+"-filter.db", os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bloomF)
	if err != nil {
		panic(err)
	}
}

func (bf *BloomF) SerializeBloom() []byte {
	var bytes bytes.Buffer
	enc := gob.NewEncoder(&bytes)
	err := enc.Encode(bf)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	return []byte(bytes.Bytes())
}

func DeserializeBloom(bt []byte) *BloomF {
	var bf BloomF
	var bytes bytes.Buffer
	bytes.Write(bt)
	dec := gob.NewDecoder(&bytes)
	err := dec.Decode(&bf)
	if err != nil {
		log.Fatal("decode error:", err)
	}
	return &bf
}

func Read(path string) *BloomF {
	file, err := os.OpenFile(path+"-filter.db", os.O_RDWR, 0666)
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var srs = new(BloomF)
	for {
		err = decoder.Decode(srs)
		if err != nil {
			break
		}
	}
	return srs
}

func (bf *BloomF) WriteGlobal() {
	file, err := os.OpenFile("data/bloomf.db", os.O_CREATE|os.O_WRONLY, 0666)
	defer file.Close()
	if err != nil {
		panic(err)
	}
	encoder := gob.NewEncoder(file)
	err = encoder.Encode(bf)
	if err != nil {
		panic(err)
	}
}

func ReadAll() *BloomF {
	file, err := os.OpenFile("data/bloomf.db", os.O_CREATE|os.O_RDWR, 0666)
	defer file.Close()

	decoder := gob.NewDecoder(file)
	var srs = new(BloomF)
	for {
		err = decoder.Decode(srs)
		if err != nil {
			break
		}
	}

	return srs
}
