package structures

import (
	"crypto/md5"
	"encoding/binary"
	"math"
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
	hashF []HashWithSeed
}

func CreateBloomFilter(n uint, p float64) *BloomF {
	m := CalculateM(int(n), p)
	k := CalculateK(int(n), m)
	hashF := CreateHashFunctions(k)
	bloomF := BloomF{m, k, p, make([]byte, m), hashF}
	return &bloomF
}

// dodavanje, tako sto upotrebimu svaku hash i postavimo u set na 1
func (bloomF *BloomF) Add(elem string) {
	for _, hashFunc := range bloomF.hashF {
		i := hashFunc.Hash([]byte(elem)) % uint64(bloomF.M)
		bloomF.Set[i] = 1
	}
}

// provera da li je za sve hash za dati string u setu 1
func (bloomF *BloomF) Query(elem string) bool {
	for _, hashFunc := range bloomF.hashF {
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
