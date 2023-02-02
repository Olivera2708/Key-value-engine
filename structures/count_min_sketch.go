package structures

import (
	"bytes"
	"encoding/gob"
	"log"
	"math"
)

type CountMinSketch interface {
	Add()
	Query()
}

type CMS struct {
	M       uint
	K       uint
	Epsilon float64
	Delta   float64
	Set     [][]int
	HashF   []HashWithSeed
}

func CreateCMS(p float64, d float64) *CMS {
	m := CalculateMC(p)
	k := CalculateKC(d)
	hashF := CreateHashFunctions(k)

	set := make([][]int, k)
	for i := range set {
		set[i] = make([]int, m)
	}

	cms := CMS{m, k, p, d, set, hashF}
	return &cms
}

func (cms *CMS) Add(elem string) {
	for i, hashF := range cms.HashF {
		j := hashF.Hash([]byte(elem)) % uint64(cms.M)
		cms.Set[i][j] += 1
	}
}

func (cms *CMS) Query(elem string) int {
	val := make([]int, cms.K)
	for i, hashF := range cms.HashF {
		j := hashF.Hash([]byte(elem)) % uint64(cms.M)
		val[i] = cms.Set[i][j]
	}
	min := val[0]
	for _, v := range val {
		if min > v {
			min = v
		}
	}
	return min
}

func CalculateMC(epsilon float64) uint {
	return uint(math.Ceil(math.E / epsilon))
}

func CalculateKC(delta float64) uint {
	return uint(math.Ceil(math.Log(math.E / delta)))
}

func (cms *CMS) SerializeCMS() []byte {
	var bytes bytes.Buffer
	enc := gob.NewEncoder(&bytes)
	err := enc.Encode(cms)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	return []byte(bytes.Bytes())
}

func DeserializeCMS(bt []byte) *CMS {
	var cms CMS
	var bytes bytes.Buffer
	bytes.Write(bt)
	dec := gob.NewDecoder(&bytes)
	err := dec.Decode(&cms)
	if err != nil {
		log.Fatal("decode error:", err)
	}
	return &cms
}
