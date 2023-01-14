package main

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
	hashF   []HashWithSeed
}

func Create(p float64, d float64) *CMS {
	m := CalculateM(p)
	k := CalculateK(d)
	hashF := CreateHashFunctions(k)

	set := make([][]int, k)
	for i, _ := range set {
		set[i] = make([]int, m)
	}

	cms := CMS{m, k, p, d, set, hashF}
	return &cms
}

func (cms *CMS) Add(elem string) {
	for i, hashF := range cms.hashF {
		j := hashF.Hash([]byte(elem)) % uint64(cms.M)
		cms.Set[i][j] += 1
	}
}

func (cms *CMS) Query(elem string) int {
	val := make([]int, cms.K)
	for i, hashF := range cms.hashF {
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
