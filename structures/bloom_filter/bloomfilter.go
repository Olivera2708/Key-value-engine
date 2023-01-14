package main

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

func Create(n uint, p float64) *BloomF {
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
