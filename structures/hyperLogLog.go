package structures

import (
	"bytes"
	"encoding/gob"
	"log"
	"math"
)

type HyperLogLog interface {
	Estimate()
	emptyCount()
	Add()
	SerializeHLL()
}

type HLL struct {
	M   uint64
	P   uint8
	Reg []uint8
}

func CreateHLL(p uint8) HLL {
	m := int(math.Pow(2, float64(p)))
	return HLL{uint64(m), p, make([]uint8, m, m)}
}

func (hll *HLL) Add(rec string) {
	hash := ToBinary(GetMD5Hash(rec))
	key := 0
	p := hll.P
	for i := 0; i < int(p); i++ {
		key += int(hash[i]-'0') * int(math.Pow(2, float64(int(p)-i-1)))
	}
	sum := 0
	for i := len(hash) - 1; i > 0; i-- {
		if hash[i] == '0' {
			sum++
		} else {
			break
		}
	}
	hll.Reg[key] = uint8(sum)
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.Reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.M))
	estimation := alpha * math.Pow(float64(hll.M), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.M) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.M) * math.Log(float64(hll.M)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.Reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}

func (hll *HLL) SerializeHLL() []byte {
	var bytes bytes.Buffer
	enc := gob.NewEncoder(&bytes)
	err := enc.Encode(hll)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	return []byte(bytes.Bytes())
}

func DeserializeHLL(bt []byte) *HLL {
	var hll HLL
	var bytes bytes.Buffer
	bytes.Write(bt)
	dec := gob.NewDecoder(&bytes)
	err := dec.Decode(&hll)
	if err != nil {
		log.Fatal("decode error:", err)
	}
	return &hll
}
