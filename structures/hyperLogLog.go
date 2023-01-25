package structures

import (
	"math"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HyperLogLog interface {
	Estimate()
	emptyCount()
	Add()
}

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

func CreateHLL(p uint8) HLL {
	m := int(math.Pow(2, float64(p)))
	return HLL{uint64(m), p, make([]uint8, m, m)}
}

func (hll *HLL) Add(rec string) {
	hash := ToBinary(GetMD5Hash(rec))
	key := 0
	p := hll.p
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
	if hll.reg[key] < uint8(sum) {
		hll.reg[key] = uint8(sum)
	}
}

// func GetMD5Hash(text string) string {
// 	hash := md5.Sum([]byte(text))
// 	return hex.EncodeToString(hash[:])
// }

// func ToBinary(s string) string {
// 	res := ""
// 	for _, c := range s {
// 		res = fmt.Sprintf("%s%.8b", res, c)
// 	}
// 	return res
// }

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) {
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) {
		estimation = -math.Pow(2.0, 32.0) * math.Log(1.0-estimation/math.Pow(2.0, 32.0))
	}
	return estimation
}

func (hll *HLL) emptyCount() int {
	sum := 0
	for _, val := range hll.reg {
		if val == 0 {
			sum++
		}
	}
	return sum
}
