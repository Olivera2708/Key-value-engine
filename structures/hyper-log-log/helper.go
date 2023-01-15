package structures

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"math"
)

const (
	HLL_MIN_PRECISION = 4
	HLL_MAX_PRECISION = 16
)

type HyperLogLog interface {
	emptyCount()
	Add()
	Estimate()
}

type HLL struct {
	m   uint64
	p   uint8
	reg []uint8
}

func Create(p uint8) HLL {
	m := int(math.Pow(2, float64(p)))
	return HLL{uint64(m), p, make([]uint8, m, m)}
}

func (hll *HLL) Add(word string) {
	binary := ToBinary(GetMD5Hash(word))
	key := 0
	for i := 0; i < int(hll.p); i++ {
		key += (int(binary[i]) - '0') * int(math.Pow(2, float64(int(hll.p)-i))) //racunanje prvih p u int
	}
	val := 0
	for i := len(binary) - 1; i > 0; i-- {
		if binary[i] == '0' { //broj poslednjih 0
			val++
		} else {
			break
		}
	}
	hll.reg[key] = uint8(val)
}

func GetMD5Hash(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func ToBinary(s string) string {
	res := ""
	for _, c := range s {
		res = fmt.Sprintf("%s%.8b", res, c)
	}
	return res
}

func (hll *HLL) Estimate() float64 {
	sum := 0.0
	for _, val := range hll.reg {
		sum += math.Pow(math.Pow(2.0, float64(val)), -1)
	}

	alpha := 0.7213 / (1.0 + 1.079/float64(hll.m))
	estimation := alpha * math.Pow(float64(hll.m), 2.0) / sum
	emptyRegs := hll.emptyCount()
	if estimation <= 2.5*float64(hll.m) { // do small range correction
		if emptyRegs > 0 {
			estimation = float64(hll.m) * math.Log(float64(hll.m)/float64(emptyRegs))
		}
	} else if estimation > 1/30.0*math.Pow(2.0, 32.0) { // do large range correction
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

func main() {
	hll := Create(6)
	hll.Add("Kompjuter")
	hll.Add("Laptop")
	hll.Add("Mis")
	hll.Add("Tastatura")
	hll.Add("Laptop")
	hll.Add("Mis")
	hll.Add("Kompjuter")
	hll.Add("Laptop")
	hll.Add("Laptop")
	hll.Add("Tastatura")
	hll.Add("Laptop")
	hll.Add("Slusalice")
	fmt.Println(hll.Estimate())
}
