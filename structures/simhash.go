package structures

import (
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
)

type SimHash struct {
	KeyVal map[string][]int
}

func CreateSimHash() SimHash {
	return SimHash{KeyVal: make(map[string][]int)}
}

func (sh *SimHash) Add(key string, value string) {
	all_words := strings.Split(value, " ")
	new_val := make(map[string]int)
	//dodelimo tezine recima, broj ponavljanja
	for i := 0; i < len(all_words); i++ {
		new_val[strings.ToLower(all_words[i])] += 1
	}
	hashed_words := HashWords(new_val)
	convert_val := Convert(hashed_words)
	sum_val := SumCol(convert_val)
	sh.KeyVal[key] = sum_val
}

// b-bitni hash za svaki el
func HashWords(words map[string]int) map[int]string {
	hash := make(map[int]string)
	for i := range words {
		hash[words[i]] = ToBinary((GetMD5Hash(i)))
	}
	return hash
}

// ako je 0 onda je -1 u nizu
func Convert(hashs map[int]string) map[int][]int {
	return_val := make(map[int][]int)
	for key, val := range hashs {
		new_arr := make([]int, 256)
		for i := 0; i < len(val); i++ {
			if val[i] == '0' {
				new_arr[i] = -1
			} else {
				new_arr[i] = 1
			}
		}
		return_val[key] = new_arr
	}
	return return_val
}

// sumiramo vrednosti iz kolona, mnozeci tezine
func SumCol(hashs map[int][]int) []int {
	return_val := make([]int, 256)
	for i := 0; i < 256; i++ {
		for key, val := range hashs {
			return_val[i] += key * val[i]
		}
	}

	//konverujemo u 0 i 1
	new_arr := make([]int, 256)
	for i := 0; i < 256; i++ {
		if return_val[i] < 0 {
			new_arr[i] = 0
		} else {
			new_arr[i] = 1
		}
	}
	return new_arr
}

// Hemingovo rastojanje, xor operacija
func (sh *SimHash) Compare(key1 string, key2 string) int {
	words1, ok := sh.KeyVal[key1]
	if !ok {
		return -1
	}
	words2, ok := sh.KeyVal[key2]
	if !ok {
		return -1
	}
	arr := make([]int, 256)

	for i := 0; i < 256; i++ {
		arr[i] = words1[i] ^ words2[i]
	}
	res := 0
	for _, val := range arr {
		if val == 1 {
			res++
		}
	}
	return res
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

func (simH *SimHash) SerializeSimHash() []byte {
	var bytes bytes.Buffer
	enc := gob.NewEncoder(&bytes)
	err := enc.Encode(simH)
	if err != nil {
		log.Fatal("encode error:", err)
	}
	return []byte(bytes.Bytes())
}

func DeserializeSimHash(bt []byte) *SimHash {
	var simH SimHash
	var bytes bytes.Buffer
	bytes.Write(bt)
	dec := gob.NewDecoder(&bytes)
	err := dec.Decode(&simH)
	if err != nil {
		log.Fatal("decode error:", err)
	}
	return &simH
}
