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
	for i := 0; i < len(all_words); i++ {
		new_val[strings.ToLower(all_words[i])] += 1
	}
	hashed_words := SumHashs(HashWords(new_val))
	sh.KeyVal[key] = hashed_words
}

func HashWords(words map[string]int) map[int]string {
	hash := make(map[int]string)
	for i := range words {
		hash[words[i]] = ToBinary((GetMD5Hash(i)))
	}
	return hash
}

func SumHashs(hashs map[int]string) []int {
	povratna := make([]int, 256, 256)
	for s, h := range hashs {
		for i := 0; i < len(h); i++ {
			if h[i] == '1' {
				povratna[i] += s
			} else {
				povratna[i] -= s
			}
		}
	}
	for i, val := range povratna {
		if val > 0 {
			povratna[i] = 1
		} else {
			povratna[i] = 0
		}
	}
	return povratna
}

// Hemingovo rastojanje
func (sh *SimHash) Compare(key1 string, key2 string) int {
	words1, ok := sh.KeyVal[key1]
	if !ok {
		return -1
	}
	words2, ok := sh.KeyVal[key2]
	if !ok {
		return -1
	}
	arr := make([]int, 256, 256)

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
