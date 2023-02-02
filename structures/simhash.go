package structures

import (
	"bufio"
	"bytes"
	"crypto/md5"
	"encoding/gob"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"strings"
)

func makeStopWords() map[string]bool {
	stopWords := []string{"A", "About", "Actually", "Almost", "Also", "Although", "Always", "Am", "An", "And", "Any", "Are",
		"As", "At", "Be", "Became", "Become", "But", "By", "Can", "Could", "Did", "Do", "Does", "Each", "Either", "Else", "For",
		"From", "Had", "Has", "Have", "Hence", "How", "I", "If", "In", "IS", "IT", "ITS", "JUST", "MAY", "MAYBE", "Me", "Might",
		"Mine", "Must", "My", "Neither", "Nor", "Not", "Of", "Oh", "Ok", "When", "Where", "Whereas", "Wherever", "Whenever",
		"Whether", "Which", "While", "Who", "Whom", "Whoever", "Whose", "Why", "Will", "With", "Within", "Without",
		"Would", "Yes", "Yet", "You", "Your"}
	stopWordsMap := make(map[string]bool)
	for _, word := range stopWords {
		stopWordsMap[strings.ToUpper(word)] = true
	}
	return stopWordsMap
}

type SimHash struct {
	StopWords map[string]bool
}

func CreateSimHash() SimHash {
	stopWords := makeStopWords()
	return SimHash{stopWords}
}

type Text struct {
	niz []int
}

func CreateText(filename string, simhash SimHash) Text {
	words := ParseText(filename, simhash)
	hashovano := HashWords(words)
	niz := SumHashs(hashovano)
	return Text{niz}
}

func ParseText(filename string, simhash SimHash) map[string]int {
	povratna := make(map[string]int)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)
	for scanner.Scan() {
		if simhash.StopWords[scanner.Text()] == false { //ako nije u stopWords
			povratna[strings.ToLower(scanner.Text())] += 1
		}
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return povratna
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

func (*SimHash) Hemingvej(t1 Text, t2 Text) int {
	niz1 := t1.niz
	niz2 := t2.niz
	niz := make([]int, 256, 256)

	for i := 0; i < 256; i++ {
		niz[i] = niz1[i] ^ niz2[i]
	}
	result := 0
	for _, val := range niz {
		if val == 1 {
			result++
		}
	}
	return result
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
