package structures

import (
	"encoding/binary"
	"math/rand"
	"strings"
)

type MemtableData interface {
	Add(key string, element []byte, stat int, timestamp uint64) bool
	Found(key string) (bool, *SkipListNode, []byte, string)
	FindAllPrefix(prefix string, j *int) string
	FindAllPrefixRange(min_prefix string, max_prefix string, j *int) string
	GetData() [][][]byte
	FindTreeNode(key string) ([]byte, uint64, int)
}

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	Key       string
	Value     []byte
	Status    int
	Next      []*SkipListNode
	Prev      *SkipListNode
	Timestamp uint64
}

func CreateSkipList(maxHeight, height, size int) *SkipList {
	return &SkipList{maxHeight: maxHeight, height: height, size: size, head: &SkipListNode{Key: "", Value: nil, Status: 0, Next: make([]*SkipListNode, maxHeight), Prev: nil, Timestamp: 0}}
}

func (s *SkipList) Add(key string, element []byte, stat int, timestamp uint64) bool {
	pronadjen, node, _, _ := s.Found(key)
	if pronadjen {
		s.Update(key, element, stat)
		return false
	}
	h := s.height
	r := s.roll()
	if r > h {
		s.height = r
	}
	newNode := SkipListNode{Key: key, Value: element, Status: stat, Next: make([]*SkipListNode, r), Prev: node, Timestamp: timestamp}
	for i := 0; i < r; i++ { //!!!
		for i >= len(node.Next) {
			if node.Prev == nil {
				break
			}
			node = node.Prev
		}
		newNode.Next[i] = node.Next[i]
		node.Next[i] = &newNode
	}
	if newNode.Next[0] != nil {
		newNode.Next[0].Prev = &newNode
	}
	return true
}

func (s *SkipList) Found(key string) (bool, *SkipListNode, []byte, string) {
	pronadjen := false
	node := SkipListNode{s.head.Key, s.head.Value, s.head.Status, s.head.Next, nil, s.head.Timestamp}
	for i := s.height - 1; i >= 0; {
		if node.Next[i] != nil {
			if strings.Split(node.Next[i].Key, "-")[0] < strings.Split(key, "-")[0] {
				node = *node.Next[i]
			} else if strings.Split(node.Next[i].Key, "-")[0] == strings.Split(key, "-")[0] && node.Status == 0 {
				pronadjen = true
				node = *node.Next[i]
				break
			} else {
				i--
			}
		} else {
			i--
		}
	}
	return pronadjen, &node, nil, ""
}

func (s *SkipList) Update(key string, element []byte, stat int) bool {
	node := SkipListNode{s.head.Key, s.head.Value, s.head.Status, s.head.Next, nil, s.head.Timestamp}
	for i := s.height - 1; i >= 0; {
		if node.Next[i] != nil {
			if strings.Split(node.Next[i].Key, "-")[0] < strings.Split(key, "-")[0] {
				node = *node.Next[i]
			} else if strings.Split(node.Next[i].Key, "-")[0] == strings.Split(key, "-")[0] {
				node.Next[i].Key = key
				node.Next[i].Value = element
				node.Next[i].Status = stat
				return true
			} else {
				i--
			}
		} else {
			i--
		}
	}
	return false
}

func (s *SkipList) FindAllPrefix(prefix string, j *int) string {
	node := s.head.Next[0]

	for node != nil {
		if strings.HasPrefix(strings.Split(node.Key, "-")[0], prefix) {
			return node.Key
		}
		node = node.Next[0]
	}
	return ""
}

func (s *SkipList) FindAllPrefixRange(min_prefix string, max_prefix string, j *int) string {
	node := s.head.Next[0]

	for node != nil {
		if min_prefix <= strings.Split(node.Key, "-")[0] && max_prefix >= strings.Split(node.Key, "-")[0] { // node.Status == 0 &&
			return node.Key
		}
		node = node.Next[0]
	}
	return ""
}

func (s *SkipList) roll() int {
	level := 1
	// possible ret values from rand are 0 and 1
	// we stop shen we get a 0
	for ; rand.Int31n(2) == 1; level++ {
		if level >= s.maxHeight {
			if level > s.height {
				s.height = level
			}
			return level
		}
	}
	if level > s.height {
		s.height = level
	}
	return level
}

func (s *SkipList) GetData() [][][]byte { // key, value, tombstone, timestamp
	data := make([][][]byte, 0)
	for node := s.head.Next[0]; node != nil; node = node.Next[0] {
		newRec := make([][]byte, 4)
		newRec[0] = []byte(node.Key)
		newRec[1] = node.Value
		tombstone := node.Status
		tombstone1 := make([]byte, 1, 1)
		tombstone1[0] = byte(tombstone)
		newRec[2] = tombstone1
		timestamp := node.Timestamp
		timestamp1 := make([]byte, 8, 8)
		binary.LittleEndian.PutUint64(timestamp1, uint64(timestamp))
		newRec[3] = timestamp1
		data = append(data, newRec)
	}
	return data
}

func (s *SkipList) FindTreeNode(key string) ([]byte, uint64, int) {
	return nil, 0, -1
}
