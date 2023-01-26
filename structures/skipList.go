package structures

import (
	"math/rand"
)

type SkipL interface {
	Add()
	Found()
	Delete()
}

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	key    string
	value  []byte
	status int
	next   []*SkipListNode
	prev   *SkipListNode
}

func CreateSkipList(maxHeight, height, size int) *SkipList {
	return &SkipList{maxHeight: maxHeight, height: height, size: size, head: &SkipListNode{key: "", value: nil, status: 0, next: make([]*SkipListNode, maxHeight), prev: nil}}
}

func (s *SkipList) Add(key string, element []byte, stat int) {
	pronadjen, node := s.Found(key)
	if pronadjen {
		return
	}
	h := s.height
	r := s.roll()
	if r > h {
		s.height = r
	}
	newNode := SkipListNode{key: key, value: element, status: stat, next: make([]*SkipListNode, r), prev: node}
	for i := 0; i < r; i++ {
		for i > len(node.next) {
			if node.prev == nil {
				break
			}
			node = node.prev
		}
		newNode.next[i] = node.next[i]
		node.next[i] = &newNode
	}
	if newNode.next[0] != nil {
		newNode.next[0].prev = &newNode
	}
}

func (s *SkipList) Delete(key string) bool {
	pronadjen, node := s.Found(key)
	if !pronadjen || key == "" {
		return false
	}
	bef := node.prev
	for i := 0; i < len(node.next); {
		for i >= len(bef.next) {
			if bef.prev == nil {
				break
			}
			bef = bef.prev
		}
		if bef.next[i] == nil {
			bef = bef.prev
		} else {
			bef.next[i] = node.next[i]
			i++
		}
	}
	for i := s.height; i > 0; i-- {
		if s.head.next[i] == nil {
			s.height--
		} else {
			break
		}
	}
	return true
}

func (s *SkipList) Found(key string) (bool, *SkipListNode) {
	pronadjen := false
	node := SkipListNode{s.head.key, s.head.value, s.head.status, s.head.next, nil}
	for i := s.height - 1; i >= 0; { //stavili smo -1
		if node.next[i] != nil {
			if node.next[i].key < key {
				node = *node.next[i]
			} else if node.next[i].key == key && node.status == 0 {
				pronadjen = true
				node = *node.next[i]
				break
			} else {
				i--
			}
		} else {
			i--
		}
	}
	return pronadjen, &node
}

func (s *SkipList) Update(key string, element []byte, stat int) bool {
	node := SkipListNode{s.head.key, s.head.value, s.head.status, s.head.next, nil}
	for i := s.height; i >= 0; {
		if node.next[i] != nil {
			if node.next[i].key < key {
				node = *node.next[i]
			} else if node.next[i].key == key {
				node.next[i].value = element
				node.next[i].status = stat
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