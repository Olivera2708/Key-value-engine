package main

import (
	"math/rand"
)

type SkipList struct {
	maxHeight int
	height    int
	size      int
	head      *SkipListNode
}

type SkipListNode struct {
	key   string
	value []byte
	next  []*SkipListNode
}

func CreateSkipList(maxHeight int) *SkipList {
	return &SkipList{
		maxHeight: maxHeight,
		size:      1,
		height:    1,
		head:      NewNode(maxHeight, "head"),
	}
}

func NewNode(level int, val string) *SkipListNode {
	node := new(SkipListNode)
	node.key = val
	node.value = []byte(val)
	node.next = make([]*SkipListNode, level+1)
	return node
}

func (s *SkipList) roll() int {
	level := 0
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

func (skiplist SkipList) Add(key string) {
	level := skiplist.roll()
	node := NewNode(level, key)

	for i := skiplist.height - 1; i >= 0; i-- {
		current := skiplist.head
		next := current.next[i]
		for next != nil {
			if next == nil || next.key > key {
				break
			}
			current = next
			next = current.next[i]
		}
		if i <= level {
			skiplist.size++
			node.next[i] = next
			current.next[i] = node
		}
	}
}

func (skiplist SkipList) Find(key string) *SkipListNode {
	current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		next := current.next[i]
		for next != nil {
			current = next
			next = current.next[i]
			if current.key == key {
				return current
			}
			if next == nil || current.key > key {
				break
			}
		}
	}
	return nil
}

// Mislim da ne radi
func (skiplist SkipList) Remove(key string) bool {
	current := skiplist.head
	for i := skiplist.height - 1; i >= 0; i-- {
		next := current.next[i]
		for next != nil {
			current = next
			next = current.next[i]
			if next == nil || current.key > key {
				break
			}
			if current.key == key {
				current = current.next[i]
				return true
			}
		}
	}
	return false
}

func main() {
	skiplist := CreateSkipList(5)

	skiplist.Add("test")
	skiplist.Add("pre")
	skiplist.Add("preskoci")
}
