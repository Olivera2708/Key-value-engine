package main

import (
	"container/list"
	"fmt"
)

type LRUC interface {
	Add()
	Found()
	Delete()
	Move()
}

type LRUCache struct {
	size  int
	lista *list.List
	mapa  map[string]*list.Element
}

type Element struct {
	key     string
	element []byte
}

func Create(size int) *LRUCache {
	return &LRUCache{size: size, lista: list.New(), mapa: make(map[string]*list.Element, size)}
}

func (lruc *LRUCache) Found(elem Element) (bool, *list.Element) {
	if lruc.mapa[elem.key] != nil {
		return true, lruc.mapa[elem.key]
	}
	return false, nil
}

func (lruc *LRUCache) Add(elem Element) {
	found, el := lruc.Found(elem)
	if found {
		lruc.Move(el, elem.element)
	} else {
		e := lruc.lista.PushFront(elem)
		lruc.mapa[elem.key] = e
		if lruc.lista.Len() > lruc.size {
			el = lruc.lista.Back()
			e := el.Value.(Element)
			lruc.Delete(e)
		}
	}
}

func (lruc *LRUCache) Delete(el Element) {
	found, elem := lruc.Found(el)
	if found {
		lruc.lista.Remove(elem)
		delete(lruc.mapa, el.key)
	}
}

func (lruc *LRUCache) Move(elem *list.Element, val []byte) {
	e := elem.Value.(Element)
	e.element = val
	elem.Value = e
	lruc.lista.MoveToFront(elem)
}

func (lruc *LRUCache) Print(lista *list.List) {
	fmt.Println()
	test := lista.Front()
	for i := 0; i < lista.Len(); i++ {
		fmt.Println(test.Value.(Element).key + ": " + string(test.Value.(Element).element))
		test = test.Next()
	}
}

func main() {
	kes := Create(5)
	kes.Print(kes.lista)
	kes.Add(Element{"1", []byte("proba1")})
	kes.Print(kes.lista)
	kes.Add(Element{"2", []byte("proba2")})
	kes.Print(kes.lista)
	kes.Add(Element{"3", []byte("proba3")})
	kes.Print(kes.lista)
	kes.Add(Element{"1", []byte("proba4")})
	kes.Print(kes.lista)
	kes.Add(Element{"5", []byte("proba5")})
	kes.Print(kes.lista)
	kes.Add(Element{"6", []byte("proba6")})
	kes.Print(kes.lista)
	kes.Add(Element{"7", []byte("proba7")})
	kes.Print(kes.lista)
	kes.Add(Element{"1", []byte("proba1")})
	kes.Print(kes.lista)
	// kes.Delete(Element{"1", []byte("")})
	// kes.Print(kes.lista)
	// kes.Delete(Element{"5", []byte("")})
	// kes.Print(kes.lista)
	// kes.Delete(Element{"3", []byte("")})
	// kes.Print(kes.lista)
	// kes.Delete(Element{"6", []byte("")})
	// kes.Print(kes.lista)
	// kes.Delete(Element{"7", []byte("")})
	// kes.Print(kes.lista)
	// kes.Add(Element{"1", []byte("proba1")})
	// kes.Print(kes.lista)
}
