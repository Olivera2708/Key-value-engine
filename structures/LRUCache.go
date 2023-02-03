package structures

import (
	"container/list"
	"strings"
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
	Key  string
	Elem []byte
	Type string
}

func CreateLRU(size int) *LRUCache {
	return &LRUCache{size: size, lista: list.New(), mapa: make(map[string]*list.Element, size)}
}

func (lruc *LRUCache) Found(elem Element) (bool, *list.Element) {
	elem.Key = strings.Split(elem.Key, "-")[0]
	if lruc.mapa[elem.Key] != nil {
		return true, lruc.mapa[elem.Key]
	}
	return false, nil
}

func (lruc *LRUCache) Add(elem Element) {
	elem.Key = strings.Split(elem.Key, "-")[0]
	// if strings.Contains(elem.Key, "-") {
	// 	elem.Type = strings.Split(elem.Key, "-")[1]
	// } else {
	// 	elem.Type = ""
	// }
	found, el := lruc.Found(elem)
	if found {
		lruc.Move(el, elem.Elem)
	} else {
		e := lruc.lista.PushFront(elem)
		lruc.mapa[elem.Key] = e
		if lruc.lista.Len() > lruc.size {
			el = lruc.lista.Back()
			e := el.Value.(Element)
			lruc.Delete(e)
		}
	}
}

func (lruc *LRUCache) Delete(el Element) bool {
	el.Key = strings.Split(el.Key, "-")[0]
	found, elem := lruc.Found(el)
	if found {
		lruc.lista.Remove(elem)
		delete(lruc.mapa, el.Key)
	}
	return found
}

func (lruc *LRUCache) Move(elem *list.Element, val []byte) {
	e := elem.Value.(Element)
	e.Elem = val
	elem.Value = e
	lruc.lista.MoveToFront(elem)
}

// func (lruc *LRUCache) Print(lista *list.List) {
// 	fmt.Println()
// 	test := lista.Front()
// 	for i := 0; i < lista.Len(); i++ {
// 		fmt.Println(test.Value.(Element).Key + ": " + string(test.Value.(Element).Element))
// 		test = test.Next()
// 	}
// }
