package structures

import (
	"Projekat/global"
	"encoding/binary"
	"fmt"
	"strings"
)

type TreeNode struct {
	keys       []string
	vals       [][]byte
	status     []int
	timestamps []uint64
	children   []*TreeNode
	parent     *TreeNode
}

type BTree struct {
	root *TreeNode
}

func CreateTreeNode(parent *TreeNode, key string, val []byte, stat int, timestamp uint64) *TreeNode {
	keys := make([]string, global.BTreeN)
	vals := make([][]byte, global.BTreeN)
	status := make([]int, global.BTreeN)
	timestamps := make([]uint64, global.BTreeN)
	children := make([]*TreeNode, global.BTreeN+1)

	keys[0] = key
	vals[0] = val
	status[0] = stat
	timestamps[0] = timestamp
	return &TreeNode{keys, vals, status, timestamps, children, parent}
}

func CreateBTree(key string, val []byte, status int, timestamp uint64) *BTree {
	root := CreateTreeNode(nil, key, val, status, timestamp)
	return &BTree{root}
}

func (node *TreeNode) isLeaf() bool {
	for i := 0; i < global.BTreeN+1; i++ {
		if node.children[i] != nil {
			return false
		}
	}
	return true
}

func (node *TreeNode) isRoot() bool {
	return node.parent == nil
}

func copy(dest *TreeNode, i int, org *TreeNode, j int) {
	dest.keys[i] = org.keys[j]
	dest.vals[i] = org.vals[j]
	dest.status[i] = org.status[j]
	dest.timestamps[i] = org.timestamps[j]
}

func (node *TreeNode) insertAt(i int, key string, val []byte, stat int, timestamp uint64) {
	node.keys[i] = key
	node.vals[i] = val
	node.status[i] = stat
	node.timestamps[i] = timestamp
}

func (btree *BTree) Found(key string) (bool, *SkipListNode, []byte, string) {
	node := btree.root
	for {
		//uporedi sa node.keys
		for i := 0; i < len(node.keys); i++ {
			if strings.Split(key, "-")[0] == strings.Split(node.keys[i], "-")[0] && node.status[i] == 0 {
				return true, nil, node.vals[i], node.keys[i]
			} else if strings.Split(key, "-")[0] == strings.Split(node.keys[i], "-")[0] && node.status[i] == 1 {
				return true, nil, nil, ""
			} else if strings.Split(key, "-")[0] < strings.Split(node.keys[i], "-")[0] {
				if node.isLeaf() {
					return false, nil, nil, ""
				}
				if node.children[i] == nil {
					return false, nil, nil, ""
				}
				node = node.children[i]
				break
			} else if strings.Split(key, "-")[0] > strings.Split(node.keys[i], "-")[0] && i == len(node.keys)-1 {
				if node.isLeaf() {
					return false, nil, nil, ""
				}
				if node.children[i+1] == nil {
					return false, nil, nil, ""
				}
				node = node.children[i+1]
				break
			}
		}
	}
}

func (btree *BTree) Add(key string, val []byte, stat int, timestamp uint64) bool {

	node := btree.root
	n := len(node.keys)
	for {
		for i := 0; i < n; i++ {
			if strings.Split(key, "-")[0] == strings.Split(node.keys[i], "-")[0] {
				node.vals[i] = val
				node.status[i] = stat
				node.timestamps[i] = timestamp
				//vec postoji, pa se izmeni
				return false

			} else if strings.Split(key, "-")[0] < strings.Split(node.keys[i], "-")[0] {
				if node.isLeaf() {
					if node.keys[n-1] == "" {
						//ima mesta u listu za bar jos jedan element
						for j := 0; j < n; j++ {
							if node.keys[j] == "" {
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								node.timestamps[j] = timestamp
								return true
							}
							if strings.Split(node.keys[j], "-")[0] > strings.Split(key, "-")[0] {
								for k := n - 1; k > j; k-- {
									copy(node, k, node, k-1)
								}
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								node.timestamps[j] = timestamp
								return true
							}

						}
					} else {
						//list je popunjen pre dodavanja
						btree.NodeDivision(key, val, stat, timestamp, node)
						return true
					}

				}
				node = node.children[i]
				break

			} else if (i < n-1 && strings.Split(key, "-")[0] > strings.Split(node.keys[i], "-")[0] && node.keys[i+1] == "") || (i == n-1 && strings.Split(key, "-")[0] > strings.Split(node.keys[i], "-")[0]) {
				if node.isLeaf() {
					if node.keys[n-1] == "" {
						//ima mesta u listu za bar jos jedan element
						for j := 0; j < n; j++ {
							if node.keys[j] == "" {
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								node.timestamps[j] = timestamp
								return true
							}
							if strings.Split(node.keys[j], "-")[0] > strings.Split(key, "-")[0] {
								for k := n - 1; k > j; k-- {
									copy(node, k, node, k-1)
								}
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								node.timestamps[j] = timestamp
								return true
							}

						}
					} else {
						//list je popunjen pre dodavanja => rotacija
						btree.NodeDivision(key, val, stat, timestamp, node)
						return true
					}
				}
				node = node.children[i+1]
				break
			}
		}
	}
}

// raspodeli decu u stari i novi cvor pri podeli cvorova
// keyp = kljuc cvora koji je otisao na visi nivo
func AdoptionService(left, right *TreeNode, keyp string) {
	n := len(left.keys)
	children := make([]*TreeNode, 0)
	children = append(children, left.children...)
	for c := 0; c < n+1; c++ {
		left.children[c] = nil
	}

	for c := 0; c < n+1; c++ {
		if children[c] != nil {
			k := children[c].keys[0] //primer kljuca iz tog deteta
			if k < keyp {
				//ide u levi cvor
				for q := 0; q < n; q++ {
					if k < left.keys[q] || left.keys[q] == "" {
						left.children[q] = children[c]
						children[c].parent = left
						break
					} else if k > left.keys[q] && q == n-1 {
						left.children[q+1] = children[c]
						children[c].parent = left
					}
				}

			} else {
				//ide u desni cvor
				for q := 0; q < n; q++ {
					if k < right.keys[q] || right.keys[q] == "" {
						right.children[q] = children[c]
						children[c].parent = right
						break
					} else if k > right.keys[q] && q == n-1 {
						right.children[q+1] = children[c]
						children[c].parent = right
					}
				}
			}
		}
	}
}

// podela cvorova
func (btree *BTree) NodeDivision(key string, val []byte, stat int, timestamp uint64, node *TreeNode) {

	n := len(node.keys)
	mid := (n + 1) / 2

	all_keys := make([]string, n+1)
	all_vals := make([][]byte, n+1)
	all_stats := make([]int, n+1)
	all_times := make([]uint64, n+1)

	in := false
	j := 0
	for i := 0; i < n+1; i++ {
		if (j == n) || (!in && strings.Split(key, "-")[0] < strings.Split(node.keys[j], "-")[0]) {
			all_keys[i] = key
			all_vals[i] = val
			all_stats[i] = stat
			all_times[i] = timestamp
			in = true
		} else {
			all_keys[i] = node.keys[j]
			all_vals[i] = node.vals[j]
			all_stats[i] = node.status[j]
			all_times[i] = node.timestamps[j]
			j++
		}
	}
	//sortirani podaci koji se dele 3 kljuca: drugi u koren, prvi levo, treci desno

	if node.isRoot() {
		new_root := CreateTreeNode(nil, all_keys[mid], all_vals[mid], all_stats[mid], all_times[mid])

		//novi desni cvor i ubaci kljuceve
		right := CreateTreeNode(new_root, all_keys[mid+1], all_vals[mid+1], all_stats[mid+1], all_times[mid+1])
		j := 1
		for i := mid + 2; i < n+1; i++ {
			right.insertAt(j, all_keys[i], all_vals[i], all_stats[i], all_times[i])
			j++
		}

		node.parent = new_root
		new_root.children[0] = node
		new_root.children[1] = right
		//upise nove kljuceve u stari cvor
		for i := 0; i < mid; i++ {
			node.insertAt(i, all_keys[i], all_vals[i], all_stats[i], all_times[i])
		}
		for i := mid; i < n; i++ {
			node.insertAt(i, "", nil, 0, 0)
		}

		btree.root = new_root

		AdoptionService(node, right, all_keys[mid])

		return
	}
	//ako node nije koren...

	//koliko kljuceva ima node-ov roditelj?
	parent := node.parent
	i := 0
	for ; i < n+1; i++ {
		if node == parent.children[i] {
			break
		}
	}

	if parent.keys[n-1] == "" { //ima mesta u roditelju za jos jedan kljuc i jos jedno dete

		//u parent se ubaci all_keys[mid+1] na odgovarajuce mesto i+1
		for j := n - 1; j > i; j-- {
			copy(parent, j, parent, j-1)
		}
		//deca se pomere koliko i kljucevi i na indeks i+1 se doda novo dete sa kljucem mid+1
		for j := n; j > i+1; j-- {
			parent.children[j] = parent.children[j-1]
		}

		parent.insertAt(i, all_keys[mid], all_vals[mid], all_stats[mid], all_times[mid])
		//i-to dete = manja polovina, dete i+1 veca polovina (novi cvor)

		//novi desni brat sa kojim deli kljuceve i decu
		sibling := CreateTreeNode(parent, all_keys[mid+1], all_vals[mid+1], all_stats[mid+1], all_times[mid+1])
		//popunimo brata drugom polovinom cvorova
		c := 1
		for j := mid + 2; j < n+1; j++ {
			sibling.insertAt(c, all_keys[j], all_vals[j], all_stats[j], all_times[j])
			c++
		}
		parent.children[i+1] = sibling

		//novi kljucevi za node
		for j := 0; j < mid; j++ {
			node.insertAt(j, all_keys[j], all_vals[j], all_stats[j], all_times[j])
		}
		for j := mid; j < n; j++ {
			node.insertAt(j, "", nil, 0, 0)
		}
		//node ostaje i-to dete
		//podeli decu cvora koji se deli na node i sibling <<<< ?
		/*
			c = 0
			for j := mid + 1; j < n+1; j++ {
				if node.children[j] != nil {
					sibling.children[c] = node.children[j]
					sibling.children[c].parent = sibling
					node.children[j] = nil
					c++
				}
			}
		*/
		AdoptionService(node, sibling, all_keys[mid])

	} else { //roditelj je maksimalno popunjen, poziva se i za njega, pa se u povratku namestaju deca dole

		btree.NodeDivision(all_keys[mid], all_vals[mid], all_stats[mid], all_times[mid], parent)

		//ko je roditelj?
		grand := parent.parent
		for c := 0; c < n+1; c++ {
			if grand.children[c] != nil {
				for q := 0; q < n; q++ {
					if grand.children[c].keys[q] == all_keys[mid] {
						parent = grand.children[c]
						c = n + 1
						break
					}
				}
			}
		}
		//ko je brat?
		var sibling *TreeNode = nil
		c := 0
		for ; c < n+1; c++ {
			if parent.children[c] == node {
				sibling = parent.children[c+1]
				break
			}
		}
		if sibling == nil && c < n+1 {
			sibling = CreateTreeNode(parent, "", nil, 0, 0)
			parent.children[c+1] = sibling
		}
		//upise kljuceve u desni cvor
		c = 0
		for j := mid + 1; j < n+1; j++ {
			sibling.insertAt(c, all_keys[j], all_vals[j], all_stats[j], all_times[j])
			c++
		}
		//upise nove kljuceve u levi cvor
		for j := 0; j < mid; j++ {
			node.insertAt(j, all_keys[j], all_vals[j], all_stats[j], all_times[j])
		}
		for j := mid; j < n; j++ {
			node.insertAt(j, "", nil, 0, 0)
		}

		//podeli decu <<<<<<
		/*
			c = 0
			for j := mid + 1; j < n+1; j++ {
				if node.children[j] != nil {
					sibling.children[c] = node.children[j]
					sibling.children[c].parent = sibling
					node.children[j] = nil
					c++
				}
			}
		*/
		AdoptionService(node, sibling, all_keys[mid])

		return

	}
}

func (btree *BTree) InOrder() {
	node := btree.root

	inorder(node, 0)
}

func inorder(node *TreeNode, dubina int) {
	n := len(node.keys)
	for i := 0; i < n; i++ {
		if node.children[i] != nil {
			inorder(node.children[i], dubina+1)
		}
		if node.keys[i] == "" {
			continue
		}
		fmt.Println("(" + node.keys[i] + ", " + fmt.Sprint(dubina) + ")")
	}
	if node.children[n] != nil {
		inorder(node.children[n], dubina+1)
	}
}

func (btree *BTree) GetData() [][][]byte {
	data := make([][][]byte, 0)
	gd(btree.root, &data)
	//fmt.Println(data)
	return data
}

func gd(node *TreeNode, data *[][][]byte) {
	n := len(node.keys)
	for i := 0; i < n; i++ {
		if node.children[i] != nil {
			gd(node.children[i], data)
		}

		if node.keys[i] == "" {
			continue
		}

		//dodaj podatak u data
		rec := make([][]byte, 4)
		rec[0] = []byte(node.keys[i]) //key
		rec[1] = node.vals[i]         //value
		tomb := node.status[i]
		rec[2] = make([]byte, 1) //tombstone
		rec[2][0] = byte(tomb)

		timestamp := node.timestamps[i]
		rec[3] = make([]byte, TIMESTAMP_SIZE) //timestamp
		binary.LittleEndian.PutUint64(rec[3], timestamp)

		*data = append(*data, rec)

	}
	if node.children[n] != nil {
		gd(node.children[n], data)
	}
}

func (btree *BTree) FindAllPrefix(prefix string, j int) string {

	data := btree.GetData()

	for i := j + 1; i < len(data); i++ {
		k := string(data[i][0])

		if strings.HasPrefix(k, prefix) { //data[i][2][0] == 0 &&
			return k
		}
	}
	return ""
}

func (btree *BTree) FindTreeNode(key string) ([]byte, uint64, int) {
	node := btree.root
	for {
		//uporedi sa node.keys
		for i := 0; i < len(node.keys); i++ {
			if strings.Split(key, "-")[0] == strings.Split(node.keys[i], "-")[0] {
				return node.vals[i], node.timestamps[i], node.status[i]
			} else if strings.Split(key, "-")[0] < strings.Split(node.keys[i], "-")[0] {
				if node.isLeaf() {
					return nil, 0, -1
				}
				if node.children[i] == nil {
					return nil, 0, -1
				}
				node = node.children[i]
				break
			} else if strings.Split(key, "-")[0] > strings.Split(node.keys[i], "-")[0] && i == len(node.keys)-1 {
				if node.isLeaf() {
					return nil, 0, -1
				}
				if node.children[i+1] == nil {
					return nil, 0, -1
				}
				node = node.children[i+1]
				break
			}
		}
	}
}

func (btree *BTree) FindAllPrefixRange(min_prefix string, max_prefix string, j int) string {

	data := btree.GetData()

	for i := j + 1; i < len(data); i++ {
		k := string(data[i][0])

		if min_prefix <= strings.Split(k, "-")[0] && max_prefix >= strings.Split(k, "-")[0] { // data[i][2][0] == 0 &&
			return k

		}
	}
	return ""
}
