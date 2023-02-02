package structures

import "strings"

type TreeNode struct {
	keys     []string
	vals     [][]byte
	status   []int
	children []*TreeNode
	parent   *TreeNode
}

type BTree struct {
	root *TreeNode
}

func CreateTreeNode(parent *TreeNode, key string, val []byte, stat int) *TreeNode {
	keys := make([]string, 2)
	vals := make([][]byte, 2)
	status := make([]int, 2)
	children := make([]*TreeNode, 3)

	keys[0] = key
	vals[0] = val
	status[0] = stat
	children[0] = nil
	children[1] = nil
	children[2] = nil
	return &TreeNode{keys, vals, status, children, parent}
}

func CreateBTree(key string, val []byte, status int) *BTree {
	root := CreateTreeNode(nil, key, val, status)
	return &BTree{root}
}

func (node *TreeNode) isLeaf() bool {
	return node.children[0] == nil && node.children[1] == nil && node.children[2] == nil
}

func (node *TreeNode) isRoot() bool {
	return node.parent == nil
}

func copy(dest *TreeNode, i int, org *TreeNode, j int) {
	dest.keys[i] = org.keys[j]
	dest.vals[i] = org.vals[j]
	dest.status[i] = org.status[j]
}

func (node *TreeNode) insertAt(i int, key string, val []byte, stat int) {
	node.keys[i] = key
	node.vals[i] = val
	node.status[i] = stat
}

func (btree *BTree) Found(key string) (bool, []byte) {
	node := btree.root
	for {
		//uporedi sa node.keys
		for i := 0; i < len(node.keys); i++ {
			// if key == node.keys[i] && node.status[i] == 0 {
			if strings.Split(key, "-")[0] == strings.Split(node.keys[i], "-")[0] && node.status[i] == 0 {
				return true, node.vals[i]
			} else if key < node.keys[i] {
				if node.isLeaf() {
					return false, nil
				}
				node = node.children[i]
				break
			} else if key > node.keys[i] && i == len(node.keys)-1 {
				if node.isLeaf() {
					return false, nil
				}
				node = node.children[i+1]
				break
			}
		}
	}
}

func (btree *BTree) Update(key string, val []byte, stat int) bool {
	node := btree.root
	for {
		for i := 0; i < len(node.keys); i++ {
			if key == node.keys[i] && node.status[i] == 0 {
				node.vals[i] = val
				node.status[i] = stat

				return true

			} else if key < node.keys[i] {
				if node.isLeaf() {
					return false
				}
				node = node.children[i]
				break
			} else if key > node.keys[i] && i == len(node.keys)-1 {
				if node.isLeaf() {
					return false
				}
				node = node.children[i+1]
				break
			}
		}
	}
}

func (btree *BTree) Add(key string, val []byte, stat int) bool {

	node := btree.root
	n := len(node.keys)
	for {
		for i := 0; i < n; i++ {
			if key == node.keys[i] && node.status[i] == 0 {
				node.vals[i] = val
				node.status[i] = stat
				//vec postoji, pa se izmeni
				return false

			} else if key < node.keys[i] {
				if node.isLeaf() {
					if node.keys[n-1] == "" {
						//ima mesta u listu za bar jos jedan element
						for j := 0; j < n; j++ {
							if node.keys[j] == "" {
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								return true
							}
							if node.keys[j] > key {
								for k := n - 1; k > j; k-- {
									/*
										node.keys[k] = node.keys[k-1]
										node.vals[k] = node.vals[k-1]
										node.status[k] = node.status[k-1]
									*/
									copy(node, k, node, k-1)
								}
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								return true
							}

						}
					} else {
						//list je popunjen pre dodavanja
						if btree.Rotate(key, val, stat, node) {
							return true
						}
						for btree.NodeDivision(&key, &val, &stat, node) {
							if btree.Rotate(key, val, stat, node) {
								return true
							}
						}
					}

				}
				node = node.children[i]
				break

			} else if key > node.keys[i] && i == n-1 {
				if node.isLeaf() {
					if node.keys[n-1] == "" {
						//ima mesta u listu za bar jos jedan element
						for j := 0; j < n; j++ {
							if node.keys[j] == "" {
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								return true
							}
							if node.keys[j] > key {
								for k := n - 1; k > j; k-- {
									/*
										node.keys[k] = node.keys[k-1]
										node.vals[k] = node.vals[k-1]
										node.status[k] = node.status[k-1]*/
									copy(node, k, node, k-1)
								}
								node.keys[j] = key
								node.vals[j] = val
								node.status[j] = stat
								return true
							}

						}
					} else {
						//list je popunjen pre dodavanja => rotacija
						if btree.Rotate(key, val, stat, node) {
							return true
						}
						for btree.NodeDivision(&key, &val, &stat, node) {
							if btree.Rotate(key, val, stat, node) {
								return true
							}
						}
					}
				}
				node = node.children[i+1]
				break
			}
		}
	}
	// fmt.Println("Nije uspelo dodavanje.")
	// return false
}

// nema mesta da se ubaci u list, radi rotaciju
// parametri: vrednosti za novi cvor i cvor u koji treba da ide novi cvor
func (btree *BTree) Rotate(key string, val []byte, stat int, node *TreeNode) bool {
	n := len(node.keys)
	parent := node.parent
	i := 0
	if node.isRoot() {
		return false
	}
	for ; i < len(parent.children); i++ {
		if node == parent.children[i] {
			break
		}
	}
	if i > 0 && parent.children[i-1] != nil { //postoji levi brat
		sibling := parent.children[i-1]
		for j := 0; j < n; j++ {
			if sibling.keys[j] == "" {
				//ima mesta kod levog brata i ubaci na kraj kod njega iz roditelja
				sibling.keys[j] = parent.keys[i-1]
				sibling.vals[j] = parent.vals[i-1]
				sibling.status[j] = parent.status[i-1]
				//iz prosledjenog cvora prvog upisi u roditelja najmanjeg
				if key < node.keys[0] {
					parent.keys[i-1] = key
					parent.vals[i-1] = val
					parent.status[i-1] = stat
					return true
				}
				parent.keys[i-1] = node.keys[0]
				parent.vals[i-1] = node.vals[0]
				parent.status[i-1] = node.status[0]
				//u prosledjenog uvrsti novi cvor
				for p := 0; p < n; p++ {
					if node.keys[p] == "" {
						node.keys[p] = key
						node.vals[p] = val
						node.status[p] = stat
						return true
					}
					if node.keys[p] > key {
						for k := n - 1; k > j; k-- {
							node.keys[k] = node.keys[k-1]
							node.vals[k] = node.vals[k-1]
							node.status[k] = node.status[k-1]
						}
						node.keys[p] = key
						node.vals[p] = val
						node.status[p] = stat
						return true
					}
				}
			}
		}
	}
	if i < n && parent.children[i+1] != nil { //postoji desni brat
		sibling := parent.children[i+1]
		for j := 0; j < n; j++ {
			if sibling.keys[j] == "" {
				//ima mesta kod desnog brata i ubaci na pocetak kod njega
				//pomeri sve za jedno mesto na desnu stranu
				for k := n - 1; k > 0; k++ {
					sibling.keys[k] = sibling.keys[k-1]
					sibling.vals[k] = sibling.vals[k-1]
					sibling.status[k] = sibling.status[k-1]
				}
				//iz roditelja ubacis na prvo mesto u brata
				sibling.keys[0] = parent.keys[i]
				sibling.vals[0] = parent.vals[i]
				sibling.status[0] = parent.status[i]
				//najveci od kljuceva iz prosledjenog cvora i novog kljuca ide u roditelja
				if key > node.keys[n-1] {
					parent.keys[i] = key
					parent.vals[i] = val
					parent.status[i] = stat
					return true
				} else {
					parent.keys[i] = node.keys[n-1]
					parent.vals[i] = node.vals[n-1]
					parent.status[i] = node.status[n-1]
					//novi se uvrsti u node
					for p := 0; p < n; p++ {
						if node.keys[p] == "" {
							node.keys[p] = key
							node.vals[p] = val
							node.status[p] = stat
							return true
						}
						if node.keys[p] > key {
							for k := n - 1; k > j; k-- {
								node.keys[k] = node.keys[k-1]
								node.vals[k] = node.vals[k-1]
								node.status[k] = node.status[k-1]
							}
							node.keys[p] = key
							node.vals[p] = val
							node.status[p] = stat
							return true
						}
					}
				}
			}

		}
	}
	return false //nije uspeo da uradi rotaciju
}

// podela cvorova
func (btree *BTree) NodeDivision(key *string, val *[]byte, stat *int, node *TreeNode) bool {

	n := len(node.keys) //=2

	all_keys := make([]string, n+1)
	all_vals := make([][]byte, n+1)
	all_stats := make([]int, n+1)

	in := false
	j := 0
	for i := 0; i < n+1; i++ {
		if !in && *key < node.keys[j] {
			all_keys[i] = *key
			all_vals[i] = *val
			all_stats[i] = *stat
			in = true
		} else {
			all_keys[i] = node.keys[j]
			all_vals[i] = node.vals[j]
			all_stats[i] = node.status[j]
			j++
		}
	}
	//sortirani podaci koji se dele 3 kljuca: drugi u koren, prvi levo, treci desno

	if node.isRoot() {
		new_root := CreateTreeNode(nil, all_keys[1], all_vals[1], all_stats[1])

		right := CreateTreeNode(new_root, all_keys[2], all_vals[2], all_stats[2])
		right.children[0] = node.children[2]

		new_root.children[0] = node
		new_root.children[1] = right

		node.keys[1] = ""
		node.children[2] = nil

		return false
	}
	//ako node nije koren...

	//koliko kljuceva ima node-ov roditelj?
	parent := node.parent
	i := 0
	for ; i < 3; i++ {
		if node == parent.children[i] {
			break
		}
	}

	if parent.keys[1] == "" { //ima mesta u roditelju za jos jedan kljuc i jos jedno dete
		if i == 0 {
			copy(parent, 1, parent, 0)
			parent.insertAt(0, all_keys[1], all_vals[1], all_stats[1])

			parent.children[2] = parent.children[1]

			parent.children[1] = CreateTreeNode(parent, all_keys[2], all_vals[2], all_stats[2])

			node.insertAt(0, all_keys[0], all_vals[0], all_stats[0])
			node.insertAt(1, "", nil, 0)

			return true

		} else if i == 1 {
			node.insertAt(1, "", nil, 0)
			node.insertAt(0, all_keys[0], all_vals[0], all_stats[0])

			parent.insertAt(1, all_keys[1], all_vals[1], all_stats[1])

			parent.children[2] = CreateTreeNode(parent, all_keys[2], all_vals[2], all_stats[2])

			return true
		}

	} else { //roditelj je maksimalno popunjen, poziva se i za njega, pa se u povratku namestaju deca dole

	}

	//ako node-ov roditelj ima maksimalan broj kljuceva, prebace se kljucevi ko gde treba i vrati true i izmeni u/i argumente
	//pa ce pozvati ponovo za njih
	return false //..............
}
