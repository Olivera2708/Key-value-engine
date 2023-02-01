package structures

import "fmt"

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
	keys[1] = ""
	vals[0] = val
	vals[1] = nil
	status[0] = stat
	status[1] = 0
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
			if key == node.keys[i] && node.status[i] == 0 {
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

func (btree *BTree) Add(key string, val []byte, stat int) bool {

	node := btree.root
	n := len(node.keys)
	for {
		for i := 0; i < n; i++ {
			if (key == node.keys[i]) && (node.status[i] == 0) {
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
						btree.NodeDivision(key, val, stat, node)
						return true
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
						btree.NodeDivision(key, val, stat, node)
						return true
					}
				}
				node = node.children[i]
				break
			}
		}
	}
}

// podela cvorova
func (btree *BTree) NodeDivision(key string, val []byte, stat int, node *TreeNode) {

	fmt.Println("node division")

	n := len(node.keys) //=2

	all_keys := make([]string, n+1)
	all_vals := make([][]byte, n+1)
	all_stats := make([]int, n+1)

	in := false
	j := 0
	for i := 0; i < n+1; i++ {
		if (j == n) || (!in && key < node.keys[j]) {
			all_keys[i] = key
			all_vals[i] = val
			all_stats[i] = stat
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
		right.children[1] = node.children[2]

		node.parent = new_root
		new_root.children[0] = node
		new_root.children[1] = right

		node.insertAt(0, all_keys[0], all_vals[0], all_stats[0])
		node.insertAt(1, "", nil, 0)
		node.children[2] = nil

		btree.root = new_root

		return
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

			sibling := CreateTreeNode(parent, all_keys[2], all_vals[2], all_stats[2])
			parent.children[1] = sibling

			node.insertAt(0, all_keys[0], all_vals[0], all_stats[0])
			node.insertAt(1, "", nil, 0)

			sibling.children[1] = node.children[2]
			node.children[2] = nil
			//node.children[1] = nil

			return

		} else if i == 1 {
			node.insertAt(1, "", nil, 0)
			node.insertAt(0, all_keys[0], all_vals[0], all_stats[0])

			parent.insertAt(1, all_keys[1], all_vals[1], all_stats[1])

			sibling := CreateTreeNode(parent, all_keys[2], all_vals[2], all_stats[2])
			parent.children[2] = sibling

			sibling.children[1] = node.children[2]
			node.children[2] = nil

			return
		}

	} else { //roditelj je maksimalno popunjen, poziva se i za njega, pa se u povratku namestaju deca dole

		btree.NodeDivision(all_keys[1], all_vals[1], all_stats[1], parent)

		grand := parent.parent

		c := 0
		for ; c < 3; c++ {
			if grand.children[c] == parent {
				break
			}
		}

		grand.children[c+1].children[0] = grand.children[c].children[1]
		grand.children[c+1].children[0].parent = grand.children[c+1]

		grand.children[c].children[0] = CreateTreeNode(grand.children[c], all_keys[0], all_vals[0], all_stats[0])
		grand.children[c].children[1] = CreateTreeNode(grand.children[c], all_keys[2], all_vals[2], all_stats[2])

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
		fmt.Println("(" + node.keys[i] + ", " + fmt.Sprint(dubina) + ")")
	}
	if node.children[n] != nil {
		inorder(node.children[n], dubina+1)
	}
}
