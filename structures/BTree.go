package structures

import (
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
	keys := make([]string, 2)
	vals := make([][]byte, 2)
	status := make([]int, 2)
	timestamps := make([]uint64, 2)
	children := make([]*TreeNode, 3)

	keys[0] = key
	keys[1] = ""
	vals[0] = val
	vals[1] = nil
	status[0] = stat
	status[1] = 0
	timestamps[0] = timestamp
	timestamps[1] = 0
	children[0] = nil
	children[1] = nil
	children[2] = nil
	return &TreeNode{keys, vals, status, timestamps, children, parent}
}

func CreateBTree(key string, val []byte, status int, timestamp uint64) *BTree {
	root := CreateTreeNode(nil, key, val, status, timestamp)
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

			} else if (i == 0 && strings.Split(key, "-")[0] > strings.Split(node.keys[i], "-")[0] && node.keys[1] == "") || (i == 1 && strings.Split(key, "-")[0] > strings.Split(node.keys[i], "-")[0]) {
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

// podela cvorova
func (btree *BTree) NodeDivision(key string, val []byte, stat int, timestamp uint64, node *TreeNode) {

	n := len(node.keys) //=2

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
		new_root := CreateTreeNode(nil, all_keys[1], all_vals[1], all_stats[1], all_times[1])

		right := CreateTreeNode(new_root, all_keys[2], all_vals[2], all_stats[2], all_times[2])
		right.children[1] = node.children[2]

		node.parent = new_root
		new_root.children[0] = node
		new_root.children[1] = right

		node.insertAt(0, all_keys[0], all_vals[0], all_stats[0], all_times[0])
		node.insertAt(1, "", nil, 0, 0)
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
			parent.insertAt(0, all_keys[1], all_vals[1], all_stats[1], all_times[1])

			parent.children[2] = parent.children[1]

			sibling := CreateTreeNode(parent, all_keys[2], all_vals[2], all_stats[2], all_times[2])
			parent.children[1] = sibling

			node.insertAt(0, all_keys[0], all_vals[0], all_stats[0], all_times[0])
			node.insertAt(1, "", nil, 0, 0)

			sibling.children[1] = node.children[2]
			node.children[2] = nil
			//node.children[1] = nil

			return

		} else if i == 1 {
			node.insertAt(1, "", nil, 0, 0)
			node.insertAt(0, all_keys[0], all_vals[0], all_stats[0], all_times[0])

			parent.insertAt(1, all_keys[1], all_vals[1], all_stats[1], all_times[1])

			sibling := CreateTreeNode(parent, all_keys[2], all_vals[2], all_stats[2], all_times[2])
			parent.children[2] = sibling

			sibling.children[1] = node.children[2]
			node.children[2] = nil

			return
		}

	} else { //roditelj je maksimalno popunjen, poziva se i za njega, pa se u povratku namestaju deca dole

		btree.NodeDivision(all_keys[1], all_vals[1], all_stats[1], all_times[1], parent)

		grand := parent.parent

		c := 0
		for ; c < 3; c++ {
			if grand.children[c] != nil && grand.children[c].keys[0] == all_keys[1] {
				parent = grand.children[c]
				break
			}
		}

		// grand.children[c+1].children[0] = grand.children[c].children[1]
		// grand.children[c+1].children[0].parent = grand.children[c+1]

		//grand.children[c].children[0] = CreateTreeNode(grand.children[c], all_keys[0], all_vals[0], all_stats[0], all_times[0])
		// grand.children[c].children[1] = CreateTreeNode(grand.children[c], all_keys[2], all_vals[2], all_stats[2], all_times[2])

		parent.children[0] = node
		node.parent = parent
		node.insertAt(0, all_keys[0], all_vals[0], all_stats[0], all_times[0])
		node.insertAt(1, "", nil, 0, 0)

		parent.children[1] = CreateTreeNode(parent, all_keys[2], all_vals[2], all_stats[2], all_times[2])
		parent.children[1].children[1] = node.children[2]
		node.children[2] = nil

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

func (btree *BTree) GetData() [][][]byte {
	data := make([][][]byte, 0)
	gd(btree.root, &data)
	fmt.Println(data)
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

func (btree *BTree) FindAllPrefix(prefix string) string {
	// res := make([]string, 0)
	// res_val := make([][]byte, 0)

	data := btree.GetData()

	for i := 0; i < len(data); i++ {
		k := string(data[i][0])

		if data[i][2][0] == 0 && strings.HasPrefix(k, prefix) {
			return k
			// res = append(res, k)
			// res_val = append(res_val, data[i][1])
		}
	}
	// return res, res_val
	return ""
}

func (btree *BTree) FindAllPrefixRange(min_prefix string, max_prefix string) ([]string, [][]byte) {
	res := make([]string, 0)
	res_val := make([][]byte, 0)

	data := btree.GetData()

	for i := 0; i < len(data); i++ {
		k := string(data[i][0])

		if data[i][2][0] == 0 && min_prefix <= strings.Split(k, "-")[0] && max_prefix >= strings.Split(k, "-")[0] {
			res = append(res, k)
			res_val = append(res_val, data[i][1])
		}
	}
	return res, res_val
}
