package structures

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"os"
)

type MerkleRoot struct {
	root *Node
}

func (mr *MerkleRoot) String() string {
	return mr.root.String()
}

type Node struct {
	data  [20]byte
	left  *Node
	right *Node
}

func (n *Node) String() string {
	return hex.EncodeToString(n.data[:])
}

func Hash(data []byte) [20]byte {
	return sha1.Sum(data)
}

func CreateMerkleTree(all_data [][]byte) *MerkleRoot {
	data := all_data

	leaves := CreateLeaves(data)
	root_node := CreateNodes(leaves)

	root := MerkleRoot{root_node}

	return &root
}

func CreateLeaves(data [][]byte) []*Node {
	all_leaves := []*Node{}

	for i := 0; i < len(data); i++ {
		node := Node{Hash(data[i]), nil, nil}
		all_leaves = append(all_leaves, &node)
	}

	return all_leaves
}

func CreateNodes(leaves []*Node) *Node {
	node_list := []*Node{}

	if len(leaves) == 1 {
		return leaves[0]
	}

	for i := 1; i < len(leaves); i += 2 {
		new_data := append(leaves[i-1].data[:], leaves[i].data[:]...)
		new_node := Node{Hash(new_data), leaves[i-1], leaves[i]}
		node_list = append(node_list, &new_node)
	}

	if len(leaves)%2 == 1 {
		empty_node := Node{[20]byte{}, nil, nil}
		new_data := append(leaves[len(leaves)-1].data[:], empty_node.data[:]...)
		new_node := Node{Hash(new_data), leaves[len(leaves)-1], &empty_node}
		node_list = append(node_list, &new_node)
	}

	return CreateNodes(node_list)
}

func PrintTree(mr *MerkleRoot) {
	next_print := []*Node{}
	next_print = append(next_print, mr.root)

	for len(next_print) != 0 {
		now_print := next_print[0]
		next_print = next_print[1:]

		fmt.Println(now_print)

		if now_print.left != nil {
			next_print = append(next_print, now_print.left)
		}

		if now_print.right != nil {
			next_print = append(next_print, now_print.right)
		}
	}
}

func WriteMerkleInFile(mr *MerkleRoot, path string) {
	file, err := os.OpenFile(path+"-Metadata.txt", os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatal(err)
	}
	file.Seek(0, 2)

	next_print := []*Node{}
	next_print = append(next_print, mr.root)

	for len(next_print) != 0 {
		now_print := next_print[0]
		next_print = next_print[1:]

		file.WriteString(now_print.String() + "\n")

		if now_print.left != nil {
			next_print = append(next_print, now_print.left)
		}

		if now_print.right != nil {
			next_print = append(next_print, now_print.right)
		}
	}

	err = file.Close()
	if err != nil {
		fmt.Print(err)
	}
}

func main6() {
	all_data := [][]byte{{97, 93}, {47, 12}, {97, 93}, {47, 12}, {97, 93}, {47, 12}}

	mr := CreateMerkleTree(all_data)
	PrintTree(mr)
}
