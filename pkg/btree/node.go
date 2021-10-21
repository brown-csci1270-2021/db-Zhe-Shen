package btree

import (
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"

	pager "github.com/brown-csci1270/db/pkg/pager"
)

// Split is a supporting data structure to propagate keys up our B+ tree.
type Split struct {
	isSplit bool  // A flag that's set if a split occurs.
	key     int64 // The key to promote.
	leftPN  int64 // The pagenumber for the left node.
	rightPN int64 // The pagenumber for the right node.
	err     error // Used to propagate errors upwards.
}

// Node defines a common interface for leaf and internal nodes.
type Node interface {
	// Interface for main node functions.
	search(int64) int64
	insert(int64, int64, bool) Split
	delete(int64)
	get(int64) (int64, bool)

	// Interface for helper functions.
	keyToNodeEntry(int64) (*LeafNode, int64, error)
	printNode(io.Writer, string, string)
	getPage() *pager.Page
	getNodeType() NodeType
}

/////////////////////////////////////////////////////////////////////////////
///////////////////////////// Leaf Node Methods /////////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key >= given key.
// If no key satisfies this condition, returns numKeys.
func (node *LeafNode) search(key int64) int64 {
	return int64(sort.Search(int(node.numKeys), func(i int) bool {
		return node.getKeyAt(int64(i)) >= key
	}))
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
// if update is true, allow overwriting existing keys. else, error.
func (node *LeafNode) insert(key int64, value int64, update bool) Split {
	idx := node.search(key)
	if update {
		if idx < node.numKeys && node.getKeyAt(idx) == key {
			node.updateKeyAt(idx, key)
			node.updateValueAt(idx, value)
			return Split{
				isSplit: false,
			}
		} else {
			return Split{
				err: fmt.Errorf("Cannot update non-existent entry"),
			}
		}
	}

	if idx < node.numKeys && node.getKeyAt(idx) == key {
		return Split{
			err: fmt.Errorf("Cannot insert duplicate key"),
		}
	}

	for i := node.numKeys; i > idx; i-- {
		node.updateKeyAt(i, node.getKeyAt(i-1))
		node.updateValueAt(i, node.getValueAt(i-1))
	}
	node.updateKeyAt(idx, key)
	node.updateValueAt(idx, value)
	node.updateNumKeys(node.numKeys + 1)
	if node.numKeys > ENTRIES_PER_LEAF_NODE {
		return node.split()
	}
	return Split{
		isSplit: false,
	}
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *LeafNode) delete(key int64) {
	idx := node.search(key)
	if idx == node.numKeys {
		return
	}
	if node.getKeyAt(idx) == key {
		for i := idx + 1; i < node.numKeys; i++ {
			node.updateKeyAt(i-1, node.getKeyAt(i))
			node.updateValueAt(i-1, node.getValueAt(i))
		}
		node.updateNumKeys(node.numKeys - 1)
		// fmt.Println(node.numKeys)
	}
}

// split is a helper function to split a leaf node, then propagate the split upwards.
func (node *LeafNode) split() Split {
	mid := node.numKeys / 2
	newNode, err := createLeafNode(node.page.GetPager())
	defer newNode.getPage().Put()
	if err != nil {
		return Split{
			err: err,
		}
	}
	for i := mid; i < node.numKeys; i++ {
		// fmt.Printf("index %v, key %v\n", i, node.getKeyAt(i))
		newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
		newNode.updateValueAt(newNode.numKeys, node.getValueAt(i))
		newNode.updateNumKeys(newNode.numKeys + 1)
	}
	node.updateNumKeys(mid)
	newNode.setRightSibling(node.rightSiblingPN)
	node.setRightSibling(newNode.page.GetPageNum())
	return Split{
		isSplit: true,
		key:     newNode.getKeyAt(0),
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
		err:     nil,
	}
}

// get returns the value associated with a given key from the leaf node.
func (node *LeafNode) get(key int64) (value int64, found bool) {
	index := node.search(key)
	if index >= node.numKeys || node.getKeyAt(index) != key {
		// Thank you Mario! But our key is in another castle!
		return 0, false
	}
	entry := node.getCell(index)
	return entry.GetValue(), true
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *LeafNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	return node, node.search(key), nil
}

// printNode pretty prints our leaf node.
func (node *LeafNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Leaf"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	for cellnum := int64(0); cellnum < node.numKeys; cellnum++ {
		entry := node.getCell(cellnum)
		io.WriteString(w, fmt.Sprintf("%v |--> (%v, %v)\n",
			prefix, entry.GetKey(), entry.GetValue()))
	}
	if node.rightSiblingPN > 0 {
		io.WriteString(w, fmt.Sprintf("%v |--+\n", prefix))
		io.WriteString(w, fmt.Sprintf("%v    | node @ %v\n",
			prefix, node.rightSiblingPN))
		io.WriteString(w, fmt.Sprintf("%v    v\n", prefix))
	}
}

/////////////////////////////////////////////////////////////////////////////
/////////////////////////// Internal Node Methods ///////////////////////////
/////////////////////////////////////////////////////////////////////////////

// search returns the first index where key > given key.
// If no such index exists, it returns numKeys.
func (node *InternalNode) search(key int64) int64 {
	return int64(sort.Search(int(node.numKeys), func(i int) bool {
		return node.getKeyAt(int64(i)) >= key
	}))
}

// insert finds the appropriate place in a leaf node to insert a new tuple.
func (node *InternalNode) insert(key int64, value int64, update bool) Split {
	idx := node.search(key)
	child, err := node.getChildAt(idx)
	defer child.getPage().Put()
	if err != nil {
		return Split{
			err: err,
		}
	}
	split := child.insert(key, value, update)
	if split.isSplit && split.err == nil {
		return node.insertSplit(split)
	}
	return split
}

// insertSplit inserts a split result into an internal node.
// If this insertion results in another split, the split is cascaded upwards.
func (node *InternalNode) insertSplit(split Split) Split {
	if !split.isSplit || split.err != nil {
		return Split{
			isSplit: split.isSplit,
			err:     split.err,
		}
	}
	idx := node.search(split.key)
	if node.getKeyAt(idx) == split.key {
		node.updatePNAt(idx, split.leftPN)
		node.updatePNAt(idx+1, split.rightPN)
		return Split{
			isSplit: false,
		}
	}
	for i := node.numKeys; i > idx; i-- {
		key := node.getKeyAt(i - 1)
		pn := node.getPNAt(i)
		node.updateKeyAt(i, key)
		node.updatePNAt(i+1, pn)
	}
	node.updateKeyAt(idx, split.key)
	node.updatePNAt(idx, split.leftPN)
	node.updatePNAt(idx+1, split.rightPN)
	node.updateNumKeys(node.numKeys + 1)
	if node.numKeys > KEYS_PER_INTERNAL_NODE {
		return node.split()
	}
	return Split{
		isSplit: false,
	}
}

// delete removes a given tuple from the leaf node, if the given key exists.
func (node *InternalNode) delete(key int64) {
	idx := node.search(key)
	child, err := node.getChildAt(idx)
	defer child.getPage().Put()
	if err != nil {
		log.Println(err)
		return
	}
	child.delete(key)
}

// split is a helper function that splits an internal node, then propagates the split upwards.
func (node *InternalNode) split() Split {
	newNode, err := createInternalNode(node.page.GetPager())
	defer newNode.getPage().Put()
	if err != nil {
		return Split{
			err: err,
		}
	}
	mid := (node.numKeys - 1) / 2
	for i := mid + 1; i < node.numKeys; i++ {
		newNode.updateKeyAt(newNode.numKeys, node.getKeyAt(i))
		newNode.updatePNAt(newNode.numKeys, node.getPNAt(i))
		newNode.updateNumKeys(newNode.numKeys + 1)
	}
	newNode.updatePNAt(newNode.numKeys, node.getPNAt(node.numKeys))
	splitKey := node.getKeyAt(mid)
	node.updateNumKeys(mid)
	return Split{
		isSplit: true,
		key:     splitKey,
		leftPN:  node.page.GetPageNum(),
		rightPN: newNode.page.GetPageNum(),
		err:     nil,
	}
}

// get returns the value associated with a given key from the leaf node.
func (node *InternalNode) get(key int64) (value int64, found bool) {
	childIdx := node.search(key)
	child, err := node.getChildAt(childIdx)
	if err != nil {
		return 0, false
	}
	defer child.getPage().Put()
	return child.get(key)
}

// keyToNodeEntry is a helper function to create cursors that point to a given index within a leaf node.
func (node *InternalNode) keyToNodeEntry(key int64) (*LeafNode, int64, error) {
	index := node.search(key)
	child, err := node.getChildAt(index)
	if err != nil {
		return &LeafNode{}, 0, err
	}
	defer child.getPage().Put()
	return child.keyToNodeEntry(key)
}

// printNode pretty prints our internal node.
func (node *InternalNode) printNode(w io.Writer, firstPrefix string, prefix string) {
	// Format header data.
	var nodeType string = "Internal"
	var isRoot string
	if node.isRoot() {
		isRoot = " (root)"
	}
	numKeys := strconv.Itoa(int(node.numKeys + 1))
	// Print header data.
	io.WriteString(w, fmt.Sprintf("%v[%v] %v%v size: %v\n",
		firstPrefix, node.page.GetPageNum(), nodeType, isRoot, numKeys))
	// Print entries.
	nextFirstPrefix := prefix + " |--> "
	nextPrefix := prefix + " |    "
	for idx := int64(0); idx <= node.numKeys; idx++ {
		io.WriteString(w, fmt.Sprintf("%v\n", nextPrefix))
		child, err := node.getChildAt(idx)
		if err != nil {
			return
		}
		defer child.getPage().Put()
		child.printNode(w, nextFirstPrefix, nextPrefix)
	}
}
