package hash

import (
	"fmt"
	"io"
	"math"
	"sync"

	pager "github.com/brown-csci1270/db/pkg/pager"
	utils "github.com/brown-csci1270/db/pkg/utils"
)

// HashTable definitions.
type HashTable struct {
	depth   int64
	buckets []int64 // Array of bucket page numbers
	pager   *pager.Pager
	rwlock  sync.RWMutex // Lock on the hash table index
}

// Returns a new HashTable.
func NewHashTable(pager *pager.Pager) (*HashTable, error) {
	depth := int64(2)
	buckets := make([]int64, powInt(2, depth))
	for i := range buckets {
		bucket, err := NewHashBucket(pager, depth)
		if err != nil {
			return nil, err
		}
		buckets[i] = bucket.page.GetPageNum()
		bucket.page.Put()
	}
	return &HashTable{depth: depth, buckets: buckets, pager: pager}, nil
}

// [CONCURRENCY] Grab a write lock on the hash table index
func (table *HashTable) WLock() {
	table.rwlock.Lock()
}

// [CONCURRENCY] Release a write lock on the hash table index
func (table *HashTable) WUnlock() {
	table.rwlock.Unlock()
}

// [CONCURRENCY] Grab a read lock on the hash table index
func (table *HashTable) RLock() {
	table.rwlock.RLock()
}

// [CONCURRENCY] Release a read lock on the hash table index
func (table *HashTable) RUnlock() {
	table.rwlock.RUnlock()
}

// Get depth.
func (table *HashTable) GetDepth() int64 {
	return table.depth
}

// Get bucket page numbers.
func (table *HashTable) GetBuckets() []int64 {
	return table.buckets
}

// Get pager.
func (table *HashTable) GetPager() *pager.Pager {
	return table.pager
}

// Finds the entry with the given key.
func (table *HashTable) Find(key int64) (utils.Entry, error) {
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return nil, err
	}
	defer bucket.GetPage().Put()
	entry, found := bucket.Find(key)
	if !found {
		return nil, fmt.Errorf("%v not found\n", key)
	}
	return entry, nil
}

// ExtendTable increases the global depth of the table by 1.
func (table *HashTable) ExtendTable() {
	table.depth = table.depth + 1
	table.buckets = append(table.buckets, table.buckets...)
}

// Split the given bucket into two, extending the table if necessary.
func (table *HashTable) Split(bucket *HashBucket, hash int64) error {
	newHash := hash + (1 << bucket.GetDepth())
	bucket.updateDepth(bucket.GetDepth() + 1)
	if bucket.GetDepth() > table.GetDepth() {
		table.ExtendTable()
	}
	newBucket, err := NewHashBucket(table.pager, bucket.GetDepth())
	if err != nil {
		return err
	}
	defer newBucket.page.Put()
	entries, err := bucket.Select()
	if err != nil {
		return err
	}
	for _, entry := range entries {
		entryHash := Hasher(entry.GetKey(), table.GetDepth())
		if entryHash == hash {
			bucket.Insert(entry.GetKey(), entry.GetValue())
		} else if entryHash == newHash {
			bucket.Delete(entry.GetKey())
			newBucket.Insert(entry.GetKey(), entry.GetValue())
		}
	}
	if bucket.numKeys > BUCKETSIZE {
		return table.Split(bucket, hash)
	}
	if newBucket.numKeys > BUCKETSIZE {
		return table.Split(newBucket, newHash)
	}
	return nil
}

// Inserts the given key-value pair, splits if necessary.
func (table *HashTable) Insert(key int64, value int64) error {
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.GetPage().Put()
	split, err := bucket.Insert(key, value)
	if err != nil {
		return err
	}
	if split {
		return table.Split(bucket, hash)
	}
	return nil
}

// Update the given key-value pair.
func (table *HashTable) Update(key int64, value int64) error {
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.GetPage().Put()
	err = bucket.Update(key, value)
	return err
}

// Delete the given key-value pair, does not coalesce.
func (table *HashTable) Delete(key int64) error {
	hash := Hasher(key, table.GetDepth())
	bucket, err := table.GetBucket(hash)
	if err != nil {
		return err
	}
	defer bucket.GetPage().Put()
	err = bucket.Delete(key)
	return err
}

// Select all entries in this table.
func (table *HashTable) Select() ([]utils.Entry, error) {
	buckets := table.GetBuckets()
	entries := make([]utils.Entry, 0)
	for _, pn := range buckets {
		bucket, err := table.GetBucketByPN(pn)
		if err != nil {
			return nil, err
		}
		defer bucket.GetPage().Put()
		tmpEntries, err := bucket.Select()
		if err != nil {
			return nil, err
		}
		entries = append(entries, tmpEntries...)
	}
	return entries, nil
}

// Print out each bucket.
func (table *HashTable) Print(w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	io.WriteString(w, "====\n")
	io.WriteString(w, fmt.Sprintf("global depth: %d\n", table.depth))
	for i := range table.buckets {
		io.WriteString(w, fmt.Sprintf("====\nbucket %d\n", i))
		bucket, err := table.GetBucket(int64(i))
		if err != nil {
			continue
		}
		bucket.RLock()
		bucket.Print(w)
		bucket.RUnlock()
		bucket.page.Put()
	}
	io.WriteString(w, "====\n")
}

// Print out a specific bucket.
func (table *HashTable) PrintPN(pn int, w io.Writer) {
	table.RLock()
	defer table.RUnlock()
	if int64(pn) >= table.pager.GetNumPages() {
		fmt.Println("out of bounds")
		return
	}
	bucket, err := table.GetBucketByPN(int64(pn))
	if err != nil {
		return
	}
	bucket.RLock()
	bucket.Print(w)
	bucket.RUnlock()
	bucket.page.Put()
}

// x^y
func powInt(x, y int64) int64 {
	return int64(math.Pow(float64(x), float64(y)))
}