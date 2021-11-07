package query

import (
	bitset "github.com/bits-and-blooms/bitset"
	"github.com/brown-csci1270/db/pkg/hash"
)

type BloomFilter struct {
	size int64
	bits *bitset.BitSet
}

// CreateFilter initializes a BloomFilter with the given size.
func CreateFilter(size int64) *BloomFilter {
	return &BloomFilter{
		size: size,
		bits: bitset.New(uint(size)),
	}
}

// Insert adds an element into the bloom filter.
func (filter *BloomFilter) Insert(key int64) {
	xx := hash.XxHasher(key, DEFAULT_FILTER_SIZE)
	murmur := hash.MurmurHasher(key, DEFAULT_FILTER_SIZE)
	filter.bits.Set(xx)
	filter.bits.Set(murmur)
}

// Contains checks if the given key can be found in the bloom filter/
func (filter *BloomFilter) Contains(key int64) bool {
	xx := hash.XxHasher(key, DEFAULT_FILTER_SIZE)
	murmur := hash.MurmurHasher(key, DEFAULT_FILTER_SIZE)
	return filter.bits.Test(xx) && filter.bits.Test(murmur)
}
