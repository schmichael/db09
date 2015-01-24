package db09

import "hash/fnv"

// tokenize returns a token for the given key
func tokenize(k string) int {
	h := fnv.New32a()
	h.Write([]byte(k))
	// Mask to uint16 and return as an int for indexing
	return int(h.Sum32() & 0xffff)
}
