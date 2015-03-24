package db09

import "hash/fnv"

// tokenize returns a token for the given key
func (d *MemDB) tokenize(k string) int {
	h := fnv.New32a()
	h.Write([]byte(k))
	return int(h.Sum32()) % d.parts
}
